import os
import sys
import time
import warnings
import sqlite3
import tempfile
import multiprocessing as mp
from bisect import bisect_left
from datetime import datetime, timezone
import shutil
import subprocess
from pathlib import Path

import pandas as pd
from tqdm import tqdm
from pydriller import Repository, Git

warnings.filterwarnings("ignore", category=ResourceWarning)

# =====================
# CONFIG
# =====================

# Debug mode (main process only)
DEBUG_MODE = False
DEBUG_LOG_PATH = "debug_log.txt"

# Input CSV produced by the smell analyzer
INPUT_CSV = os.path.join("e2e-test-smell-analyzer-main", "typescript_analysis.csv")

# SQLite output
DB_PATH = "historical_smellsTS.db"

# Clone root
CLONE_ROOT = "repos"

# Parallelism
NUM_WORKERS = max(1, (os.cpu_count() or 2) - 1)
TASK_QUEUE_MAXSIZE = 2000
RESULT_QUEUE_MAXSIZE = 5000
SQLITE_BATCH_SIZE = 1000

# Quick smoke mode
TEST_MODE = False
TEST_MAX_REPOS = 1

# Optional overrides via env vars
if os.getenv("E2E_TEST_MODE") in {"1", "true", "True", "yes", "YES"}:
    TEST_MODE = True
if os.getenv("E2E_TEST_MAX_REPOS"):
    TEST_MAX_REPOS = int(os.getenv("E2E_TEST_MAX_REPOS"))
if os.getenv("E2E_NUM_WORKERS"):
    NUM_WORKERS = max(1, int(os.getenv("E2E_NUM_WORKERS")))
if os.getenv("E2E_DB_PATH"):
    DB_PATH = os.getenv("E2E_DB_PATH")

if os.getenv("E2E_CLONE_ROOT"):
    CLONE_ROOT = os.getenv("E2E_CLONE_ROOT")

MAX_COMMITS = None
if os.getenv("E2E_MAX_COMMITS"):
    MAX_COMMITS = int(os.getenv("E2E_MAX_COMMITS"))


def _open_debug_file():
    if not DEBUG_MODE:
        return None
    return open(DEBUG_LOG_PATH, "w", encoding="utf-8")


def debug_log(debug_file, msg: str) -> None:
    if not DEBUG_MODE or not debug_file:
        return
    debug_file.write(msg + "\n")
    debug_file.flush()


def _reset_db_file(db_path: str, debug_file) -> None:
    """Delete existing SQLite DB and sidecar files at startup."""
    for path in (db_path, f"{db_path}-wal", f"{db_path}-shm"):
        try:
            if os.path.exists(path):
                os.remove(path)
                debug_log(debug_file, f"[DEBUG] Removed existing DB file: {path}")
        except Exception as e:
            raise RuntimeError(f"Unable to remove existing database file '{path}': {e}")

# ========= PATH TOOL =========
analyzer_path = os.path.join(os.path.dirname(__file__), "e2e-test-smell-analyzer-main")
sys.path.insert(0, analyzer_path)

# ========= IMPORT DETECTORS =========
from parser.typescript_parser import TypeScriptParser
from detectors.typescript.conditional_logic_detector import ConditionalLogicDetectorTS
from detectors.typescript.constructor_initialization_detector import ConstructorInitializationDetectorTS
from detectors.typescript.exception_handling_detector import ExceptionHandlingDetectorTS
from detectors.typescript.sleepy_test_detector import SleepyTestDetectorTS
from detectors.typescript.sensitive_equality_detector import SensitiveEqualityDetectorTS
from detectors.typescript.empty_test_detector import EmptyTestDetectorTS
from detectors.typescript.assertion_roulette_detector import AssertionRouletteDetectorTS
from detectors.typescript.duplicate_assert_detector import DuplicateAssertDetectorTS
from detectors.typescript.redundant_print_detector import RedundantPrintDetectorTS
from detectors.typescript.redundant_assertion_detector import RedundantAssertionDetectorTS
from detectors.typescript.magic_number_detector import MagicNumberDetectorTS
from detectors.typescript.mystery_guest_detector import MysteryGuestDetectorTS
from detectors.typescript.unknown_test_detector import UnknownTestDetectorTS
from detectors.typescript.absolute_url_detector import AbsoluteUrlDetectorTS
from detectors.typescript.absolute_xpath_detector import AbsoluteXPathDetectorTS
from detectors.typescript.global_variable_detector import GlobalVariableDetectorTS
from detectors.typescript.misused_tag_locator_detector import MisusedTagLocatorDetectorTS
from detectors.typescript.preferred_locator_detector import PreferredLocatorDetectorTS
from detectors.typescript.unstable_link_text_detector import UnstableLinkTextDetectorTS
from detectors.typescript.complex_test_detector import ComplexTestDetectorTS

# ========= DETECTORS =========
detectors = [
    ConditionalLogicDetectorTS,
    ConstructorInitializationDetectorTS,
    ExceptionHandlingDetectorTS,
    SleepyTestDetectorTS,
    SensitiveEqualityDetectorTS,
    EmptyTestDetectorTS,
    AssertionRouletteDetectorTS,
    DuplicateAssertDetectorTS,
    RedundantPrintDetectorTS,
    RedundantAssertionDetectorTS,
    MagicNumberDetectorTS,
    MysteryGuestDetectorTS,
    UnknownTestDetectorTS,
    AbsoluteUrlDetectorTS,
    AbsoluteXPathDetectorTS,
    GlobalVariableDetectorTS,
    MisusedTagLocatorDetectorTS,
    PreferredLocatorDetectorTS,
    UnstableLinkTextDetectorTS,
    ComplexTestDetectorTS
]


def normalize_path(repo_name: str, file_path: str) -> str:
    """Normalize file path from the CSV to a path relative to repo root."""
    repo_underscore = repo_name.replace('/', '_')
    path = (file_path or "").lstrip('/').replace('\\', '/')
    if path.startswith(repo_underscore + '/'):
        path = path[len(repo_underscore) + 1:]
    return path


def _norm_path(p: str | None) -> str:
    if not p:
        return ""
    return p.replace('\\', '/').lstrip('./')


def _paths_match(mod_path: str | None, target_path: str) -> bool:
    mod = _norm_path(mod_path)
    target = _norm_path(target_path)
    if not mod or not target:
        return False
    if mod == target:
        return True
    if mod.endswith('/' + target) or target.endswith('/' + mod):
        return True
    return False


def _to_utc_datetime(value: datetime | str | None) -> datetime | None:
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except Exception:
            return None
    else:
        return None

    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _build_release_index(git_repo: Git, debug_file):
    """Build a sorted index of repository tags by creation date (used as releases)."""
    try:
        out = git_repo.repo.git.for_each_ref(
            "refs/tags",
            "--sort=creatordate",
            "--format=%(refname:short)|%(creatordate:iso-strict)",
        )
    except Exception as e:
        debug_log(debug_file, f"[DEBUG] Could not read tags/releases: {e}")
        return {"dates": [], "tags": [], "date_iso": []}

    release_dates: list[datetime] = []
    release_tags: list[str] = []
    release_date_iso: list[str] = []

    for line in out.splitlines():
        if not line or "|" not in line:
            continue
        tag, date_str = line.split("|", 1)
        dt = _to_utc_datetime(date_str.strip())
        if dt is None:
            continue
        release_dates.append(dt)
        release_tags.append(tag.strip())
        release_date_iso.append(dt.isoformat())

    debug_log(debug_file, f"[DEBUG] Release/tag index size: {len(release_dates)}")
    return {"dates": release_dates, "tags": release_tags, "date_iso": release_date_iso}


def _nearest_future_release(commit_dt: datetime | None, release_index) -> tuple[str | None, str | None]:
    dates = release_index.get("dates", []) if release_index else []
    if not commit_dt or not dates:
        return None, None

    idx = bisect_left(dates, commit_dt)
    if idx >= len(dates):
        return None, None

    tags = release_index.get("tags", [])
    date_iso = release_index.get("date_iso", [])
    return tags[idx], date_iso[idx]


def _nearest_previous_release(commit_dt: datetime | None, release_index) -> tuple[str | None, str | None]:
    dates = release_index.get("dates", []) if release_index else []
    if not commit_dt or not dates:
        return None, None

    idx = bisect_left(dates, commit_dt)
    prev_idx = idx - 1
    if prev_idx < 0:
        return None, None

    tags = release_index.get("tags", [])
    date_iso = release_index.get("date_iso", [])
    return tags[prev_idx], date_iso[prev_idx]


def _discover_repo_root(local_repo_path: str, repo_short_name: str) -> str | None:
    if os.path.isdir(os.path.join(local_repo_path, '.git')):
        return local_repo_path
    nested = os.path.join(local_repo_path, repo_short_name)
    if os.path.isdir(os.path.join(nested, '.git')):
        return nested
    return None


def _clone_if_needed(repo_url: str, local_repo_path: str, repo_short_name: str, debug_file) -> str:
    os.makedirs(CLONE_ROOT, exist_ok=True)

    if os.path.isfile(local_repo_path):
        os.remove(local_repo_path)
        os.makedirs(local_repo_path, exist_ok=True)

    os.makedirs(local_repo_path, exist_ok=True)

    existing_repo_root = _discover_repo_root(local_repo_path, repo_short_name)
    if existing_repo_root:
        debug_log(debug_file, f"[DEBUG] Repo already cloned at: {existing_repo_root}")
        return existing_repo_root

    def _git_clone_no_checkout(url: str, dest_dir: str) -> None:
        """Clone without checkout to avoid Windows long-path issues."""
        if os.path.isdir(dest_dir):
            shutil.rmtree(dest_dir, ignore_errors=True)

        os.makedirs(os.path.dirname(dest_dir), exist_ok=True)
        cmd = [
            "git",
            "clone",
            "--no-checkout",
            "--config",
            "core.longpaths=true",
            url,
            dest_dir,
        ]
        debug_log(debug_file, f"[DEBUG] Running: {' '.join(cmd)}")
        subprocess.run(cmd, check=True, capture_output=True, text=True)

    # Prefer manual clone with --no-checkout (prevents 'Filename too long' on Windows)
    dest_dir = os.path.join(local_repo_path, repo_short_name)
    debug_log(debug_file, f"[DEBUG] Cloning {repo_url} into {dest_dir} (no-checkout)")
    try:
        _git_clone_no_checkout(repo_url, dest_dir)
    except Exception as e:
        # Fallback to PyDriller clone if system git is missing or other issues occur.
        debug_log(debug_file, f"[DEBUG] no-checkout clone failed: {e}. Falling back to PyDriller clone.")
        try:
            # Trigger clone (Repository will clone if path is remote)
            for _ in Repository(repo_url, clone_repo_to=local_repo_path).traverse_commits():
                break
        except Exception as e2:
            # Re-raise the original error if it's the likely root cause.
            raise e2

    repo_root = _discover_repo_root(local_repo_path, repo_short_name)
    if not repo_root:
        raise RuntimeError(f"Clone completed but .git not found under {local_repo_path}")
    return repo_root


def _commits_touching_path(git_repo: Git, filepath: str) -> list[str]:
    """Return commit hashes that modified `filepath`.

    Uses `git log --follow --format=%H -- <path>` so it works even when the
    working tree is empty (e.g., clones created with `--no-checkout`).
    """
    try:
        path = str(Path(filepath))
        out = git_repo.repo.git.log("--follow", "--format=%H", "--", path)
    except Exception:
        return []
    return [h for h in out.splitlines() if h]


def _commit_unix_ts(git_repo: Git, commit_hash: str) -> int:
    """Return commit timestamp in epoch seconds, 0 if unavailable."""
    try:
        out = git_repo.repo.git.show("-s", "--format=%ct", commit_hash).strip()
        return int(out)
    except Exception:
        return 0


def _build_targets_index(files_rows: list[tuple[str, str, str]], debug_file):
    """Build an index to match ModifiedFile paths quickly (by basename)."""
    targets = []
    by_basename: dict[str, list[int]] = {}

    for framework, repo_name, file_path in files_rows:
        normalized = normalize_path(repo_name, file_path)
        targets.append({
            "framework": framework,
            "repo": repo_name,
            "path": normalized,
            "basename": os.path.basename(normalized),
        })
        by_basename.setdefault(os.path.basename(normalized), []).append(len(targets) - 1)

    debug_log(debug_file, f"[DEBUG] Targets: {len(targets)}")
    return targets, by_basename


def _extract_smell_rows(commit, repo_name: str, targets: list[dict], by_basename: dict[str, list[int]], ts_parser: TypeScriptParser, release_index):
    """Analyze a commit and return rows for all target files modified in this commit."""
    rows = []
    commit_hash = commit.hash
    commit_dt = _to_utc_datetime(commit.author_date)
    commit_date = commit_dt.isoformat() if commit_dt is not None else str(commit.author_date)
    nearest_release_tag, nearest_release_date = _nearest_future_release(commit_dt, release_index)
    nearest_previous_release_tag, nearest_previous_release_date = _nearest_previous_release(commit_dt, release_index)

    author_obj = getattr(commit, "author", None)
    commit_author = (
        getattr(author_obj, "name", None)
        or getattr(commit, "author_name", None)
        or getattr(commit, "committer", None)
        or ""
    )
    commit_message = (
        getattr(commit, "msg", None)
        or getattr(commit, "message", None)
        or getattr(commit, "commit_message", None)
        or ""
    )

    for mod in commit.modified_files:
        candidates_idx: set[int] = set()

        for p in (mod.new_path, mod.old_path):
            if not p:
                continue
            base = os.path.basename(_norm_path(p))
            for idx in by_basename.get(base, []):
                candidates_idx.add(idx)

        if not candidates_idx:
            continue

        matched_targets = []
        for idx in candidates_idx:
            target_path = targets[idx]["path"]
            if _paths_match(mod.new_path, target_path) or _paths_match(mod.old_path, target_path):
                matched_targets.append(targets[idx])

        if not matched_targets:
            continue

        test_class = None
        if mod.source_code is not None:
            source_code = mod.source_code

            # Parse source code into a TestClass by writing to a temp file
            temp_path = None
            try:
                with tempfile.NamedTemporaryFile(delete=False, suffix='.ts', mode='w', encoding='utf-8') as tf:
                    tf.write(source_code)
                    temp_path = tf.name
                try:
                    test_class = ts_parser.parse_file(temp_path)
                except Exception:
                    test_class = None
            finally:
                if temp_path and os.path.exists(temp_path):
                    try:
                        os.remove(temp_path)
                    except Exception:
                        pass

        for target in matched_targets:
            framework = target["framework"]
            target_path = target["path"]
            found_smell = False

            if test_class is not None:
                for detector_cls in detectors:
                    detector = detector_cls()
                    try:
                        smells = detector.detect(test_class)
                    except Exception:
                        smells = None

                    if not smells:
                        continue

                    smell_type = detector_cls.__name__.replace('DetectorTS', '')
                    for s in smells:
                        found_smell = True
                        method_name = s.get('method_name') or s.get('method') or s.get('name') or 'unknown'
                        line_no = s.get('line') or s.get('line_number') or s.get('start_line') or None

                        rows.append((
                            framework,
                            repo_name,
                            target_path,
                            commit_hash,
                            commit_date,
                            commit_author,
                            commit_message,
                            nearest_release_tag,
                            nearest_release_date,
                            nearest_previous_release_tag,
                            nearest_previous_release_date,
                            smell_type,
                            method_name,
                            line_no,
                        ))

            if not found_smell:
                rows.append((
                    framework,
                    repo_name,
                    target_path,
                    commit_hash,
                    commit_date,
                    commit_author,
                    commit_message,
                    nearest_release_tag,
                    nearest_release_date,
                    nearest_previous_release_tag,
                    nearest_previous_release_date,
                    "NO_SMELL",
                    None,
                    None,
                ))

    return rows


def worker_loop(repo_root: str, repo_name: str, targets: list[dict], by_basename: dict[str, list[int]], release_index, task_q, completed_q, progress_q, repo_open_lock):
    # PyDriller's Git() writes into .git/config on init, which creates a config.lock.
    # With multiple processes on Windows this can race; serialize the init.
    with repo_open_lock:
        lock_path = os.path.join(repo_root, ".git", "config.lock")
        if os.path.exists(lock_path):
            try:
                os.remove(lock_path)
            except Exception:
                pass
        git = Git(repo_root)
    ts_parser = TypeScriptParser()

    while True:
        task = task_q.get()
        if task is None:
            break

        commit_idx, commit_hash = task

        try:
            commit = git.get_commit(commit_hash)
            rows = _extract_smell_rows(commit, repo_name, targets, by_basename, ts_parser, release_index)
        except Exception:
            # keep going even if a single commit fails
            rows = []
        finally:
            completed_q.put((commit_idx, rows))
            progress_q.put((1, len(rows)))


def writer_loop(db_path: str, result_q, batch_size: int = SQLITE_BATCH_SIZE):
    conn = sqlite3.connect(db_path, timeout=60)
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA temp_store=MEMORY;")
        conn.execute("PRAGMA cache_size=-200000;")

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS historical_smells (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                framework TEXT,
                repository TEXT,
                file TEXT,
                commit_hash TEXT,
                date TEXT,
                commit_author TEXT,
                commit_message TEXT,
                nearest_future_release_tag TEXT,
                nearest_future_release_date TEXT,
                nearest_previous_release_tag TEXT,
                nearest_previous_release_date TEXT,
                smell_type TEXT,
                method TEXT,
                line INTEGER
            )
            """
        )

        # Lightweight schema migration for existing DBs
        existing_cols = {row[1] for row in conn.execute("PRAGMA table_info(historical_smells);").fetchall()}
        if "commit_author" not in existing_cols:
            conn.execute("ALTER TABLE historical_smells ADD COLUMN commit_author TEXT;")
        if "commit_message" not in existing_cols:
            conn.execute("ALTER TABLE historical_smells ADD COLUMN commit_message TEXT;")
        if "nearest_future_release_tag" not in existing_cols:
            conn.execute("ALTER TABLE historical_smells ADD COLUMN nearest_future_release_tag TEXT;")
        if "nearest_future_release_date" not in existing_cols:
            conn.execute("ALTER TABLE historical_smells ADD COLUMN nearest_future_release_date TEXT;")
        if "nearest_previous_release_tag" not in existing_cols:
            conn.execute("ALTER TABLE historical_smells ADD COLUMN nearest_previous_release_tag TEXT;")
        if "nearest_previous_release_date" not in existing_cols:
            conn.execute("ALTER TABLE historical_smells ADD COLUMN nearest_previous_release_date TEXT;")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_hist_repo_file ON historical_smells(repository, file);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_hist_commit ON historical_smells(commit_hash);")
        conn.commit()

        buffer: list[tuple] = []
        while True:
            item = result_q.get()
            if item is None:
                break

            buffer.append(item)
            if len(buffer) >= batch_size:
                conn.executemany(
                    """
                    INSERT INTO historical_smells (
                        framework, repository, file, commit_hash, date, commit_author, commit_message, nearest_future_release_tag, nearest_future_release_date, nearest_previous_release_tag, nearest_previous_release_date, smell_type, method, line
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    buffer,
                )
                conn.commit()
                buffer.clear()

        if buffer:
            conn.executemany(
                """
                INSERT INTO historical_smells (
                    framework, repository, file, commit_hash, date, commit_author, commit_message, nearest_future_release_tag, nearest_future_release_date, nearest_previous_release_tag, nearest_previous_release_date, smell_type, method, line
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                buffer,
            )
            conn.commit()

    finally:
        conn.close()


def main():
    debug_file = _open_debug_file()

    _reset_db_file(DB_PATH, debug_file)

    # ========= INPUT CSV =========
    df = pd.read_csv(INPUT_CSV)
    file_columns = ["framework", "repository", "file_path"]
    unique_files = df[file_columns].drop_duplicates()
    unique_files_list = list(map(tuple, unique_files.to_numpy()))

    if TEST_MODE:
        # keep only first N repos (not just first N files)
        seen = set()
        trimmed = []
        for row in unique_files_list:
            repo = row[1]
            if repo not in seen:
                if len(seen) >= TEST_MAX_REPOS:
                    break
                seen.add(repo)
            trimmed.append(row)
        unique_files_list = trimmed

    debug_log(debug_file, f"[DEBUG] Total unique files: {len(unique_files_list)}")

    # group by repo
    files_by_repo: dict[str, list[tuple[str, str, str]]] = {}
    for framework, repo_name, file_path in unique_files_list:
        files_by_repo.setdefault(repo_name, []).append((framework, repo_name, file_path))

    os.makedirs(CLONE_ROOT, exist_ok=True)

    # start writer once
    result_q = mp.Queue(maxsize=RESULT_QUEUE_MAXSIZE)
    writer_p = mp.Process(target=writer_loop, args=(DB_PATH, result_q, SQLITE_BATCH_SIZE), name="writer")
    writer_p.start()

    global_start = time.time()
    global_smells = 0
    skipped_repos = 0

    try:
        for repo_idx, (repo_name, files_rows) in enumerate(files_by_repo.items(), start=1):
            repo_start = time.time()
            repo_url = f"https://github.com/{repo_name}.git"
            repo_short_name = repo_name.split('/')[-1]
            local_repo_path = os.path.join(CLONE_ROOT, repo_name.replace('/', '_'))

            workers: list[mp.Process] = []

            try:
                print("\n========================================")
                print(f"📦 Repository: {repo_name} ({repo_idx}/{len(files_by_repo)})")
                print("========================================")
                print("\n⏳ Cloning & scanning commit history...")

                repo_root = _clone_if_needed(repo_url, local_repo_path, repo_short_name, debug_file)
                debug_log(debug_file, f"[DEBUG] Using repo root: {repo_root}")

                targets, by_basename = _build_targets_index(files_rows, debug_file)

                for t in targets:
                    print(f"📄 File: {t['path']}")

                # commit hashes: only commits that touched at least one target file (much faster)
                git_repo = Git(repo_root)
                release_index = _build_release_index(git_repo, debug_file)

                try:
                    total_commits = git_repo.total_commits()
                except Exception:
                    total_commits = None

                commit_hashes = []
                seen_hashes: set[str] = set()
                for t in targets:
                    try:
                        for h in _commits_touching_path(git_repo, t["path"]):
                            if h and h not in seen_hashes:
                                seen_hashes.add(h)
                                commit_hashes.append(h)
                    except Exception:
                        # if git query fails for a path (e.g., never existed), ignore
                        continue

                if not commit_hashes:
                    # fallback to full history
                    commit_hashes = [c.hash for c in git_repo.get_list_commits()]

                commit_hashes.sort(key=lambda h: (_commit_unix_ts(git_repo, h), h))

                if MAX_COMMITS is not None:
                    commit_hashes = commit_hashes[:MAX_COMMITS]
                if total_commits is not None:
                    print(f"🔎 Total commits: {total_commits}")
                else:
                    print(f"🔎 Total commits: {len(commit_hashes)}")

                relevant_commits = len(commit_hashes)
                print(f"Commits to analyze: {relevant_commits}\n")

                task_q = mp.Queue(maxsize=TASK_QUEUE_MAXSIZE)
                completed_q = mp.Queue(maxsize=TASK_QUEUE_MAXSIZE)
                progress_q = mp.Queue()

                repo_open_lock = mp.Lock()

                workers = [
                    mp.Process(
                        target=worker_loop,
                        args=(repo_root, repo_name, targets, by_basename, release_index, task_q, completed_q, progress_q, repo_open_lock),
                        name=f"worker-{i+1}",
                    )
                    for i in range(NUM_WORKERS)
                ]
                for w in workers:
                    w.start()

                # enqueue tasks
                for idx, h in enumerate(commit_hashes):
                    task_q.put((idx, h))
                for _ in workers:
                    task_q.put(None)

                # progress
                processed = 0
                repo_smells = 0
                next_commit_idx = 0
                pending_commit_rows: dict[int, list[tuple]] = {}
                with tqdm(total=len(commit_hashes), desc="Progress", unit="commit") as pbar:
                    while processed < len(commit_hashes):
                        commit_idx, rows = completed_q.get()
                        pending_commit_rows[commit_idx] = rows
                        while next_commit_idx in pending_commit_rows:
                            ordered_rows = pending_commit_rows.pop(next_commit_idx)
                            for row in ordered_rows:
                                result_q.put(row)
                            next_commit_idx += 1

                        _, smells_in_commit = progress_q.get()
                        processed += 1
                        repo_smells += smells_in_commit
                        pbar.update(1)

                for w in workers:
                    w.join()

                repo_time = time.time() - repo_start
                global_smells += repo_smells

                print("\n📊 ===== REPOSITORY SUMMARY =====")
                print(f"Relevant commits: {relevant_commits}")
                print(f"Total smells found: {repo_smells}")
                print(f"Time: {repo_time:.2f} seconds")

            except KeyboardInterrupt:
                # Propagate Ctrl+C but make best effort to stop workers
                for w in workers:
                    try:
                        if w.is_alive():
                            w.terminate()
                    except Exception:
                        pass
                raise
            except Exception as e:
                skipped_repos += 1
                print(f"⚠️  Skipping repository due to error: {repo_name}")
                print(f"   Reason: {e}")
                debug_log(debug_file, f"[DEBUG] Skipping repo {repo_name}: {e}")

                # best-effort worker cleanup
                for w in workers:
                    try:
                        if w.is_alive():
                            w.terminate()
                    except Exception:
                        pass
                for w in workers:
                    try:
                        w.join(timeout=5)
                    except Exception:
                        pass

                # best-effort cleanup of partial clone to avoid reusing broken state
                try:
                    shutil.rmtree(local_repo_path, ignore_errors=True)
                except Exception:
                    pass

                continue

    finally:
        # stop writer
        result_q.put(None)
        writer_p.join()
        if debug_file:
            debug_file.close()

    global_time = time.time() - global_start
    print("\n========================================")
    print("✅ ANALYSIS COMPLETED")
    print(f"📁 Output: {DB_PATH}")
    print(f"🧾 Total smells inserted: {global_smells}")
    print(f"⏭ Skipped repositories: {skipped_repos}")
    print(f"⏱ Total execution time: {global_time:.2f} seconds")
    print("========================================")


if __name__ == "__main__":
    mp.freeze_support()
    main()