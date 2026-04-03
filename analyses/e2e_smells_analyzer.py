import argparse
import json
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import re

from pydriller import Repository


DB_DEFAULTS = {
	"ts": "historical_smellsTS.db",
	"js": "historical_smellsJS.db",
}

NO_SMELL = "NO_SMELL"
OWNER_THRESHOLD = 0.45


@dataclass
class CommitReport:
	commit_hash: str
	date: str
	author: str
	commit_message: str
	nearest_future_release_tag: str
	nearest_future_release_date: str
	nearest_previous_release_tag: str
	nearest_previous_release_date: str
	smells_count: int
	smell_types: list[str]
	most_frequent_smell: str


@dataclass
class DeveloperReport:
	author: str
	is_owner: bool
	is_creator: bool
	developer_type: str
	smells_introduced_pct: float
	ownership_pct: float


@dataclass
class FileReportData:
	repository: str
	file_name: str
	file_creator: str
	file_owners: list[str]
	ownership_map: dict[str, float]
	total_commits: int
	total_smell_commits: int
	no_smell_commits: int
	introduction_commits: int
	improving_commits: int
	worsening_commits: int
	top_inducing_dev: str
	top_inducing_pct: float
	file_deleted: bool
	commit_reports: list[CommitReport]
	developer_reports: list[DeveloperReport]


def _clean_author(value: str | None) -> str:
	if value is None:
		return "Unknown"
	cleaned = str(value).strip()
	return cleaned if cleaned else "Unknown"


def _clean_str(value: str | None, fallback: str = "N/A") -> str:
	if value is None:
		return fallback
	cleaned = str(value).strip()
	return cleaned if cleaned else fallback


def _safe_date_key(value: str | None) -> tuple[int, str]:
	raw = _clean_str(value, "")
	if not raw:
		return (1, "")

	normalized = raw.replace("Z", "+00:00")
	try:
		dt = datetime.fromisoformat(normalized)
		return (0, dt.isoformat())
	except ValueError:
		return (1, raw)


def _sanitize_filename(value: str) -> str:
	return re.sub(r"[^a-zA-Z0-9._-]", "_", value)


def _resolve_repo_root() -> Path:
	base_dir = Path(__file__).resolve().parent
	candidates = [
		base_dir.parent / "repos",
		base_dir / "repos",
		Path("repos"),
	]
	for candidate in candidates:
		if candidate.exists() and candidate.is_dir():
			return candidate.resolve()
	raise FileNotFoundError(
		"Cloned repositories folder not found. Expected a 'repos' directory near the project root."
	)


def _resolve_cloned_repository_path(repository: str, repos_root: Path) -> Path:
	def _find_git_repo_dir(base: Path) -> Path | None:
		if not (base.exists() and base.is_dir()):
			return None
		if (base / ".git").exists():
			return base.resolve()

		children = [p for p in base.iterdir() if p.is_dir()]
		if len(children) == 1 and (children[0] / ".git").exists():
			return children[0].resolve()
		return None

	clean_repo = repository.strip()
	candidates = [
		repos_root / clean_repo,
		repos_root / clean_repo.replace("/", "_"),
		repos_root / clean_repo.replace("/", "-"),
		repos_root / clean_repo.split("/")[-1],
	]

	for candidate in candidates:
		resolved = _find_git_repo_dir(candidate)
		if resolved:
			return resolved

	raise FileNotFoundError(
		f"Cloned repository not found for '{repository}' in {repos_root}. "
		"Expected a git repository folder under repos/."
	)


def _load_repo_smelly_commits(conn: sqlite3.Connection, repository: str) -> set[str]:
	rows = conn.execute(
		"""
		SELECT DISTINCT commit_hash
		FROM historical_smells
		WHERE repository = ?
		  AND smell_type <> ?
		""",
		(repository, NO_SMELL),
	).fetchall()
	return {str(row[0]) for row in rows}


def _build_newcomer_map_from_repo_history(
	repository: str,
	smelly_commits: set[str],
	repos_root: Path,
) -> dict[str, bool]:
	repo_path = _resolve_cloned_repository_path(repository, repos_root)
	commits_by_author: dict[str, list[str]] = defaultdict(list)

	for commit in Repository(str(repo_path)).traverse_commits():
		author = _clean_author(commit.author.name)
		commits_by_author[author].append(commit.hash)

	newcomer_map: dict[str, bool] = {}
	for author, commit_hashes_desc in commits_by_author.items():
		first_three_commits = list(reversed(commit_hashes_desc))[:3]
		newcomer_map[author] = any(ch in smelly_commits for ch in first_three_commits)

	return newcomer_map


def _connect(db_path: str) -> sqlite3.Connection:
	db_file = _resolve_db_path(db_path)
	return sqlite3.connect(str(db_file))


def _resolve_db_path(db_path: str) -> Path:
	db_file = Path(db_path)
	if db_file.exists():
		return db_file

	parent_db = Path("..") / db_file.name
	if parent_db.exists():
		return parent_db

	root_db = Path("../..") / db_file.name
	if root_db.exists():
		return root_db

	raise FileNotFoundError(
		f"Database not found: {db_file}\n"
		f"Also search in: {parent_db.resolve()} o {root_db.resolve()}\n"
		"You can specify the path with --db"
	)


def _fetch_rows(conn: sqlite3.Connection, repository: str, file_name: str) -> list[sqlite3.Row]:
	conn.row_factory = sqlite3.Row

	query_exact = """
		SELECT
			repository,
			file,
			commit_hash,
			date,
			COALESCE(NULLIF(TRIM(commit_author), ''), 'Unknown') AS commit_author,
			COALESCE(NULLIF(TRIM(file_creator), ''), '') AS file_creator,
			COALESCE(file_deleted, 0) AS file_deleted,
			nearest_future_release_tag,
			nearest_future_release_date,
			nearest_previous_release_tag,
			nearest_previous_release_date,
			smell_type,
			method,
			line,
			commit_message
		FROM historical_smells
		WHERE repository = ?
		  AND file = ?
		ORDER BY date, commit_hash
	"""

	rows = conn.execute(query_exact, (repository, file_name)).fetchall()
	if rows:
		return rows

	query_like = """
		SELECT
			repository,
			file,
			commit_hash,
			date,
			COALESCE(NULLIF(TRIM(commit_author), ''), 'Unknown') AS commit_author,
			COALESCE(NULLIF(TRIM(file_creator), ''), '') AS file_creator,
			COALESCE(file_deleted, 0) AS file_deleted,
			nearest_future_release_tag,
			nearest_future_release_date,
			nearest_previous_release_tag,
			nearest_previous_release_date,
			smell_type,
			method,
			line,
			commit_message
		FROM historical_smells
		WHERE repository = ?
		  AND lower(file) LIKE ?
		ORDER BY date, commit_hash
	"""
	return conn.execute(query_like, (repository, f"%{file_name.lower()}%")).fetchall()


def _build_commit_reports(
	rows: list[sqlite3.Row],
) -> list[CommitReport]:
	commit_groups: dict[str, list[sqlite3.Row]] = defaultdict(list)
	for row in rows:
		commit_groups[str(row["commit_hash"])].append(row)

	reports: list[CommitReport] = []
	for commit_hash, commit_rows in commit_groups.items():
		first = commit_rows[0]
		date = _clean_str(first["date"], "N/A")
		author = _clean_author(first["commit_author"])
		commit_message = _clean_str(first["commit_message"], "")

		smell_instances = []
		smell_types = []
		for row in commit_rows:
			smell_type = _clean_str(row["smell_type"], NO_SMELL)
			if smell_type == NO_SMELL:
				continue
			method = _clean_str(row["method"], "")
			line = "" if row["line"] is None else str(row["line"])
			smell_instances.append(f"{smell_type}|{method}|{line}")
			smell_types.append(smell_type)

		smell_types_counter = Counter(smell_types)
		distinct_smell_types = sorted(smell_types_counter.keys())
		if smell_types_counter:
			highest = max(smell_types_counter.values())
			top_smells = sorted([k for k, v in smell_types_counter.items() if v == highest])
			most_frequent = top_smells[0]
		else:
			most_frequent = "N/A"

		reports.append(
			CommitReport(
				commit_hash=commit_hash,
				date=date,
				author=author,
				commit_message=commit_message,
				nearest_future_release_tag=_clean_str(first["nearest_future_release_tag"]),
				nearest_future_release_date=_clean_str(first["nearest_future_release_date"]),
				nearest_previous_release_tag=_clean_str(first["nearest_previous_release_tag"]),
				nearest_previous_release_date=_clean_str(first["nearest_previous_release_date"]),
				smells_count=len(set(smell_instances)),
				smell_types=distinct_smell_types,
				most_frequent_smell=most_frequent,
			)
		)

	return sorted(reports, key=lambda r: (_safe_date_key(r.date), r.commit_hash))


def _compute_smell_transition_counts(rows: list[sqlite3.Row]) -> tuple[int, int, int]:
	commit_groups: dict[str, list[sqlite3.Row]] = defaultdict(list)
	for row in rows:
		commit_groups[str(row["commit_hash"])].append(row)

	ordered_commits = sorted(
		commit_groups.keys(),
		key=lambda ch: (_safe_date_key(commit_groups[ch][0]["date"]), ch),
	)

	introduction_commits = 0
	improving_commits = 0
	worsening_commits = 0
	previous_smells: set[str] = set()

	for commit_hash in ordered_commits:
		current_smells: set[str] = set()
		for row in commit_groups[commit_hash]:
			smell_type = _clean_str(row["smell_type"], NO_SMELL)
			if smell_type == NO_SMELL:
				continue
			method = _clean_str(row["method"], "")
			line = "" if row["line"] is None else str(row["line"])
			current_smells.add(f"{smell_type}|{method}|{line}")

		introduced = current_smells - previous_smells
		removed = previous_smells - current_smells

		if introduced:
			if previous_smells:
				worsening_commits += 1
			else:
				introduction_commits += 1
		if removed:
			improving_commits += 1

		previous_smells = current_smells

	return introduction_commits, improving_commits, worsening_commits


def _build_report_text(
	rows: list[sqlite3.Row],
	repository: str,
	file_name: str,
) -> str:
	report_data = _build_report_data(rows, repository, file_name)
	return _render_report_text(report_data)


def _build_report_data(
	rows: list[sqlite3.Row],
	repository: str,
	file_name: str,
	newcomer_map: dict[str, bool] | None = None,
) -> FileReportData:
	if not rows:
		raise ValueError(
			"No data found for the indicated filters. "
			f"repository='{repository}', file='{file_name}'."
		)

	unique_commits = sorted({str(r["commit_hash"]) for r in rows})
	total_commits = len(unique_commits)

	commit_to_author: dict[str, str] = {}
	for row in rows:
		ch = str(row["commit_hash"])
		if ch not in commit_to_author:
			commit_to_author[ch] = _clean_author(row["commit_author"])

	author_commit_count: Counter[str] = Counter(commit_to_author.values())
	ownership_map: dict[str, float] = {}
	for author, commits in author_commit_count.items():
		ownership_map[author] = (commits / total_commits) if total_commits > 0 else 0.0

	file_owners = [author for author, own in ownership_map.items() if own > OWNER_THRESHOLD]

	smell_commits_set: set[str] = set()
	smell_commits_by_author: Counter[str] = Counter()
	for row in rows:
		smell_type = _clean_str(row["smell_type"], NO_SMELL)
		if smell_type == NO_SMELL:
			continue
		commit_hash = str(row["commit_hash"])
		if commit_hash not in smell_commits_set:
			smell_commits_set.add(commit_hash)
			smell_commits_by_author[_clean_author(row["commit_author"])] += 1

	total_smell_commits = len(smell_commits_set)
	no_smell_commits = total_commits - total_smell_commits
	introduction_commits, improving_commits, worsening_commits = _compute_smell_transition_counts(rows)

	if smell_commits_by_author:
		max_value = max(smell_commits_by_author.values())
		top_inducing_devs = sorted([k for k, v in smell_commits_by_author.items() if v == max_value])
		top_inducing_dev = top_inducing_devs[0]
		top_inducing_pct = (max_value / total_smell_commits * 100.0) if total_smell_commits > 0 else 0.0
	else:
		top_inducing_dev = "N/A"
		top_inducing_pct = 0.0

	file_deleted = any(int(r["file_deleted"] or 0) == 1 for r in rows)

	creator_candidates = [_clean_author(r["file_creator"]) for r in rows if _clean_str(r["file_creator"], "")]
	if creator_candidates:
		file_creator = Counter(creator_candidates).most_common(1)[0][0]
	else:
		earliest = sorted(rows, key=lambda r: (_safe_date_key(r["date"]), str(r["commit_hash"])))
		file_creator = _clean_author(earliest[0]["commit_author"])

	commit_reports = _build_commit_reports(rows)

	commits_by_author_ordered: dict[str, list[CommitReport]] = defaultdict(list)
	smell_commit_set = {c.commit_hash for c in commit_reports if c.smells_count > 0}
	for commit in commit_reports:
		commits_by_author_ordered[commit.author].append(commit)

	developer_reports: list[DeveloperReport] = []
	for author in sorted(commits_by_author_ordered.keys()):
		author_commits = commits_by_author_ordered[author]
		is_newcomer = bool(newcomer_map.get(author, False)) if newcomer_map else False
		dev_smell_commits = sum(1 for c in author_commits if c.commit_hash in smell_commit_set)
		dev_smell_pct = (dev_smell_commits / total_smell_commits * 100.0) if total_smell_commits > 0 else 0.0

		developer_reports.append(
			DeveloperReport(
				author=author,
				is_owner=author in file_owners,
				is_creator=author == file_creator,
				developer_type="newcomer" if is_newcomer else "expert",
				smells_introduced_pct=dev_smell_pct,
				ownership_pct=ownership_map.get(author, 0.0) * 100.0,
			)
		)

	return FileReportData(
		repository=repository,
		file_name=file_name,
		file_creator=file_creator,
		file_owners=sorted(file_owners),
		ownership_map=ownership_map,
		total_commits=total_commits,
		total_smell_commits=total_smell_commits,
		no_smell_commits=no_smell_commits,
		introduction_commits=introduction_commits,
		improving_commits=improving_commits,
		worsening_commits=worsening_commits,
		top_inducing_dev=top_inducing_dev,
		top_inducing_pct=top_inducing_pct,
		file_deleted=file_deleted,
		commit_reports=commit_reports,
		developer_reports=developer_reports,
	)


def _render_report_text(report_data: FileReportData) -> str:
	repository = report_data.repository
	file_name = report_data.file_name

	lines: list[str] = []
	lines.append("-------------------------------------------------------------------------------")
	lines.append(f"📦Repository: {repository}")
	lines.append(f"📄File: {file_name}")
	lines.append("")
	lines.append(f"File creator (who introduced the file): {report_data.file_creator}")
	if report_data.file_owners:
		owner_entries = [
			f"{dev} ({report_data.ownership_map[dev] * 100:.2f}%)"
			for dev in report_data.file_owners
		]
		owner_repr = ", ".join(owner_entries)
	else:
		owner_repr = "No owner with ownership > 0.45"
	lines.append(
		"File owner (ownership = dev_commits / total_commits, owner if > 0.45): "
		f"{owner_repr}"
	)
	lines.append("")
	lines.append(f"Total number of commits involving the file: {report_data.total_commits}")
	lines.append(f"Number of commits with smells: {report_data.total_smell_commits}")
	lines.append(f"Number of commits without smells: {report_data.no_smell_commits}")
	lines.append(f"Introduction commits (new smells introduced): {report_data.introduction_commits}")
	lines.append(f"Improving commits (smells removed): {report_data.improving_commits}")
	lines.append(f"Worsening commits (smells added): {report_data.worsening_commits}")
	lines.append("")
	lines.append(
		"Developer with the highest number of inducing-smells: "
		f"{report_data.top_inducing_dev} | {report_data.top_inducing_pct:.2f}%"
	)
	lines.append("")
	lines.append(f"File deleted: {'YES' if report_data.file_deleted else 'NO'}")
	lines.append("")

	for commit in report_data.commit_reports:
		lines.append("-------------------------------------------------------------------------------")
		lines.append("")
		lines.append(f"🔎 Commit: {commit.commit_hash}")
		lines.append(f"Date: {commit.date}")
		lines.append(f"Author: {commit.author}")
		lines.append("")
		lines.append(f"Nearest future release: {commit.nearest_future_release_tag}")
		lines.append(f"Date: {commit.nearest_future_release_date}")
		lines.append("")
		lines.append(f"Nearest previous release: {commit.nearest_previous_release_tag}")
		lines.append(f"Date: {commit.nearest_previous_release_date}")
		lines.append("")
		lines.append(f"Number of smells present in the commit: {commit.smells_count}")
		lines.append("")
		lines.append("Types of smells present in the commit:")
		if commit.smell_types:
			for smell_type in commit.smell_types:
				lines.append(f"- {smell_type}")
		else:
			lines.append("- None")
		lines.append("")
		lines.append(f"Most frequent smell type: {commit.most_frequent_smell}")
		lines.append("")
		lines.append("✉️​ Comment: ")
		lines.append("")
		lines.append(f'"{commit.commit_message}"')
		lines.append("")

	for dev in report_data.developer_reports:
		lines.append("-------------------------------------------------------------------------------")
		lines.append("")
		lines.append(f"🚹 Developer: {dev.author}")
		lines.append(f"File owner: {'YES' if dev.is_owner else 'NO'}")
		lines.append(f"File creator: {'YES' if dev.is_creator else 'NO'}")
		lines.append(f"Type: {dev.developer_type}")
		lines.append(f"Percentage of smells introduced: {dev.smells_introduced_pct:.2f}%")
		lines.append(f"Ownership on file commits: {dev.ownership_pct:.2f}%")
		lines.append("")

	lines.append("-------------------------------------------------------------------------------")
	return "\n".join(lines)


def _init_output_db(conn: sqlite3.Connection) -> None:
	conn.execute(
		"""
		CREATE TABLE IF NOT EXISTS report_summary (
			dataset TEXT NOT NULL,
			repository TEXT NOT NULL,
			file_name TEXT NOT NULL,
			file_creator TEXT NOT NULL,
			file_deleted INTEGER NOT NULL,
			total_commits INTEGER NOT NULL,
			total_smell_commits INTEGER NOT NULL,
			no_smell_commits INTEGER NOT NULL,
			introduction_commits INTEGER NOT NULL,
			improving_commits INTEGER NOT NULL,
			worsening_commits INTEGER NOT NULL DEFAULT 0,
			top_inducing_dev TEXT NOT NULL,
			top_inducing_pct REAL NOT NULL,
			file_owners_json TEXT NOT NULL,
			source_db_path TEXT NOT NULL,
			generated_at TEXT NOT NULL,
			PRIMARY KEY(dataset, repository, file_name)
		)
		"""
	)

	table_info = conn.execute("PRAGMA table_info(report_summary)").fetchall()
	existing_columns = {row[1] for row in table_info}
	if "worsening_commits" not in existing_columns:
		conn.execute(
			"ALTER TABLE report_summary ADD COLUMN worsening_commits INTEGER NOT NULL DEFAULT 0"
		)

	conn.execute(
		"""
		CREATE TABLE IF NOT EXISTS report_commit_details (
			dataset TEXT NOT NULL,
			repository TEXT NOT NULL,
			file_name TEXT NOT NULL,
			commit_hash TEXT NOT NULL,
			date TEXT NOT NULL,
			author TEXT NOT NULL,
			commit_message TEXT NOT NULL,
			nearest_future_release_tag TEXT NOT NULL,
			nearest_future_release_date TEXT NOT NULL,
			nearest_previous_release_tag TEXT NOT NULL,
			nearest_previous_release_date TEXT NOT NULL,
			smells_count INTEGER NOT NULL,
			smell_types_json TEXT NOT NULL,
			most_frequent_smell TEXT NOT NULL,
			PRIMARY KEY(dataset, repository, file_name, commit_hash)
		)
		"""
	)

	conn.execute(
		"""
		CREATE TABLE IF NOT EXISTS report_developer_details (
			dataset TEXT NOT NULL,
			repository TEXT NOT NULL,
			file_name TEXT NOT NULL,
			author TEXT NOT NULL,
			is_owner INTEGER NOT NULL,
			is_creator INTEGER NOT NULL,
			developer_type TEXT NOT NULL,
			smells_introduced_pct REAL NOT NULL,
			ownership_pct REAL NOT NULL,
			PRIMARY KEY(dataset, repository, file_name, author)
		)
		"""
	)

	conn.commit()


def _persist_report_data(
	conn: sqlite3.Connection,
	report_data: FileReportData,
	dataset: str,
	source_db_path: str,
) -> None:
	_init_output_db(conn)
	generated_at = datetime.utcnow().isoformat(timespec="seconds") + "Z"
	owners_json = json.dumps(
		[
			{
				"author": owner,
				"ownership_pct": round(report_data.ownership_map.get(owner, 0.0) * 100.0, 2),
			}
			for owner in report_data.file_owners
		],
		ensure_ascii=True,
	)

	base_params = (dataset, report_data.repository, report_data.file_name)
	conn.execute(
		"DELETE FROM report_commit_details WHERE dataset = ? AND repository = ? AND file_name = ?",
		base_params,
	)
	conn.execute(
		"DELETE FROM report_developer_details WHERE dataset = ? AND repository = ? AND file_name = ?",
		base_params,
	)
	conn.execute(
		"DELETE FROM report_summary WHERE dataset = ? AND repository = ? AND file_name = ?",
		base_params,
	)

	conn.execute(
		"""
		INSERT INTO report_summary (
			dataset,
			repository,
			file_name,
			file_creator,
			file_deleted,
			total_commits,
			total_smell_commits,
			no_smell_commits,
			introduction_commits,
			improving_commits,
			worsening_commits,
			top_inducing_dev,
			top_inducing_pct,
			file_owners_json,
			source_db_path,
			generated_at
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		""",
		(
			dataset,
			report_data.repository,
			report_data.file_name,
			report_data.file_creator,
			1 if report_data.file_deleted else 0,
			report_data.total_commits,
			report_data.total_smell_commits,
			report_data.no_smell_commits,
			report_data.introduction_commits,
			report_data.improving_commits,
			report_data.worsening_commits,
			report_data.top_inducing_dev,
			report_data.top_inducing_pct,
			owners_json,
			source_db_path,
			generated_at,
		),
	)

	conn.executemany(
		"""
		INSERT INTO report_commit_details (
			dataset,
			repository,
			file_name,
			commit_hash,
			date,
			author,
			commit_message,
			nearest_future_release_tag,
			nearest_future_release_date,
			nearest_previous_release_tag,
			nearest_previous_release_date,
			smells_count,
			smell_types_json,
			most_frequent_smell
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		""",
		[
			(
				dataset,
				report_data.repository,
				report_data.file_name,
				commit.commit_hash,
				commit.date,
				commit.author,
				commit.commit_message,
				commit.nearest_future_release_tag,
				commit.nearest_future_release_date,
				commit.nearest_previous_release_tag,
				commit.nearest_previous_release_date,
				commit.smells_count,
				json.dumps(commit.smell_types, ensure_ascii=True),
				commit.most_frequent_smell,
			)
			for commit in report_data.commit_reports
		],
	)

	conn.executemany(
		"""
		INSERT INTO report_developer_details (
			dataset,
			repository,
			file_name,
			author,
			is_owner,
			is_creator,
			developer_type,
			smells_introduced_pct,
			ownership_pct
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		""",
		[
			(
				dataset,
				report_data.repository,
				report_data.file_name,
				dev.author,
				1 if dev.is_owner else 0,
				1 if dev.is_creator else 0,
				dev.developer_type,
				dev.smells_introduced_pct,
				dev.ownership_pct,
			)
			for dev in report_data.developer_reports
		],
	)

	conn.commit()


def main() -> None:
	parser = argparse.ArgumentParser(
		description="Generate a complete textual report on ownership and history smells for repository/file."
	)
	parser.add_argument("repository", nargs="?", help="Repository name (repository column in the DB)")
	parser.add_argument("file_name", nargs="?", help="File path or file basename")
	parser.add_argument(
		"--dataset",
		required=True,
		choices=["ts", "js"],
		help="Required: ts uses historical_smellsTS.db, js uses historical_smellsJS.db",
	)
	parser.add_argument(
		"--db",
		default=None,
		help="SQLite DB path. If omitted, uses the DB of the selected dataset.",
	)
	parser.add_argument(
		"--output",
		default=None,
		help="Optional .txt output file (single mode only).",
	)
	parser.add_argument(
		"--report-db",
		default=None,
		help="Optional SQLite output DB path for generated reports. Default: same DB selected with --db/--dataset.",
	)
	parser.add_argument(
		"--write-txt",
		action="store_true",
		help="Also generate .txt reports in batch mode (secondary output).",
	)

	args = parser.parse_args()
	db_path = args.db if args.db else DB_DEFAULTS[args.dataset]
	resolved_source_db = _resolve_db_path(db_path)
	base_dir = Path(__file__).resolve().parent
	out_dir = base_dir / "reports" / args.dataset
	out_dir.mkdir(parents=True, exist_ok=True)
	output_db_path = Path(args.report_db) if args.report_db else resolved_source_db
	if output_db_path.parent and str(output_db_path.parent) != ".":
		output_db_path.parent.mkdir(parents=True, exist_ok=True)

	with sqlite3.connect(str(output_db_path)) as report_conn:
		_init_output_db(report_conn)

	repos_root = _resolve_repo_root()
	repository_newcomer_cache: dict[str, dict[str, bool]] = {}

	if args.repository and args.file_name:
		with _connect(db_path) as conn:
			rows = _fetch_rows(conn, args.repository, args.file_name)
			smelly_commits = _load_repo_smelly_commits(conn, args.repository)

		newcomer_map = repository_newcomer_cache.get(args.repository)
		if newcomer_map is None:
			newcomer_map = _build_newcomer_map_from_repo_history(
				args.repository,
				smelly_commits,
				repos_root,
			)
			repository_newcomer_cache[args.repository] = newcomer_map

		report_data = _build_report_data(
			rows,
			args.repository,
			args.file_name,
			newcomer_map,
		)
		report_text = _render_report_text(report_data)
		print(report_text)

		with sqlite3.connect(str(output_db_path)) as report_conn:
			_persist_report_data(report_conn, report_data, args.dataset, db_path)

		if args.output:
			out_path = Path(args.output)
			if out_path.parent and str(out_path.parent) != ".":
				out_path.parent.mkdir(parents=True, exist_ok=True)
			out_path.write_text(report_text, encoding="utf-8")
		print("-------------------------")
		print(f"📦Repository: {args.repository}")
		print(f"📄File: {args.file_name}")
		print(f"✅Report persisted on DB: {output_db_path}")
		if args.output:
			print(f"📝Secondary TXT report: {args.output}")
		print("-------------------------")
		return

	if args.repository or args.file_name:
		parser.error("For single mode you must specify both repository and file_name.")

	print("[INFO] Batch mode: generating report for all repository/file pairs with smells...")
	with _connect(db_path) as conn:
		pairs = conn.execute(
			"""
			SELECT DISTINCT repository, file
			FROM historical_smells
			WHERE smell_type <> 'NO_SMELL'
			ORDER BY repository, file
			"""
		).fetchall()

	if not pairs:
		raise ValueError("No data available in the DB for batch mode.")

	ok = 0
	fail = 0
	batch_total = len(pairs)

	for repository, file_name in pairs:
		try:
			with _connect(db_path) as conn:
				rows = _fetch_rows(conn, repository, file_name)
				smelly_commits = _load_repo_smelly_commits(conn, repository)

			newcomer_map = repository_newcomer_cache.get(repository)
			if newcomer_map is None:
				newcomer_map = _build_newcomer_map_from_repo_history(
					repository,
					smelly_commits,
					repos_root,
				)
				repository_newcomer_cache[repository] = newcomer_map
			report_data = _build_report_data(
				rows,
				repository,
				file_name,
				newcomer_map,
			)

			with sqlite3.connect(str(output_db_path)) as report_conn:
				_persist_report_data(report_conn, report_data, args.dataset, db_path)

			if args.write_txt:
				report_text = _render_report_text(report_data)
				output_path = out_dir / f"{_sanitize_filename(repository)}_{_sanitize_filename(file_name)}.txt"
				output_path.write_text(report_text, encoding="utf-8")
			ok += 1
			print("-------------------------")
			print(f"📦Repository: {repository}")
			print(f"📄File: {file_name}")
			print("✅Report persisted on DB")
			if args.write_txt:
				print("📝Secondary TXT report generated")
		except Exception as exc:
			fail += 1
			print(f"[ERROR] {repository} | {file_name}: {exc}")

	print(f"\nBatch completed: {ok}/{batch_total} reports saved in DB, {fail} errors.")
	print(f"DB output: {output_db_path}")


if __name__ == "__main__":
	main()
