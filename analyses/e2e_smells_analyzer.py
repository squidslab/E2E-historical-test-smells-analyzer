import argparse
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import re


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


def _connect(db_path: str) -> sqlite3.Connection:
	db_file = Path(db_path)
	if db_file.exists():
		return sqlite3.connect(str(db_file))

	parent_db = Path("..") / db_file.name
	if parent_db.exists():
		return sqlite3.connect(str(parent_db))

	root_db = Path("../..") / db_file.name
	if root_db.exists():
		return sqlite3.connect(str(root_db))

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


def _compute_smell_transition_counts(rows: list[sqlite3.Row]) -> tuple[int, int]:
	commit_groups: dict[str, list[sqlite3.Row]] = defaultdict(list)
	for row in rows:
		commit_groups[str(row["commit_hash"])].append(row)

	ordered_commits = sorted(
		commit_groups.keys(),
		key=lambda ch: (_safe_date_key(commit_groups[ch][0]["date"]), ch),
	)

	introduction_commits = 0
	improving_commits = 0
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
			introduction_commits += 1
		if removed:
			improving_commits += 1

		previous_smells = current_smells

	return introduction_commits, improving_commits


def _build_report_text(
	rows: list[sqlite3.Row],
	repository: str,
	file_name: str,
) -> str:
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
	introduction_commits, improving_commits = _compute_smell_transition_counts(rows)

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

	lines: list[str] = []
	lines.append("-------------------------------------------------------------------------------")
	lines.append(f"📦Repository: {repository}")
	lines.append(f"📄File: {file_name}")
	lines.append("")
	lines.append(f"File creator (who introduced the file): {file_creator}")
	if file_owners:
		owner_entries = [f"{dev} ({ownership_map[dev] * 100:.2f}%)" for dev in sorted(file_owners)]
		owner_repr = ", ".join(owner_entries)
	else:
		owner_repr = "No owner with ownership > 0.45"
	lines.append(
		"File owner (ownership = dev_commits / total_commits, owner if > 0.45): "
		f"{owner_repr}"
	)
	lines.append("")
	lines.append(f"Total number of commits involving the file: {total_commits}")
	lines.append(f"Number of commits with smells: {total_smell_commits}")
	lines.append(f"Number of commits without smells: {no_smell_commits}")
	lines.append(f"Introduction commits (new smells introduced): {introduction_commits}")
	lines.append(f"Improving commits (smells removed): {improving_commits}")
	lines.append("")
	lines.append(
		"Developer with the highest number of inducing-smells: "
		f"{top_inducing_dev} | {top_inducing_pct:.2f}%"
	)
	lines.append("")
	lines.append(f"File deleted: {'YES' if file_deleted else 'NO'}")
	lines.append("")

	for commit in commit_reports:
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


	for author in sorted(commits_by_author_ordered.keys()):
		author_commits = commits_by_author_ordered[author]
		first_three = author_commits[:3]
		is_newcomer = any(c.commit_hash in smell_commit_set for c in first_three)
		dev_smell_commits = sum(1 for c in author_commits if c.commit_hash in smell_commit_set)
		dev_smell_pct = (dev_smell_commits / total_smell_commits * 100.0) if total_smell_commits > 0 else 0.0

		lines.append("-------------------------------------------------------------------------------")
		lines.append("")
		lines.append(f"🚹 Developer: {author}")
		lines.append(f"File owner: {'YES' if author in file_owners else 'NO'}")
		lines.append(f"File creator: {'YES' if author == file_creator else 'NO'}")
		lines.append(f"Type: {'newcomer' if is_newcomer else 'expert'}")
		lines.append(f"Percentage of smells introduced: {dev_smell_pct:.2f}%")
		lines.append("")

	lines.append("-------------------------------------------------------------------------------")
	return "\n".join(lines)


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
		help="Output .txt file (valid in single mode). If omitted, uses reports/<dataset>/...",
	)

	args = parser.parse_args()
	db_path = args.db if args.db else DB_DEFAULTS[args.dataset]
	base_dir = Path(__file__).resolve().parent
	out_dir = base_dir / "reports" / args.dataset
	out_dir.mkdir(parents=True, exist_ok=True)


	if args.repository and args.file_name:
		with _connect(db_path) as conn:
			rows = _fetch_rows(conn, args.repository, args.file_name)

		report_text = _build_report_text(
			rows,
			args.repository,
			args.file_name,
		)
		print(report_text)

		default_output = out_dir / f"{_sanitize_filename(args.repository)}_{_sanitize_filename(args.file_name)}.txt"
		out_path = Path(args.output) if args.output else default_output
		if out_path.parent and str(out_path.parent) != ".":
			out_path.parent.mkdir(parents=True, exist_ok=True)
		out_path.write_text(report_text, encoding="utf-8")
		print("-------------------------")
		print(f"📦Repository: {args.repository}")
		print(f"📄File: {args.file_name}")
		print("✅Report generated")
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
			report_text = _build_report_text(
				rows,
				repository,
				file_name,
			)
			output_path = out_dir / f"{_sanitize_filename(repository)}_{_sanitize_filename(file_name)}.txt"
			output_path.write_text(report_text, encoding="utf-8")
			ok += 1
			print("-------------------------")
			print(f"📦Repository: {repository}")
			print(f"📄File: {file_name}")
			print("✅Report generated")
		except Exception as exc:
			fail += 1
			print(f"[ERROR] {repository} | {file_name}: {exc}")

	print(f"\nBatch completed: {ok} reports created, {fail} errors.")


if __name__ == "__main__":
	main()
