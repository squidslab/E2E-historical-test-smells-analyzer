import argparse
import re
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
from matplotlib.ticker import MaxNLocator, StrMethodFormatter


SEPARATOR = "-------------------------------------------------------------------------------"


@dataclass
class CommitInfo:
    commit_hash: str
    commit_date: datetime | None
    smells_count: int
    smell_types: list[str]
    prev_release_tag: str | None
    prev_release_date: datetime | None
    future_release_tag: str | None
    future_release_date: datetime | None


@dataclass
class ReportData:
    repository: str
    file_name: str
    introduction_commits: int | None
    improving_commits: int | None
    developers: list[dict[str, str | float | bool | None]]
    commits: list[CommitInfo]


def _sanitize_filename(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9._-]", "_", value)


def _parse_datetime(value: str) -> datetime | None:
    raw = value.strip()
    if not raw:
        return None
    normalized = raw.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def _parse_int_after_colon(line: str) -> int | None:
    if ":" not in line:
        return None
    tail = line.split(":", 1)[1].strip()
    match = re.search(r"-?\d+", tail)
    if not match:
        return None
    return int(match.group())


def parse_report_file(report_path: Path) -> ReportData:
    lines = report_path.read_text(encoding="utf-8", errors="replace").splitlines()

    repository = "unknown_repository"
    file_name = report_path.stem
    introduction_commits = None
    improving_commits = None

    commits: list[CommitInfo] = []
    developers: list[dict[str, str | float | bool | None]] = []
    current_commit: dict | None = None
    current_developer: dict | None = None

    in_smell_list = False
    previous_non_empty = ""

    for raw_line in lines:
        line = raw_line.strip()

        if line.startswith("📦Repository:"):
            repository = line.split(":", 1)[1].strip() or repository
        elif line.startswith("📄File:"):
            file_name = line.split(":", 1)[1].strip() or file_name
        elif line.startswith("Introduction commits"):
            introduction_commits = _parse_int_after_colon(line)
        elif line.startswith("Improving commits"):
            improving_commits = _parse_int_after_colon(line)

        if line.startswith("🚹 Developer:"):
            if current_developer is not None:
                developers.append(current_developer)
            current_developer = {
                "author": line.split(":", 1)[1].strip() or "Unknown",
                "is_file_owner": None,
                "smell_intro_pct": 0.0,
            }

        if current_developer is not None:
            if line.startswith("File owner:"):
                flag = line.split(":", 1)[1].strip().upper()
                current_developer["is_file_owner"] = flag == "YES"
            elif line.startswith("Percentage of smells introduced:"):
                raw_pct = line.split(":", 1)[1].strip().replace("%", "")
                try:
                    current_developer["smell_intro_pct"] = float(raw_pct)
                except ValueError:
                    current_developer["smell_intro_pct"] = 0.0

        if line.startswith("🔎 Commit:"):
            if current_commit is not None:
                commits.append(
                    CommitInfo(
                        commit_hash=current_commit.get("commit_hash", "unknown"),
                        commit_date=current_commit.get("commit_date"),
                        smells_count=int(current_commit.get("smells_count", 0) or 0),
                        smell_types=current_commit.get("smell_types", []),
                        prev_release_tag=current_commit.get("prev_release_tag"),
                        prev_release_date=current_commit.get("prev_release_date"),
                        future_release_tag=current_commit.get("future_release_tag"),
                        future_release_date=current_commit.get("future_release_date"),
                    )
                )

            current_commit = {
                "commit_hash": line.split(":", 1)[1].strip(),
                "commit_date": None,
                "smells_count": 0,
                "smell_types": [],
                "prev_release_tag": None,
                "prev_release_date": None,
                "future_release_tag": None,
                "future_release_date": None,
            }
            in_smell_list = False

        if current_commit is not None:
            if line.startswith("Date:"):
                parsed_date = _parse_datetime(line.split(":", 1)[1].strip())
                if previous_non_empty.startswith("🔎 Commit:"):
                    current_commit["commit_date"] = parsed_date
                elif previous_non_empty.startswith("Nearest future release"):
                    current_commit["future_release_date"] = parsed_date
                elif previous_non_empty.startswith("Nearest previous release"):
                    current_commit["prev_release_date"] = parsed_date

            elif line.startswith("Nearest future release:"):
                current_commit["future_release_tag"] = line.split(":", 1)[1].strip() or None

            elif line.startswith("Nearest previous release:"):
                current_commit["prev_release_tag"] = line.split(":", 1)[1].strip() or None

            elif line.startswith("Number of smells present in the commit:"):
                parsed = _parse_int_after_colon(line)
                current_commit["smells_count"] = int(parsed or 0)

            elif line.startswith("Types of smells present in the commit:"):
                in_smell_list = True

            elif line.startswith("Most frequent smell type:") or line.startswith("✉") or line == SEPARATOR:
                in_smell_list = False

            elif in_smell_list and line.startswith("- "):
                smell_type = line[2:].strip()
                if smell_type:
                    current_commit["smell_types"].append(smell_type)

        if line:
            previous_non_empty = line

    if current_commit is not None:
        commits.append(
            CommitInfo(
                commit_hash=current_commit.get("commit_hash", "unknown"),
                commit_date=current_commit.get("commit_date"),
                smells_count=int(current_commit.get("smells_count", 0) or 0),
                smell_types=current_commit.get("smell_types", []),
                prev_release_tag=current_commit.get("prev_release_tag"),
                prev_release_date=current_commit.get("prev_release_date"),
                future_release_tag=current_commit.get("future_release_tag"),
                future_release_date=current_commit.get("future_release_date"),
            )
        )

    if current_developer is not None:
        developers.append(current_developer)

    commits.sort(key=lambda c: (c.commit_date or datetime.min, c.commit_hash))

    return ReportData(
        repository=repository,
        file_name=file_name,
        introduction_commits=introduction_commits,
        improving_commits=improving_commits,
        developers=developers,
        commits=commits,
    )


def _infer_intro_improve_counts(commits: list[CommitInfo]) -> tuple[int, int]:
    intro = 0
    improve = 0
    previous_smells: set[str] = set()

    for commit in commits:
        current_smells = set(commit.smell_types)
        if current_smells - previous_smells:
            intro += 1
        if previous_smells - current_smells:
            improve += 1
        previous_smells = current_smells

    return intro, improve


def plot_smell_distribution(report: ReportData, output_dir: Path) -> None:
    all_smells: list[str] = []
    for commit in report.commits:
        all_smells.extend(commit.smell_types)

    counter = Counter(all_smells)

    fig, ax = plt.subplots(figsize=(12, 6))
    if counter:
        labels, values = zip(*counter.most_common())
        ax.bar(labels, values, color="#3274A1")
        ax.set_ylabel("Occurrences across commits")
        ax.tick_params(axis="x", rotation=30)
    else:
        ax.text(0.5, 0.5, "No smell data found", ha="center", va="center", transform=ax.transAxes)

    ax.set_title("1) Smell Distribution")
    ax.set_xlabel("Smell type")
    ax.yaxis.set_major_locator(MaxNLocator(integer=True))
    fig.tight_layout()
    fig.savefig(output_dir / "1_smell_distribution_bar.png", dpi=180)
    plt.close(fig)


def plot_smell_evolution(report: ReportData, output_dir: Path) -> None:
    dated_commits = [c for c in report.commits if c.commit_date is not None]

    fig, ax = plt.subplots(figsize=(12, 6))
    if dated_commits:
        x = [c.commit_date for c in dated_commits]
        y = [c.smells_count for c in dated_commits]
        ax.plot(x, y, marker="o", linewidth=2, color="#2E8B57")

        # Draw dashed vertical lines for unique release dates found in the report.
        release_points: list[tuple[datetime, str]] = []
        seen: set[tuple[str, datetime]] = set()
        for commit in report.commits:
            if commit.prev_release_date is not None and commit.prev_release_tag is not None:
                key = (commit.prev_release_tag, commit.prev_release_date)
                if key not in seen:
                    seen.add(key)
                    release_points.append((commit.prev_release_date, commit.prev_release_tag))
            if commit.future_release_date is not None and commit.future_release_tag is not None:
                key = (commit.future_release_tag, commit.future_release_date)
                if key not in seen:
                    seen.add(key)
                    release_points.append((commit.future_release_date, commit.future_release_tag))

        release_points.sort(key=lambda item: item[0])
        for idx, (release_date, release_tag) in enumerate(release_points):
            ax.axvline(
                release_date,
                color="#8B0000",
                linestyle="--",
                linewidth=1.2,
                alpha=0.6,
                label="Release" if idx == 0 else None,
            )
            ax.text(
                release_date,
                0.02,
                release_tag,
                transform=ax.get_xaxis_transform(),
                rotation=90,
                ha="right",
                va="bottom",
                fontsize=8,
                color="#8B0000",
            )

        if release_points:
            ax.legend(loc="upper left")

        ax.set_ylabel("Number of smells present")
        ax.yaxis.set_major_locator(MaxNLocator(integer=True))
    else:
        ax.text(0.5, 0.5, "No dated commits found", ha="center", va="center", transform=ax.transAxes)

    ax.set_title("2) Smell Evolution Over Time")
    ax.set_xlabel("Commit date")
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(output_dir / "2_smell_evolution_line.png", dpi=180)
    plt.close(fig)


def plot_ownership(report: ReportData, output_dir: Path) -> None:
    fig, ax = plt.subplots(figsize=(12, 6))

    developers = report.developers
    if developers:
        ordered = sorted(
            developers,
            key=lambda d: float(d.get("smell_intro_pct", 0.0) or 0.0),
            reverse=True,
        )
        authors = [str(d.get("author", "Unknown")) for d in ordered]
        values = [float(d.get("smell_intro_pct", 0.0) or 0.0) for d in ordered]
        owners = {str(d.get("author", "")) for d in ordered if bool(d.get("is_file_owner", False))}

        colors = ["#d62728" if a in owners else "#4c78a8" for a in authors]
        bars = ax.barh(authors, values, color=colors)
        ax.invert_yaxis()

        for bar, value in zip(bars, values):
            ax.text(value + 0.5, bar.get_y() + bar.get_height() / 2, f"{value:.0f}%", va="center")

        for tick in ax.get_yticklabels():
            if tick.get_text() in owners:
                tick.set_color("#b22222")
                tick.set_fontweight("bold")

        ax.set_xlim(0, 100)
        ax.xaxis.set_major_locator(MaxNLocator(integer=True))
        ax.xaxis.set_major_formatter(StrMethodFormatter("{x:.0f}%"))
        ax.set_xlabel("Percentage of smell-introducing commits (%)")
        ax.set_ylabel("Developer")
    else:
        ax.text(0.5, 0.5, "No developer ownership data found", ha="center", va="center", transform=ax.transAxes)
        ax.set_xlabel("Percentage of smell-introducing commits (%)")
        ax.set_ylabel("Developer")

    ax.set_title("3) Ownership Plot")
    fig.tight_layout()
    fig.savefig(output_dir / "3_ownership_plot.png", dpi=180)
    plt.close(fig)


def plot_smells_vs_release_distance(report: ReportData, output_dir: Path) -> None:
    distances: list[float] = []
    smells: list[int] = []

    for commit in report.commits:
        if commit.commit_date is None:
            continue

        candidates: list[float] = []
        if commit.prev_release_date is not None:
            candidates.append(abs((commit.commit_date - commit.prev_release_date).total_seconds()) / 86400.0)
        if commit.future_release_date is not None:
            candidates.append(abs((commit.commit_date - commit.future_release_date).total_seconds()) / 86400.0)

        if not candidates:
            continue

        distances.append(min(candidates))
        smells.append(commit.smells_count)

    fig, ax = plt.subplots(figsize=(10, 6))
    if distances:
        ax.scatter(distances, smells, color="#8F63B8", alpha=0.75)
        if len(distances) > 1 and len(set(distances)) > 1:
            coeffs = np.polyfit(distances, smells, 1)
            trend = np.poly1d(coeffs)
            xs = np.linspace(min(distances), max(distances), 200)
            ax.plot(xs, trend(xs), linestyle="--", color="#333333", linewidth=1.5)
    else:
        ax.text(0.5, 0.5, "No release-distance data found", ha="center", va="center", transform=ax.transAxes)

    ax.set_title("4) Smells vs Release Distance")
    ax.set_xlabel("Distance from nearest release (days)")
    ax.set_ylabel("Number of smells present")
    ax.yaxis.set_major_locator(MaxNLocator(integer=True))
    fig.tight_layout()
    fig.savefig(output_dir / "4_smells_vs_release_distance.png", dpi=180)
    plt.close(fig)


def plot_smell_cooccurrence_heatmap(report: ReportData, output_dir: Path) -> None:
    smell_universe = sorted({smell for c in report.commits for smell in c.smell_types})

    fig, ax = plt.subplots(figsize=(10, 8))
    if smell_universe:
        index = {name: i for i, name in enumerate(smell_universe)}
        matrix = np.zeros((len(smell_universe), len(smell_universe)), dtype=int)

        for commit in report.commits:
            unique_smells = sorted(set(commit.smell_types))
            for i_name in unique_smells:
                i = index[i_name]
                for j_name in unique_smells:
                    j = index[j_name]
                    matrix[i, j] += 1

        sns.heatmap(
            matrix,
            xticklabels=smell_universe,
            yticklabels=smell_universe,
            annot=True,
            fmt="d",
            cmap="YlGnBu",
            square=True,
            cbar_kws={"label": "Co-occurrence across commits"},
            ax=ax,
        )
        cbar = ax.collections[0].colorbar
        min_count = int(matrix.min())
        max_count = int(matrix.max())
        if max_count <= min_count:
            ticks = [min_count]
        else:
            # Keep around <= 10 ticks while preserving integer values.
            step = max(1, int(np.ceil((max_count - min_count) / 10)))
            ticks = list(range(min_count, max_count + 1, step))
            if ticks[-1] != max_count:
                ticks.append(max_count)

        cbar.set_ticks(ticks)
        cbar.ax.yaxis.set_major_formatter(StrMethodFormatter("{x:.0f}"))
        cbar.update_ticks()
        ax.tick_params(axis="x", rotation=45)
        ax.tick_params(axis="y", rotation=0)
    else:
        ax.text(0.5, 0.5, "No smell data found", ha="center", va="center", transform=ax.transAxes)

    ax.set_title("5) Smell Co-occurrence Heatmap")
    fig.tight_layout()
    fig.savefig(output_dir / "5_smell_cooccurrence_heatmap.png", dpi=180)
    plt.close(fig)


def generate_plots_for_report(report_file: Path, output_root: Path) -> Path:
    report = parse_report_file(report_file)
    folder_name = f"{_sanitize_filename(report.repository)}_{_sanitize_filename(report.file_name)}"
    output_dir = output_root / folder_name
    output_dir.mkdir(parents=True, exist_ok=True)

    sns.set_theme(style="whitegrid", context="talk")

    plot_smell_distribution(report, output_dir)
    plot_smell_evolution(report, output_dir)
    plot_ownership(report, output_dir)
    plot_smells_vs_release_distance(report, output_dir)
    plot_smell_cooccurrence_heatmap(report, output_dir)

    return output_dir


def _collect_report_files(single_report: Path | None, reports_dir: Path) -> list[Path]:
    if single_report is not None:
        return [single_report]
    return sorted(reports_dir.rglob("*.txt"))


def _detect_dataset_from_report_path(report_file: Path, reports_dir: Path) -> str:
    lowered_parts = [part.lower() for part in report_file.parts]
    if "ts" in lowered_parts:
        return "ts"
    if "js" in lowered_parts:
        return "js"

    try:
        relative_parts = [part.lower() for part in report_file.relative_to(reports_dir).parts]
        if relative_parts:
            if relative_parts[0] == "ts":
                return "ts"
            if relative_parts[0] == "js":
                return "js"
    except ValueError:
        pass

    return "unknown"


def _resolve_from_project_root(path_value: Path, project_root: Path) -> Path:
    if path_value.is_absolute():
        return path_value
    return (project_root / path_value).resolve()


def main() -> None:
    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent

    parser = argparse.ArgumentParser(
        description="Generate 5 smell charts from textual report files (single file or batch mode)."
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=None,
        help="Path to a single report .txt file.",
    )
    parser.add_argument(
        "--reports-dir",
        type=Path,
        default=Path("analyses") / "reports",
        help="Root directory containing report .txt files (used in batch mode).",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path("analyses") / "plots",
        help="Root output directory. Plots are saved in output_root/<repo>_<file>/.",
    )

    args = parser.parse_args()

    reports_dir = _resolve_from_project_root(args.reports_dir, project_root)
    output_root_base = _resolve_from_project_root(args.output_root, project_root)
    single_report = _resolve_from_project_root(args.report, project_root) if args.report is not None else None

    report_files = _collect_report_files(single_report, reports_dir)
    if not report_files:
        raise ValueError("No report files found with the given input parameters.")

    ok = 0
    fail = 0

    for report_file in report_files:
        try:
            dataset = _detect_dataset_from_report_path(report_file, reports_dir)
            output_root = output_root_base / dataset
            out_dir = generate_plots_for_report(report_file, output_root)
            ok += 1
            print(f"[OK] {report_file} -> {out_dir}")
        except Exception as exc:
            fail += 1
            print(f"[ERROR] {report_file}: {exc}")

    print(f"Done. Generated: {ok}, Errors: {fail}")


if __name__ == "__main__":
    main()
