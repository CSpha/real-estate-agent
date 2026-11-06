"""Backup retention and off-machine copy helpers (standard library only).

Invoked by scripts/backup_postgres.sh inside the backup container:

    python3 backup_retention.py prune DIR --db NAME --newest FILE
    python3 backup_retention.py copy FILE DEST_DIR

Dump names look like ``<db>_<YYYYMMDDTHHMMSS...>Z.dump``; the timestamp in the
name decides age. Files that do not match are never touched.
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import sys
from datetime import datetime
from pathlib import Path

VERIFIED_MARKER = ".last_verified"
_STAMP = re.compile(r"_(\d{8}T\d{6})\d*Z\.dump$")


def parse_stamp(name: str) -> datetime | None:
    match = _STAMP.search(name)
    if match is None:
        return None
    try:
        return datetime.strptime(match.group(1), "%Y%m%dT%H%M%S")
    except ValueError:
        return None


def select_retention(
    names: list[str],
    keep_daily: int,
    keep_weekly: int,
    protected: set[str] | None = None,
) -> tuple[dict[str, str], list[str]]:
    """Return ({kept name: reason}, [names to delete]), newest first.

    Keeps the newest ``keep_daily`` dumps, the newest dump of each of the
    newest ``keep_weekly`` ISO weeks, the newest dump overall, and anything in
    ``protected``. Names without a parsable timestamp are ignored entirely.
    """
    protected = protected or set()
    dated = sorted(
        ((parse_stamp(n), n) for n in names if parse_stamp(n) is not None),
        reverse=True,
    )
    kept: dict[str, str] = {}
    if dated:
        kept[dated[0][1]] = "newest"
    for _, name in dated[: max(keep_daily, 0)]:
        kept.setdefault(name, "daily")
    weeks: list[tuple[int, int]] = []
    for stamp, name in dated:
        iso = stamp.isocalendar()
        week = (iso[0], iso[1])
        if week in weeks:
            continue
        if len(weeks) >= max(keep_weekly, 0):
            break
        weeks.append(week)
        kept.setdefault(name, "weekly")
    for _, name in dated:
        if name in protected:
            kept[name] = "verified"
    return kept, [n for _, n in dated if n not in kept]


def read_verified(directory: Path) -> set[str]:
    try:
        text = (directory / VERIFIED_MARKER).read_text().strip()
    except OSError:
        return set()
    return {text} if text else set()


def write_verified(directory: Path, dump_name: str) -> None:
    temp = directory / f"{VERIFIED_MARKER}.tmp.{os.getpid()}"
    temp.write_text(dump_name + "\n")
    os.replace(temp, directory / VERIFIED_MARKER)


def prune(
    directory: Path,
    db: str,
    keep_daily: int,
    keep_weekly: int,
    newest: str,
    protected: set[str],
) -> int:
    names = sorted(p.name for p in directory.glob(f"{db}_*.dump"))
    if newest not in names:
        print(f"Retention skipped in {directory}: {newest} is not present")
        return 1
    kept, doomed = select_retention(names, keep_daily, keep_weekly, protected)
    if newest in doomed or not kept:
        print(f"Retention skipped in {directory}: newest dump would be deleted")
        return 1
    if max(parse_stamp(n) for n in names if parse_stamp(n)) != parse_stamp(newest):
        print(f"Retention skipped in {directory}: {newest} is not the newest dump")
        return 1
    for name, reason in kept.items():
        print(f"Retention keep ({reason}): {directory / name}")
    for name in doomed:
        (directory / name).unlink()
        print(f"Retention deleted: {directory / name}")
    print(f"Retention in {directory}: kept {len(kept)}, deleted {len(doomed)}")
    return 0


def copy_atomic(src: Path, dest_dir: Path) -> Path:
    """Copy ``src`` into ``dest_dir`` via a temp file then rename."""
    final = dest_dir / src.name
    temp = dest_dir / f"{src.name}.tmp.{os.getpid()}"
    try:
        shutil.copyfile(src, temp)
        with open(temp, "r+b") as handle:
            os.fsync(handle.fileno())
        if temp.stat().st_size != src.stat().st_size:
            raise OSError("copied size does not match source")
        os.replace(temp, final)
    finally:
        temp.unlink(missing_ok=True)
    return final


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("prune")
    p.add_argument("directory", type=Path)
    p.add_argument("--db", required=True)
    p.add_argument("--newest", required=True, help="file name just written")
    p.add_argument("--keep-daily", type=int, default=14)
    p.add_argument("--keep-weekly", type=int, default=8)
    p.add_argument("--verified-file-dir", type=Path, help="holds the marker")
    c = sub.add_parser("copy")
    c.add_argument("src", type=Path)
    c.add_argument("dest_dir", type=Path)
    args = parser.parse_args(argv)
    try:
        if args.cmd == "copy":
            final = copy_atomic(args.src, args.dest_dir)
            print(f"Off-machine copy complete: {final}")
            return 0
        if args.keep_daily < 1 or args.keep_weekly < 0:
            print("BACKUP_KEEP_DAILY must be >= 1 and BACKUP_KEEP_WEEKLY >= 0")
            return 2
        marker_dir = args.verified_file_dir or args.directory
        return prune(
            args.directory,
            args.db,
            args.keep_daily,
            args.keep_weekly,
            args.newest,
            read_verified(marker_dir),
        )
    except OSError as exc:
        print(f"{args.cmd} failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
