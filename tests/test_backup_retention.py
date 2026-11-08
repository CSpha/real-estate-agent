from datetime import datetime, timedelta

from app.ops import backup_retention as br


def name(day: datetime) -> str:
    return f"realestate_{day:%Y%m%dT%H%M%S}000000Z.dump"


def daily_names(count: int, start=datetime(2026, 11, 6, 10)) -> list[str]:
    return [name(start - timedelta(days=i)) for i in range(count)]


def make_dumps(tmp_path, names):
    for n in names:
        (tmp_path / n).write_bytes(b"x")


def test_keeps_newest_n_daily_and_deletes_rest():
    names = daily_names(20)
    kept, doomed = br.select_retention(names, keep_daily=5, keep_weekly=0)
    assert set(kept) == set(names[:5])
    assert set(doomed) == set(names[5:])


def test_weekly_keeps_newest_dump_of_each_iso_week():
    names = daily_names(30)
    kept, doomed = br.select_retention(names, keep_daily=2, keep_weekly=3)
    assert names[0] in kept and names[1] in kept
    for n, reason in kept.items():
        if reason != "weekly":
            continue
        week = br.parse_stamp(n).isocalendar()[:2]
        same_week = [m for m in names if br.parse_stamp(m).isocalendar()[:2] == week]
        assert n == same_week[0]
    assert set(kept) | set(doomed) == set(names)
    assert not set(kept) & set(doomed)


def test_newest_always_kept_even_with_zero_limits():
    names = daily_names(3)
    kept, doomed = br.select_retention(names, keep_daily=0, keep_weekly=0)
    assert list(kept) == [names[0]]
    assert set(doomed) == set(names[1:])


def test_protected_verified_dump_is_never_deleted():
    names = daily_names(10)
    kept, doomed = br.select_retention(
        names, keep_daily=2, keep_weekly=0, protected={names[7]}
    )
    assert kept[names[7]] == "verified"
    assert names[7] not in doomed


def test_unrecognized_files_are_ignored():
    kept, doomed = br.select_retention(
        ["notes.dump", "realestate_bad.dump"], keep_daily=1, keep_weekly=1
    )
    assert kept == {} and doomed == []


def test_prune_deletes_old_and_keeps_verified(tmp_path):
    names = daily_names(6)
    make_dumps(tmp_path, names)
    br.write_verified(tmp_path, names[5])
    rc = br.prune(tmp_path, "realestate", 2, 0, names[0], br.read_verified(tmp_path))
    assert rc == 0
    left = {p.name for p in tmp_path.glob("*.dump")}
    assert left == {names[0], names[1], names[5]}


def test_prune_refuses_when_given_dump_is_not_newest(tmp_path):
    names = daily_names(4)
    make_dumps(tmp_path, names)
    assert br.prune(tmp_path, "realestate", 1, 0, names[2], set()) == 1
    assert len(list(tmp_path.glob("*.dump"))) == 4


def test_prune_refuses_when_newest_missing(tmp_path):
    make_dumps(tmp_path, daily_names(3))
    assert br.prune(tmp_path, "realestate", 1, 0, "realestate_x.dump", set()) == 1
    assert len(list(tmp_path.glob("*.dump"))) == 3


def test_prune_ignores_other_databases(tmp_path):
    names = daily_names(3)
    make_dumps(tmp_path, names)
    other = tmp_path / "legacy_20200101T000000000000Z.dump"
    other.write_bytes(b"x")
    br.prune(tmp_path, "realestate", 1, 0, names[0], set())
    assert other.exists()


def test_copy_atomic_leaves_no_temp_file(tmp_path):
    src = tmp_path / "src" / "realestate_20261106T100000000000Z.dump"
    src.parent.mkdir()
    src.write_bytes(b"data")
    dest = tmp_path / "dest"
    dest.mkdir()
    final = br.copy_atomic(src, dest)
    assert final.read_bytes() == b"data"
    assert [p.name for p in dest.iterdir()] == [src.name]


def test_main_prune_and_bad_limits(tmp_path, capsys):
    names = daily_names(3)
    make_dumps(tmp_path, names)
    args = ["prune", str(tmp_path), "--db", "realestate", "--newest", names[0]]
    assert br.main(args + ["--keep-daily", "1", "--keep-weekly", "0"]) == 0
    assert "Retention deleted" in capsys.readouterr().out
    assert br.main(args + ["--keep-daily", "0"]) == 2
