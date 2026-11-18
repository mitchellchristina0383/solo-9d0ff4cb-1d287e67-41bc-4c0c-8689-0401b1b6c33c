"""Tests for alert rules, windowing, acknowledgment and failure atomicity."""

from __future__ import annotations

import sqlite3
import threading
import time
from datetime import datetime, timedelta

import pytest

from app.indexer import (
    AlertAlreadyAcked,
    AlertNotFound,
    IndexManager,
    RuleExists,
)


class FakeClock:
    """Deterministic clock producing the same ISO format as utcnow()."""

    def __init__(self, start: str = "2026-01-01T00:00:00+00:00") -> None:
        self.moment = datetime.fromisoformat(start)

    def __call__(self) -> str:
        return self.moment.isoformat(timespec="seconds")

    def advance(self, seconds: float) -> None:
        self.moment += timedelta(seconds=seconds)


@pytest.fixture()
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture()
def manager(tmp_path, clock):
    idx = IndexManager(str(tmp_path / "logs.db"), now=clock)
    idx.initialize()
    yield idx
    idx.shutdown()


def wait_for(predicate, timeout=10.0, interval=0.02):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(interval)
    raise AssertionError("condition was not met before timeout")


def open_alerts(manager):
    return manager.list_alerts("open", limit=200)["items"]


def all_alerts(manager):
    return manager.list_alerts(None, limit=200)["items"]


def windows(manager, rule_id):
    with sqlite3.connect(manager.db_path) as conn:
        conn.row_factory = sqlite3.Row
        return [
            dict(row)
            for row in conn.execute(
                "SELECT * FROM alert_windows WHERE rule_id = ? ORDER BY id",
                (rule_id,),
            )
        ]


# ------------------------------------------------------------- registration
def test_rule_only_counts_logs_committed_after_registration(manager):
    manager.add_log("api", "error burst before the rule existed")
    rule = manager.create_rule("api", "error", 60, 2)
    assert rule["start_log_id"] == 1

    manager.add_log("api", "error one")
    assert open_alerts(manager) == []
    manager.add_log("api", "error two")
    alerts = open_alerts(manager)
    assert len(alerts) == 1
    # The pre-registration log is not part of the window.
    assert alerts[0]["hit_count"] == 2
    assert alerts[0]["first_log_id"] == 2


def test_duplicate_rule_for_same_service_is_rejected(manager):
    manager.create_rule("api", "error", 60, 2)
    with pytest.raises(RuleExists):
        manager.create_rule("api", "timeout", 30, 5)
    # A different service is fine.
    manager.create_rule("web", "error", 60, 2)
    assert len(manager.list_rules()) == 2


def test_rule_keyword_must_be_searchable(manager):
    with pytest.raises(ValueError):
        manager.create_rule("api", "!!!", 60, 2)
    with pytest.raises(ValueError):
        manager.create_rule("api", "   ", 60, 2)
    with pytest.raises(ValueError):
        manager.create_rule("api", "error", 0, 2)
    with pytest.raises(ValueError):
        manager.create_rule("api", "error", 60, 0)
    assert manager.list_rules() == []


def test_deleted_rule_stops_alerting_but_keeps_history(manager):
    rule = manager.create_rule("api", "error", 60, 1)
    manager.add_log("api", "error now")
    assert len(open_alerts(manager)) == 1

    assert manager.delete_rule(rule["id"]) is True
    assert manager.delete_rule(rule["id"]) is False
    manager.add_log("api", "error again")
    manager.add_log("api", "error thrice")
    # No new windows or alerts after deletion; the old alert survives.
    assert windows(manager, rule["id"]) == []
    assert len(all_alerts(manager)) == 1
    # Re-registration starts fresh from the current log id.
    again = manager.create_rule("api", "error", 60, 1)
    assert again["start_log_id"] == 3


# ----------------------------------------------------------------- matching
def test_keyword_matching_follows_search_semantics(manager):
    manager.create_rule("api", "timeout", 60, 1)
    manager.create_rule("auth", "登录失败", 60, 1)
    # Latin prefix matching, case-insensitive.
    manager.add_log("api", "Upstream TIMEOUTS after 30s")
    # CJK bigram matching.
    manager.add_log("auth", "用户登录失败：密码错误")
    # Non-matching logs open nothing.
    manager.add_log("api", "everything is fine")
    manager.add_log("auth", "用户退出登录")
    alerts = open_alerts(manager)
    assert len(alerts) == 2
    assert {a["service"] for a in alerts} == {"api", "auth"}


def test_rule_is_scoped_to_its_service(manager):
    manager.create_rule("api", "error", 60, 1)
    manager.add_log("other", "error but for another service")
    assert open_alerts(manager) == []
    manager.add_log("api", "error here")
    assert len(open_alerts(manager)) == 1


# ----------------------------------------------------------------- windows
def test_threshold_opens_exactly_one_alert_per_window(manager, clock):
    rule = manager.create_rule("api", "error", 60, 3)
    for _ in range(2):
        manager.add_log("api", "error hit")
    assert open_alerts(manager) == []
    manager.add_log("api", "error hit")
    assert len(open_alerts(manager)) == 1
    # Further hits in the same window keep counting but never re-open.
    manager.add_log("api", "error hit")
    manager.add_log("api", "error hit")
    assert len(open_alerts(manager)) == 1
    [window] = windows(manager, rule["id"])
    assert window["hit_count"] == 5
    assert window["alerted"] == 1


def test_window_is_non_overlapping_and_anchored_at_first_hit(manager, clock):
    rule = manager.create_rule("api", "error", 60, 2)
    manager.add_log("api", "error a")  # t=0, anchors window [0, 60)
    clock.advance(59)
    manager.add_log("api", "error b")  # t=59, still inside -> alert
    assert len(open_alerts(manager)) == 1

    clock.advance(1)  # t=60: previous window ended exactly here
    manager.add_log("api", "error c")  # anchors a new window [60, 120)
    assert len(open_alerts(manager)) == 1  # count 1 in the new window
    clock.advance(59)
    manager.add_log("api", "error d")  # t=119 -> second window reaches 2
    alerts = open_alerts(manager)
    assert len(alerts) == 2
    assert alerts[0]["window_start"] != alerts[1]["window_start"]

    wins = windows(manager, rule["id"])
    assert len(wins) == 2
    assert wins[0]["window_end"] == wins[1]["window_start"]


def test_expired_window_without_threshold_opens_nothing(manager, clock):
    manager.create_rule("api", "error", 30, 2)
    manager.add_log("api", "error one")
    clock.advance(31)
    manager.add_log("api", "error two")  # first window expired, count restarts
    assert open_alerts(manager) == []
    manager.add_log("api", "error three")
    assert len(open_alerts(manager)) == 1


def test_threshold_one_alerts_on_first_hit(manager):
    manager.create_rule("api", "error", 60, 1)
    manager.add_log("api", "single error")
    alerts = open_alerts(manager)
    assert len(alerts) == 1
    assert alerts[0]["hit_count"] == 1


# -------------------------------------------------------------- acknowledge
def test_ack_then_same_window_stays_silent_next_window_alerts(manager, clock):
    manager.create_rule("api", "error", 60, 2)
    manager.add_log("api", "error 1")
    manager.add_log("api", "error 2")
    [alert] = open_alerts(manager)

    acked = manager.ack_alert(alert["id"])
    assert acked["status"] == "ack"
    assert acked["acked_at"] is not None
    # Acknowledged: more hits in the same window must not open another alert.
    manager.add_log("api", "error 3")
    manager.add_log("api", "error 4")
    assert all_alerts(manager).__len__() == 1
    # The next window reaching the threshold opens a fresh alert.
    clock.advance(61)
    manager.add_log("api", "error 5")
    manager.add_log("api", "error 6")
    alerts = all_alerts(manager)
    assert len(alerts) == 2
    statuses = {a["status"] for a in alerts}
    assert statuses == {"open", "ack"}


def test_ack_is_state_based(manager):
    manager.create_rule("api", "error", 60, 1)
    manager.add_log("api", "error")
    [alert] = open_alerts(manager)
    with pytest.raises(AlertNotFound):
        manager.ack_alert(alert["id"] + 999)
    manager.ack_alert(alert["id"])
    with pytest.raises(AlertAlreadyAcked):
        manager.ack_alert(alert["id"])


def test_concurrent_acks_settle_exactly_once(manager):
    manager.create_rule("api", "error", 60, 1)
    manager.add_log("api", "error")
    [alert] = open_alerts(manager)
    outcomes = []
    barrier = threading.Barrier(2)

    def ack():
        barrier.wait()
        try:
            manager.ack_alert(alert["id"])
            outcomes.append("ok")
        except AlertAlreadyAcked:
            outcomes.append("late")

    threads = [threading.Thread(target=ack) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert sorted(outcomes) == ["late", "ok"]
    assert manager.list_alerts("ack")["total"] == 1


# -------------------------------------------------------------- interleaving
def test_rule_registration_interleaved_with_a_blocked_write(manager, clock):
    """A log whose commit precedes the rule's commit is never counted.

    The log write is held inside its transaction while the rule registration
    waits on the write lock; once the log commits, the rule's start snapshot
    includes it.
    """
    entered = threading.Event()
    release = threading.Event()

    def hook(event, payload):
        if event == "before_commit":
            entered.set()
            release.wait(10)

    manager.write_hook = hook
    writer = threading.Thread(
        target=lambda: manager.add_log("api", "error in flight")
    )
    writer.start()
    assert entered.wait(5)

    registered = {}

    def register():
        registered["rule"] = manager.create_rule("api", "error", 60, 1)

    registrar = threading.Thread(target=register)
    registrar.start()
    # The registrar is blocked behind the in-flight write.
    time.sleep(0.1)
    assert "rule" not in registered
    release.set()
    writer.join(5)
    registrar.join(5)
    manager.write_hook = None

    rule = registered["rule"]
    assert rule["start_log_id"] == 1  # the in-flight log committed first
    # Threshold is 1, yet the pre-registration log opened nothing.
    assert open_alerts(manager) == []
    # Logs committed after the rule are evaluated.
    manager.add_log("api", "error after registration")
    assert len(open_alerts(manager)) == 1


def test_alerting_continues_through_index_rebuild_without_duplicates(manager):
    manager.create_rule("api", "error", 3600, 3)
    manager.add_log("api", "error one")
    manager.add_log("api", "error two")
    old = manager.status()["active"]["id"]

    # Hold the builder so writes interleave with the rebuild.
    held = threading.Event()
    seen = threading.Event()

    def hook(event, payload):
        if event == "batch":
            seen.set()
            held.wait(10)

    manager.build_hook = hook
    manager.start_rebuild()
    assert seen.wait(5)
    # These writes cross the threshold while the rebuild is running.
    manager.add_log("api", "error three")
    manager.add_log("api", "error four")
    held.set()
    wait_for(lambda: manager.status()["active"]["id"] != old)

    # Exactly one alert, opened once; the rebuild did not re-evaluate rules.
    alerts = open_alerts(manager)
    assert len(alerts) == 1
    assert alerts[0]["hit_count"] == 3
    # Everything is searchable in the new index.
    assert manager.search("error", "api")["total"] == 4

    # A second rebuild over the same logs still opens nothing new.
    manager.build_hook = None
    old = manager.status()["active"]["id"]
    manager.start_rebuild()
    wait_for(lambda: manager.status()["active"]["id"] != old)
    assert len(open_alerts(manager)) == 1


# -------------------------------------------------------------- failure mode
def test_failed_write_leaves_no_partial_results(manager):
    manager.create_rule("api", "error", 60, 1)

    def boom(event, payload):
        raise RuntimeError("模拟写入失败")

    manager.write_hook = boom
    with pytest.raises(RuntimeError, match="模拟写入失败"):
        manager.add_log("api", "error that must be rolled back")
    manager.write_hook = None

    # No log row, no index entry, no window, no alert.
    assert manager.search("error", None)["total"] == 0
    assert manager.status()["total_logs"] == 0
    assert all_alerts(manager) == []
    with sqlite3.connect(manager.db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM alert_windows").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM logs").fetchone()[0] == 0

    # The same write succeeds once the failure is gone.
    manager.add_log("api", "error that must be rolled back")
    assert manager.search("error", None)["total"] == 1
    assert len(open_alerts(manager)) == 1


def test_status_reports_open_alert_count(manager):
    assert manager.status()["open_alerts"] == 0
    manager.create_rule("api", "error", 60, 1)
    manager.add_log("api", "error")
    assert manager.status()["open_alerts"] == 1
    [alert] = open_alerts(manager)
    manager.ack_alert(alert["id"])
    assert manager.status()["open_alerts"] == 0


def test_alert_listing_filters_and_paginates(manager, clock):
    manager.create_rule("api", "error", 10, 1)
    for _ in range(3):
        manager.add_log("api", "error")
        clock.advance(11)  # each hit lands in its own window
    assert manager.list_alerts("open")["total"] == 3
    [first] = manager.list_alerts("open", limit=1)["items"]
    manager.ack_alert(first["id"])
    assert manager.list_alerts("open")["total"] == 2
    assert manager.list_alerts("ack")["total"] == 1
    page = manager.list_alerts(None, limit=2, offset=2)
    assert page["total"] == 3
    assert len(page["items"]) == 1
