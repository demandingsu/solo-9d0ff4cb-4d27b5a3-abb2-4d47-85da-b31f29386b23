"""Citation-graph tests for the collector.

Covers the cataloguing requirements:

* source events carry a cited-paper id array; legacy events default to [];
* only the highest revision's *whole* citation group is kept, a new revision
  replaces the old group, and cross-page redelivery never duplicates rows;
* citation query distinguishes indexed / retracted / not_indexed, and the
  status flips live when a cited paper is later ingested or retracted without
  re-collecting the citing paper;
* references are published atomically with the library and completion time,
  so fetch failures, cursor expiry and resumed syncs keep serving the previous
  complete result.

The fixtures reuse the in-memory fake source machinery from test_sync.
"""
from __future__ import annotations

import threading

import httpx
import pytest
from fastapi.testclient import TestClient

from collector.app import db, sync
from collector.app.main import app
from tests.test_sync import FakeSource, _next_db_path


@pytest.fixture
def source():
    return FakeSource()


@pytest.fixture
def client(source, monkeypatch):
    transport = httpx.MockTransport(source.handler)
    real_client = httpx.Client

    def patched_client(*args, **kwargs):
        kwargs["transport"] = transport
        return real_client(*args, **kwargs)

    monkeypatch.setattr(httpx, "Client", patched_client)
    db_path = _next_db_path("refs")
    monkeypatch.setattr("collector.app.main.DB_PATH", db_path)
    monkeypatch.setattr(
        "collector.app.main.sync.start_background", lambda *a, **k: True
    )
    db.init_db(db_path)
    with TestClient(app) as c:
        c.db_path = db_path
        yield c
        for thread in threading.enumerate():
            if thread.name.startswith("sync-job-") and thread.is_alive():
                thread.join(timeout=10)
        assert not sync._running.locked(), "worker run lock leaked across tests"


def _run(client, job_id):
    sync.run_sync(job_id, client.db_path)


def _sync(client, source, end="2026-11-20T00:00:00Z"):
    job = client.post("/sync/jobs", json={"end": end}).json()
    _run(client, job["id"])
    assert client.get(f"/sync/jobs/{job['id']}").json()["status"] == "completed"
    return job


def _refs(client, paper_id, **params):
    return client.get(f"/papers/{paper_id}/references", params=params).json()


def _status_map(payload):
    return {item["cited_paper_id"]: item["status"] for item in payload["references"]}


# --------------------------------------------------------------------------- #
def test_legacy_events_default_to_empty_references(client, source):
    # No references argument -> the field is absent on the wire.
    source.add("P1", 1, False, "2026-11-01T08:00:00Z")
    _sync(client, source)

    payload = _refs(client, "P1")
    assert payload["reference_count"] == 0
    assert payload["references"] == []
    assert payload["rev"] == 1


def test_highest_revision_replaces_whole_group_and_dedupes(client, source):
    # rev 1: cites P2 and not-yet-indexed P9.
    source.add("P1", 1, False, "2026-11-01T08:00:00Z",
               references=["P2", "P9"])
    # rev 2: whole-group replacement; P9 dropped, P3 added; P2 duplicated in
    # the payload and must collapse to a single citation.
    source.add("P1", 2, False, "2026-11-02T08:00:00Z",
               references=["P2", "P2", "P3"])
    # Stale rev 1 redelivered late must not erase the rev-2 group.
    source.add("P1", 1, False, "2026-11-01T08:00:00Z",
               references=["P2", "P9"])
    source.add("P2", 1, False, "2026-11-01T09:00:00Z")
    source.add("P3", 1, False, "2026-11-01T10:00:00Z")

    _sync(client, source)
    payload = _refs(client, "P1")
    assert payload["rev"] == 2
    assert [i['cited_paper_id'] for i in payload['references']] == ["P2", "P3"]
    assert _status_map(payload) == {"P2": "indexed", "P3": "indexed"}

    # No duplicate rows even at the storage layer.
    conn = db.connect(client.db_path)
    try:
        rows = conn.execute(
            "SELECT cited_paper_id, COUNT(*) c FROM paper_references "
            "WHERE paper_id='P1' GROUP BY cited_paper_id"
        ).fetchall()
        assert all(r["c"] == 1 for r in rows)
    finally:
        conn.close()


def test_cross_page_redelivery_creates_no_duplicate_citations(client, source):
    # PAGE_SIZE=2 in FakeSource: spread more than two rows over pages and
    # redeliver the P1 event verbatim (exact duplicate, like the seed data).
    source.add("P1", 1, False, "2026-11-01T08:00:00Z",
               references=["P2", "P3", "P4"])
    source.add("P2", 1, False, "2026-11-01T09:00:00Z")
    source.add("P3", 1, False, "2026-11-01T10:00:00Z")
    source.add("P4", 1, False, "2026-11-01T11:00:00Z")
    source.add("P1", 1, False, "2026-11-01T08:00:00Z",
               references=["P2", "P3", "P4"])

    _sync(client, source)
    payload = _refs(client, "P1")
    assert [i['cited_paper_id'] for i in payload['references']] == ["P2", "P3", "P4"]
    assert payload["reference_count"] == 3


def test_three_statuses_are_distinguished_and_retracted_citing_410s(client, source):
    # P1 cites an indexed paper, a retracted paper and an unindexed one.
    source.add("P1", 1, False, "2026-11-01T08:00:00Z",
               references=["P2", "P3", "P9"])
    source.add("P2", 1, False, "2026-11-01T09:00:00Z")
    source.add("P3", 1, False, "2026-11-02T09:00:00Z")
    source.add("P3", 2, True, "2026-11-03T09:00:00Z",
               references=["P9"])
    # P1 itself gets retracted later, keeping the same citation set.
    source.add("P1", 2, True, "2026-11-04T09:00:00Z",
               references=["P2", "P3", "P9"])

    _sync(client, source)

    # Retracted citing paper is hidden by default, auditable with the flag.
    hidden = client.get("/papers/P1/references")
    assert hidden.status_code == 410
    payload = _refs(client, "P1", include_retracted="true")
    assert payload["is_retracted"] == 1 and payload["rev"] == 2
    statuses = _status_map(payload)
    assert statuses == {"P2": "indexed", "P3": "retracted", "P9": "not_indexed"}

    p3 = next(i for i in payload['references'] if i['cited_paper_id'] == "P3")
    assert p3['rev'] == 2 and p3['is_retracted'] == 1 and p3['title']
    p9 = next(i for i in payload['references'] if i['cited_paper_id'] == "P9")
    assert p9['rev'] is None and p9['title'] is None and p9['is_retracted'] is None

    # Unknown citing paper is 404.
    assert client.get("/papers/NOPE/references").status_code == 404


def test_cited_paper_ingestion_flips_status_without_recollecting_citer(client, source):
    # P1 cites P8 which does not exist yet.
    source.add("P1", 1, False, "2026-11-01T08:00:00Z",
               references=["P2", "P8"])
    source.add("P2", 1, False, "2026-11-01T09:00:00Z")
    _sync(client, source, end="2026-11-05T00:00:00Z")
    assert _status_map(_refs(client, "P1")) == {
        "P2": "indexed", "P8": "not_indexed",
    }

    # Later incremental window ingests P8 — P1 is never re-sent, yet its
    # citation query must reflect the newly published library.
    source.add("P8", 1, False, "2026-11-06T08:00:00Z")
    _sync(client, source, end="2026-11-10T00:00:00Z")
    payload = _refs(client, "P1")
    assert _status_map(payload) == {"P2": "indexed", "P8": "indexed"}
    p8 = next(i for i in payload['references'] if i['cited_paper_id'] == "P8")
    assert p8['rev'] == 1


def test_cited_paper_retraction_flips_status_live(client, source):
    source.add("P1", 1, False, "2026-11-01T08:00:00Z", references=["P2"])
    source.add("P2", 1, False, "2026-11-01T09:00:00Z")
    _sync(client, source, end="2026-11-05T00:00:00Z")
    assert _status_map(_refs(client, "P1")) == {"P2": "indexed"}

    # P2 retracted in a later window; P1 untouched.
    source.add("P2", 2, True, "2026-11-07T09:00:00Z")
    _sync(client, source, end="2026-11-10T00:00:00Z")
    assert _status_map(_refs(client, "P1")) == {"P2": "retracted"}


def test_revision_replaces_group_across_sync_windows(client, source):
    source.add("P1", 1, False, "2026-11-01T08:00:00Z",
               references=["P2", "P3"])
    source.add("P2", 1, False, "2026-11-01T09:00:00Z")
    source.add("P3", 1, False, "2026-11-01T10:00:00Z")
    _sync(client, source, end="2026-11-05T00:00:00Z")

    # New window: rev 2 replaces the group (P3 removed, P4 added).
    source.add("P1", 2, False, "2026-11-06T08:00:00Z",
               references=["P2", "P4"])
    source.add("P4", 1, False, "2026-11-06T09:00:00Z")
    _sync(client, source, end="2026-11-10T00:00:00Z")

    payload = _refs(client, "P1")
    assert payload["rev"] == 2
    assert [i['cited_paper_id'] for i in payload['references']] == ["P2", "P4"]
    # P3 must be fully gone from the committed graph.
    conn = db.connect(client.db_path)
    try:
        leftover = conn.execute(
            "SELECT COUNT(*) c FROM paper_references "
            "WHERE paper_id='P1' AND cited_paper_id='P3'"
        ).fetchone()["c"]
        assert leftover == 0
    finally:
        conn.close()


def test_failed_sync_keeps_previous_citation_result_hidden_staging(client, source):
    source.add("P1", 1, False, "2026-11-01T08:00:00Z", references=["P2"])
    source.add("P2", 1, False, "2026-11-01T09:00:00Z")
    _sync(client, source, end="2026-11-05T00:00:00Z")

    # Incremental window fails mid-way; P1's revision with a new citation set
    # and the new P5 paper must stay invisible while the old result is served.
    source.add("P1", 2, False, "2026-11-06T08:00:00Z",
               references=["P2", "P5"])
    source.add("P5", 1, False, "2026-11-06T09:00:00Z")
    source.add("P6", 1, False, "2026-11-06T10:00:00Z")
    source.fail_next = 1

    job = client.post("/sync/jobs", json={"end": "2026-11-10T00:00:00Z"}).json()
    _run(client, job["id"])
    assert client.get(f"/sync/jobs/{job['id']}").json()["status"] == "failed"

    # Previous complete citation set still served; P5 not visible anywhere.
    payload = _refs(client, "P1")
    assert payload["rev"] == 1
    assert [i['cited_paper_id'] for i in payload['references']] == ["P2"]
    assert client.get("/papers/P5").status_code == 404

    # Resume publishes the new group atomically.
    client.post(f"/sync/jobs/{job['id']}/resume")
    _run(client, job["id"])
    assert client.get(f"/sync/jobs/{job['id']}").json()["status"] == "completed"
    payload = _refs(client, "P1")
    assert payload["rev"] == 2
    assert _status_map(payload) == {"P2": "indexed", "P5": "indexed"}


def test_cursor_expiry_refetch_publishes_consistent_citations(client, source):
    source.add("P1", 1, False, "2026-11-01T08:00:00Z",
               references=["P2", "P3"])
    source.add("P2", 1, False, "2026-11-02T08:00:00Z")
    source.add("P3", 1, False, "2026-11-03T08:00:00Z")

    job = client.post("/sync/jobs", json={"end": "2026-11-05T00:00:00Z"}).json()
    job_id = job["id"]
    original_end = job["interval_end"]

    orig_fetch = sync._fetch_page

    def flaky_fetch(http_client, url, start, end, cursor):
        result = orig_fetch(http_client, url, start, end, cursor)
        if cursor is None:
            source.generation += 1  # invalidate the cursor of page 1
        return result

    sync._fetch_page = flaky_fetch
    try:
        _run(client, job_id)
    finally:
        sync._fetch_page = orig_fetch

    failed = client.get(f"/sync/jobs/{job_id}").json()
    assert failed["status"] == "failed" and failed["cursor_resets"] == 1
    # Staged citations were discarded together with staged papers.
    conn = db.connect(client.db_path)
    try:
        staged_refs = conn.execute(
            "SELECT COUNT(*) c FROM staged_references WHERE job_id=?", (job_id,)
        ).fetchone()["c"]
        assert staged_refs == 0
    finally:
        conn.close()
    # Nothing published yet: the citing paper is absent.
    assert client.get("/papers/P1/references").status_code == 404

    client.post(f"/sync/jobs/{job_id}/resume")
    _run(client, job_id)
    done = client.get(f"/sync/jobs/{job_id}").json()
    assert done["status"] == "completed" and done["interval_end"] == original_end
    payload = _refs(client, "P1")
    assert [i['cited_paper_id'] for i in payload['references']] == ["P2", "P3"]


def test_empty_reference_revision_replaces_previous_group(client, source):
    source.add("P1", 1, False, "2026-11-01T08:00:00Z", references=["P2"])
    source.add("P2", 1, False, "2026-11-01T09:00:00Z")
    _sync(client, source, end="2026-11-05T00:00:00Z")

    # A later revision legitimately drops every citation (explicit empty set).
    source.add("P1", 2, False, "2026-11-06T08:00:00Z", references=[])
    _sync(client, source, end="2026-11-10T00:00:00Z")
    payload = _refs(client, "P1")
    assert payload["rev"] == 2 and payload["reference_count"] == 0


def test_stale_revision_in_same_job_cannot_erase_higher_group(client, source):
    # Higher revision is delivered FIRST, then a stale lower revision with an
    # empty/legacy citation set arrives later in the same interval (mirrors the
    # seed-data redelivery pattern). The rev-2 group must survive both staging
    # and publication.
    source.add("P1", 2, False, "2026-11-02T08:00:00Z", references=["P2", "P3"])
    source.add("P2", 1, False, "2026-11-01T09:00:00Z")
    source.add("P3", 1, False, "2026-11-01T10:00:00Z")
    # Legacy event: references field absent entirely.
    source.add("P1", 1, False, "2026-11-01T08:00:00Z")

    _sync(client, source)
    payload = _refs(client, "P1")
    assert payload["rev"] == 2
    assert [i["cited_paper_id"] for i in payload["references"]] == ["P2", "P3"]


def test_stale_redelivery_in_later_window_keeps_published_group(client, source):
    source.add("P1", 1, False, "2026-11-01T08:00:00Z", references=["P2"])
    source.add("P2", 1, False, "2026-11-01T09:00:00Z")
    _sync(client, source, end="2026-11-05T00:00:00Z")

    # New window only redelivers the OLD rev (boundary re-fetch) with a
    # different group; nothing published may change, citations included.
    source.add("P1", 1, False, "2026-11-01T08:00:00Z", references=["P3"])
    source.add("P3", 1, False, "2026-11-06T10:00:00Z")
    _sync(client, source, end="2026-11-10T00:00:00Z")
    payload = _refs(client, "P1")
    assert payload["rev"] == 1
    assert [i["cited_paper_id"] for i in payload["references"]] == ["P2"]
