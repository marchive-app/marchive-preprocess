"""DB 와 모델 사이의 계약을 실제 커넥션으로 확인한다 — 전부 읽기 전용.

스키마의 주인은 우리가 아니므로(모델이 DB 를 따라간다), 여기서 검증하는 것은
'우리 코드가 가정하는 것이 실제 DB 에서 참인가' 하나다.

  1) 커넥션 타임존이 UTC 인가        — claimed_at 시각 비교의 전제
  2) MySQL 8.0 이상인가              — SKIP LOCKED 가 없으면 대기열이 성립하지 않는다
  3) claimed_at 이 실제로 있는가     — 모델에만 있고 DB 에 없으면 첫 쿼리에서 죽는다
  4) 상태 컬럼에 인덱스가 있는가     — 없으면 선점 쿼리가 풀 스캔이 된다
  5) 선점 쿼리가 그 인덱스를 타는가  — 있다고 타는 게 아니다. EXPLAIN 으로 본다
  6) SKIP LOCKED 가 실제로 겹치지 않는가 ★ 이 파일의 핵심

INSERT/UPDATE/DDL 을 하지 않으므로 운영 DB 에 그대로 돌려도 된다.
6) 은 잠금을 걸지만 커밋하지 않고 롤백하므로 데이터는 그대로다.
"""

from __future__ import annotations

import time

import pytest
from sqlalchemy import func, inspect, select, text

from app.db.session import get_session, session_scope
from app.models import PostMedia, ProcessStatus

TABLE = PostMedia.__tablename__


# --- 1) 커넥션 --------------------------------------------------------------


def test_session_timezone_is_utc() -> None:
    """DATETIME 에는 타임존이 없다. NOW() 의 의미는 커넥션 설정이 정한다.

    이게 UTC 가 아니면 좀비 회수(claimed_at < NOW() - INTERVAL n MINUTE)가
    서버 로캘에 따라 조용히 어긋난다. KST 라면 9시간이다.
    """
    with session_scope() as db:
        tz = db.execute(text("SELECT @@session.time_zone")).scalar_one()
    assert tz == "+00:00", f"세션 타임존이 UTC 가 아니다: {tz!r}"


def test_mysql_version_supports_skip_locked() -> None:
    """SKIP LOCKED 는 8.0 부터다. 5.7 이면 워커 2 대가 서로 막혀 대기열이 직렬화된다."""
    with session_scope() as db:
        version = db.execute(text("SELECT VERSION()")).scalar_one()
    major = int(version.split(".", 1)[0])
    assert major >= 8, f"MySQL 8.0 이상이 필요하다 (현재 {version})"


# --- 2) 스키마 계약 ---------------------------------------------------------


def test_table_exists(engine) -> None:
    assert inspect(engine).has_table(TABLE), f"'{TABLE}' 테이블이 없다"


def test_model_columns_exist_in_db(engine) -> None:
    """모델이 선언한 컬럼이 전부 DB 에 있는지.

    DB 에만 있는 컬럼은 문제 삼지 않는다(스키마 주인이 더 알고 있을 수 있다).
    반대 방향 — 모델에만 있는 컬럼 — 만이 우리 코드를 죽인다.
    """
    actual = {c["name"] for c in inspect(engine).get_columns(TABLE)}
    declared = {c.name for c in PostMedia.__table__.columns}
    missing = declared - actual
    assert not missing, f"모델에만 있고 DB 에 없는 컬럼: {sorted(missing)}"


def test_claimed_at_is_nullable_datetime(engine) -> None:
    """NULL = 아직 아무도 집지 않음. NOT NULL 이면 그 구분이 사라진다.

    아직 스키마 주인이 ALTER 를 하지 않았고 모델에도 주석 처리되어 있으면 skip 한다.
    모델의 주석을 푸는 순간 이 테스트가 살아나며 DB 에도 있는지 따진다.
    """
    if "claimed_at" not in PostMedia.__table__.columns:
        pytest.skip("모델에 claimed_at 이 아직 없다 (주석 처리됨)")

    by_name = {c["name"]: c for c in inspect(engine).get_columns(TABLE)}
    # next(...) 로 꺼내면 없을 때 StopIteration 이 나서 원인이 안 보인다.
    assert "claimed_at" in by_name, (
        "모델은 claimed_at 을 선언했는데 DB 에 없다 — "
        "ALTER TABLE post_media ADD COLUMN claimed_at DATETIME NULL; 이 필요하다"
    )
    col = by_name["claimed_at"]
    assert col["nullable"], "claimed_at 은 NULL 을 허용해야 한다"
    assert "datetime" in str(col["type"]).lower(), f"예상 밖 타입: {col['type']}"


@pytest.mark.parametrize("column", ["upload_status", "ocr_status"])
def test_status_column_is_indexed(engine, column: str) -> None:
    """선점 쿼리는 상태 컬럼만 훑는다. 인덱스가 없으면 풀 스캔이고,
    FOR UPDATE 가 스쳐 간 행마다 잠금을 시도해 워커끼리 서로 밟는다.

    선두 컬럼이면 된다 — InnoDB 세컨더리 인덱스에는 PK 가 자동으로 붙으므로
    KEY (upload_status) 는 물리적으로 (upload_status, post_media_id) 다.
    """
    leading = {
        idx["column_names"][0]
        for idx in inspect(engine).get_indexes(TABLE)
        if idx["column_names"]
    }
    assert column in leading, f"{column} 을 선두로 하는 인덱스가 없다 (선두들: {leading})"


# 옵티마이저는 테이블이 작으면 인덱스를 무시하는 게 정상이다.
# 그 판단이 의미를 가지려면 행이 어느 정도 있어야 한다.
_MIN_ROWS_FOR_PLAN = 1000


def test_claim_query_uses_the_index() -> None:
    """인덱스가 있다고 타는 것은 아니다. 실행 계획으로 확인한다.

    'key 가 NULL 이 아니다' 는 검사로는 부족하다. ORDER BY post_media_id 때문에
    옵티마이저가 PK 를 순회하며(type='index') 상태 조건은 Using where 로 사후
    필터링하는 계획을 고를 수 있는데, 이건 인덱스를 탄 게 아니라 풀 스캔이다.
    실제로 그렇게 통과한 적이 있어서 조건을 조인다.

      type='ref'  → 상태 인덱스로 등치 검색   ✅ 우리가 원하는 것
      type='range'→ 인덱스 범위 검색          ✅
      type='index'→ 인덱스 풀 스캔            ❌ 전부 읽는다
      type='ALL'  → 테이블 풀 스캔            ❌
    """
    with session_scope() as db:
        rows = db.execute(select(func.count()).select_from(PostMedia)).scalar_one()
        if rows < _MIN_ROWS_FOR_PLAN:
            pytest.skip(
                f"행이 {rows} 개뿐이라 실행 계획을 신뢰할 수 없다 "
                f"(최소 {_MIN_ROWS_FOR_PLAN} 필요)"
            )
        plan = db.execute(
            text(
                f"EXPLAIN SELECT post_media_id FROM {TABLE} "  # noqa: S608 - 테이블명은 상수
                "WHERE upload_status = 'PENDING' ORDER BY post_media_id LIMIT 10"
            )
        ).mappings().one()

    assert plan["type"] in ("ref", "range"), f"선점 쿼리가 전부 읽는다: {dict(plan)}"
    assert plan["key"] not in (None, "PRIMARY"), (
        f"상태 인덱스가 아니라 {plan['key']!r} 를 쓴다: {dict(plan)}"
    )


# --- 3) SKIP LOCKED 실동작 ---------------------------------------------------


def _claim_stmt(status: ProcessStatus, limit: int):
    """워커의 선점 쿼리와 같은 모양. 여기서는 UPDATE 없이 잠금만 건다."""
    return (
        select(PostMedia.post_media_id)
        .where(PostMedia.upload_status == status)
        .order_by(PostMedia.post_media_id)
        .limit(limit)
        .with_for_update(skip_locked=True)
    )


def _status_with_rows(minimum: int) -> ProcessStatus:
    """행이 minimum 개 이상 있는 상태를 하나 고른다. 없으면 skip."""
    with session_scope() as db:
        counts = dict(
            db.execute(
                select(PostMedia.upload_status, func.count()).group_by(
                    PostMedia.upload_status
                )
            ).all()
        )
    for status, n in sorted(counts.items(), key=lambda kv: -kv[1]):
        if n >= minimum:
            return status
    pytest.skip(f"행이 {minimum} 개 이상인 상태가 없다 (분포: {counts})")


@pytest.mark.parametrize("batch", [1, 3])
def test_skip_locked_returns_disjoint_rows(batch: int) -> None:
    """★ 핵심 — 워커 두 대가 같은 행을 집지 않는가.

    커밋하지 않고 양쪽 다 롤백하므로 데이터는 바뀌지 않는다.
    잠금은 트랜잭션이 끝날 때까지만 유지된다.
    """
    status = _status_with_rows(batch * 2)

    a, b = get_session(), get_session()
    try:
        first = a.execute(_claim_stmt(status, batch)).scalars().all()
        assert len(first) == batch, "첫 세션이 예상만큼 집지 못했다"

        # 두 번째 세션이 '기다리지 않고' 다른 행을 받아야 한다.
        started = time.perf_counter()
        second = b.execute(_claim_stmt(status, batch)).scalars().all()
        elapsed = time.perf_counter() - started

        assert not (set(first) & set(second)), (
            f"두 세션이 같은 행을 집었다: {sorted(set(first) & set(second))}"
        )
        assert len(second) == batch, "두 번째 세션이 남은 행을 집지 못했다"
        # SKIP LOCKED 가 빠졌다면 innodb_lock_wait_timeout(기본 50s)까지 블로킹된다.
        assert elapsed < 2.0, f"두 번째 세션이 대기했다 ({elapsed:.1f}s) — 잠금을 건너뛰지 않는다"
    finally:
        a.rollback()
        b.rollback()
        a.close()
        b.close()


def test_lock_is_released_after_rollback() -> None:
    """잠금이 트랜잭션 종료와 함께 풀리는지.

    풀리지 않으면 워커가 한 배치를 처리하는 동안 그 행들이 영구히 잠긴 것처럼
    보이게 되고, 위 테스트가 우연히 통과한 것인지 구분할 수 없다.
    """
    status = _status_with_rows(1)

    a = get_session()
    try:
        locked = a.execute(_claim_stmt(status, 1)).scalars().all()
    finally:
        a.rollback()
        a.close()

    b = get_session()
    try:
        again = b.execute(_claim_stmt(status, 1)).scalars().all()
    finally:
        b.rollback()
        b.close()

    assert again == locked, "잠금이 풀린 뒤에는 같은 행이 다시 잡혀야 한다"
