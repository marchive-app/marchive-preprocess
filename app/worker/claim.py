"""대기열 선점 — post_media 테이블을 큐로 쓴다.

브로커(Redis/Celery)를 두지 않는다. 진실의 원천이 MySQL 하나면
"DB 는 커밋됐는데 브로커 push 는 실패" 같은 어긋난 상태가 존재할 수 없다.
대신 워커가 주기적으로 PENDING 행을 집어간다.

선점의 핵심은 두 가지다.

 1) SELECT ... FOR UPDATE SKIP LOCKED  (MySQL 8.0+)
    다른 워커가 이미 잠근 행은 기다리지 않고 건너뛴다. 이게 브로커의
    배타적 배달을 대신한다. 5.7 에는 없어서 워커끼리 서로 막힌다.

 2) SELECT 와 UPDATE 가 반드시 한 트랜잭션 안에 있어야 한다.
    MySQL 에는 RETURNING 이 없어 UPDATE 만으로는 어떤 행을 집었는지 알 수 없다.
    SELECT 가 건 행 잠금이 COMMIT 까지 유지되어야 다른 워커가 그 행을 건너뛴다.
    (커밋 후 잠금은 풀리지만 그때는 이미 PROCESSING 이라 조건에 걸리지 않는다)

session_scope() 가 그 트랜잭션 경계다. ORM 이라 raw SQL 이 아니어도
원자성은 동일하다 — 잠금은 커넥션이 아니라 트랜잭션에 매인다.

**선점은 짧게 커밋하고 실제 처리는 이 모듈 밖에서 한다.**
다운로드/OCR 은 수 초~수십 초인데 그동안 잠금과 커넥션을 붙들면
InnoDB 의 undo log 가 계속 커지고 커넥션 풀이 마른다.

전제 두 가지가 아직 충족되지 않았다(2026-09-10 기준).
  - upload_status / ocr_status 에 인덱스가 없다. 없으면 선점 쿼리가 풀 스캔이고,
    FOR UPDATE 가 스쳐 간 행마다 잠금을 시도해 워커를 늘릴수록 서로 밟는다.
        ALTER TABLE post_media ADD INDEX idx_upload_status (upload_status);
        ALTER TABLE post_media ADD INDEX idx_ocr_status    (ocr_status);
  - claimed_at 컬럼이 없다(모델에도 주석 처리). 없으면 좀비 회수를 할 수 없어
    처리 중 죽은 워커가 잡고 있던 행이 PROCESSING 인 채로 영영 갇힌다.
        ALTER TABLE post_media ADD COLUMN claimed_at DATETIME NULL;
    아래 HAS_CLAIMED_AT 이 이 상태를 런타임에 감지해 동작을 바꾼다.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import Select, select, update
from sqlalchemy.orm import InstrumentedAttribute

from app.db.session import session_scope
from app.models import MediaType, PostMedia, ProcessStatus

logger = logging.getLogger(__name__)

__all__ = [
    "HAS_CLAIMED_AT",
    "build_claim_stmt",
    "claim_upload",
    "claim_ocr",
    "load",
    "finish_upload",
    "fail_upload",
    "finish_ocr",
    "fail_ocr",
    "reap_zombies",
]

# 스키마 주인이 ALTER 하고 모델의 주석을 푸는 순간 True 가 된다.
# 모듈 임포트 시점에 한 번만 본다(매핑은 이미 확정되어 있다).
HAS_CLAIMED_AT: bool = "claimed_at" in PostMedia.__table__.columns


def _utcnow() -> datetime:
    """타임존 없는 UTC 시각.

    DATETIME 컬럼에는 타임존이 붙지 않는다. aware datetime 을 그대로 넣으면
    드라이버가 오프셋을 붙여 보내는지 떼고 보내는지가 구현마다 다르다.
    커넥션은 이미 time_zone = '+00:00' 으로 고정돼 있으므로(session.py)
    여기서도 UTC 로 맞춘 naive 값을 넣어 NOW() 와 같은 기준을 쓴다.
    """
    return datetime.now(timezone.utc).replace(tzinfo=None)


# --- 선점 -------------------------------------------------------------------


def build_claim_stmt(
    column: InstrumentedAttribute[ProcessStatus],
    batch: int,
    *extra_where: Any,
) -> Select[tuple[int]]:
    """선점 SELECT 를 만든다. 테스트에서 SQL 모양을 검사할 수 있게 분리해 둔다.

    엔티티가 아니라 id 컬럼만 뽑는다. 어차피 id 만 쓰는데 객체 수십 개를 만들어
    identity map 에 올릴 이유가 없다.

    ORDER BY post_media_id 는 정렬 자체보다 교착 회피가 목적이다.
    모든 워커가 같은 순서로 행을 집으면 잠금 순서가 어긋나지 않는다.
    InnoDB 세컨더리 인덱스에는 PK 가 자동으로 붙으므로(= (status, post_media_id))
    상태 인덱스만 있으면 이 정렬은 공짜다.
    """
    return (
        select(PostMedia.post_media_id)
        .where(column == ProcessStatus.PENDING, *extra_where)
        .order_by(PostMedia.post_media_id)
        .limit(batch)
        .with_for_update(skip_locked=True)
    )


def _claim(
    column: InstrumentedAttribute[ProcessStatus],
    batch: int,
    *extra_where: Any,
) -> list[int]:
    """PENDING 행 batch 개를 잠그고 PROCESSING 으로 바꾼 뒤 id 만 돌려준다."""
    if batch < 1:
        raise ValueError(f"batch 는 1 이상이어야 한다 (받은 값: {batch})")

    with session_scope() as db:
        ids = list(db.execute(build_claim_stmt(column, batch, *extra_where)).scalars())
        if not ids:
            return []

        # 행마다 객체를 고쳐 flush 하면 UPDATE 가 행 수만큼 나간다.
        # batch=50 이면 UPDATE 50 개다. 한 문장으로 끝낸다.
        values: dict[str, Any] = {column.key: ProcessStatus.PROCESSING}
        if HAS_CLAIMED_AT:
            values["claimed_at"] = _utcnow()
        db.execute(
            update(PostMedia)
            .where(PostMedia.post_media_id.in_(ids))
            .values(**values)
            .execution_options(synchronize_session=False)
        )
        return ids


def claim_upload(batch: int) -> list[int]:
    """다운로드 대기 중인 미디어를 선점한다.

    ig_cdn_url 이 NULL 인 행은 내려받을 주소가 없으므로 애초에 집지 않는다.
    (집었다가 실패로 처리하면 FAILED 만 쌓이고 원인은 스키마 쪽에 있다)
    """
    return _claim(PostMedia.upload_status, batch, PostMedia.ig_cdn_url.is_not(None))


def claim_ocr(batch: int) -> list[int]:
    """OCR 대기 중인 미디어를 선점한다.

    조건 셋을 모두 만족해야 한다.
      - upload_status = DONE : S3 에 올라가 있어야 읽을 수 있다
      - media_key IS NOT NULL: 그 S3 키가 실제로 기록돼 있어야 한다
      - media_type = image   : stage_ocr 은 이미지 바이트를 그대로 읽는다.
                               영상은 키프레임 추출이 먼저라 아직 처리할 수 없다.
    """
    return _claim(
        PostMedia.ocr_status,
        batch,
        PostMedia.upload_status == ProcessStatus.DONE,
        PostMedia.media_key.is_not(None),
        PostMedia.media_type == MediaType.image,
    )


# --- 처리 대상 읽기 ----------------------------------------------------------


def load(ids: Sequence[int]) -> list[PostMedia]:
    """선점한 id 들의 행을 읽어온다.

    잠금은 이미 풀린 뒤다(선점 트랜잭션이 커밋됐다). 상태가 PROCESSING 이라
    다른 워커가 다시 집지 않으므로 잠금 없이 읽어도 안전하다.

    expire_on_commit=False 라서 세션이 닫힌 뒤에도 속성 접근이 된다.
    """
    if not ids:
        return []
    with session_scope() as db:
        return list(
            db.execute(
                select(PostMedia).where(PostMedia.post_media_id.in_(list(ids)))
            ).scalars()
        )


# --- 결과 반영 ---------------------------------------------------------------


def _update_one(post_media_id: int, **values: Any) -> None:
    if HAS_CLAIMED_AT:
        # 선점이 끝났으니 놓아준다. 남겨두면 좀비 회수가 멀쩡한 행을 집는다.
        values.setdefault("claimed_at", None)
    with session_scope() as db:
        db.execute(
            update(PostMedia)
            .where(PostMedia.post_media_id == post_media_id)
            .values(**values)
            .execution_options(synchronize_session=False)
        )


def finish_upload(post_media_id: int, media_key: str) -> None:
    _update_one(
        post_media_id,
        media_key=media_key,
        upload_status=ProcessStatus.DONE,
    )


def fail_upload(post_media_id: int, error: BaseException) -> None:
    """실패는 상태로만 남는다.

    post_media 에는 error 컬럼이 없어 사유는 로그에만 남는다. 나중에 원인을
    추적하려면 error / retry_count 컬럼이 필요하다(스키마 주인과 협의 대상).
    """
    logger.warning(
        "upload 실패 id=%s: %s: %s", post_media_id, type(error).__name__, error
    )
    _update_one(post_media_id, upload_status=ProcessStatus.FAILED)


def finish_ocr(post_media_id: int, lines: Sequence[str]) -> None:
    _update_one(
        post_media_id,
        ocr_text="\n".join(lines),
        ocr_status=ProcessStatus.DONE,
    )


def fail_ocr(post_media_id: int, error: BaseException) -> None:
    logger.warning(
        "ocr 실패 id=%s: %s: %s", post_media_id, type(error).__name__, error
    )
    _update_one(post_media_id, ocr_status=ProcessStatus.FAILED)


# --- 좀비 회수 ---------------------------------------------------------------


def reap_zombies(
    column: InstrumentedAttribute[ProcessStatus],
    older_than_minutes: int,
) -> int:
    """오래 PROCESSING 인 행을 PENDING 으로 되돌린다. 되돌린 행 수를 반환.

    워커가 상태를 PROCESSING 으로 바꾼 직후 죽으면 그 행은 PENDING 이 아니라
    아무도 다시 집지 않는다. 이 회수가 없으면 데이터가 조용히 사라진다.

    반환값이 매번 0 이 아니라면 뭔가 반복해서 죽고 있다는 신호다. 로그로 남긴다.
    (같은 행이 계속 워커를 죽이는 경우까지 잡으려면 retry_count 가 필요하다)
    """
    if not HAS_CLAIMED_AT:
        raise RuntimeError(
            "claimed_at 컬럼이 없어 좀비 회수를 할 수 없다. "
            "ALTER TABLE post_media ADD COLUMN claimed_at DATETIME NULL; 후 "
            "모델의 claimed_at 주석을 풀 것"
        )

    cutoff = _utcnow() - timedelta(minutes=older_than_minutes)
    with session_scope() as db:
        result = db.execute(
            update(PostMedia)
            .where(
                column == ProcessStatus.PROCESSING,
                PostMedia.claimed_at.is_not(None),
                PostMedia.claimed_at < cutoff,
            )
            .values(**{column.key: ProcessStatus.PENDING, "claimed_at": None})
            .execution_options(synchronize_session=False)
        )
        reaped = result.rowcount or 0

    if reaped:
        logger.warning(
            "좀비 회수: %s 행을 PENDING 으로 되돌림 (%s, %s분 초과)",
            reaped,
            column.key,
            older_than_minutes,
        )
    return reaped
