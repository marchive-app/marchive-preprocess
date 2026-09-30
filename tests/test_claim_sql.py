"""선점 쿼리의 SQL 모양을 검사한다 — DB 없이 돈다.

여기서 잡으려는 것은 "조용히 사라지는 절"이다.
with_for_update(skip_locked=True) 를 빠뜨리거나, 방언이 MySQL 이 아니어서
SKIP LOCKED 가 렌더링되지 않으면 코드는 멀쩡히 돌지만 워커 두 대가
같은 행을 중복 처리한다. 테스트가 없으면 운영에서야 알게 된다.

실제 잠금 동작은 DB 가 있어야 하므로 test_db_contract.py 에서 따로 본다.
"""

from __future__ import annotations

from sqlalchemy.dialects import mysql

from app.models import MediaType, PostMedia, ProcessStatus
from app.worker.claim import build_claim_stmt


def _sql(stmt) -> str:
    return str(
        stmt.compile(
            dialect=mysql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )


def test_claim_stmt_locks_and_skips() -> None:
    sql = _sql(build_claim_stmt(PostMedia.upload_status, 5))
    assert "FOR UPDATE SKIP LOCKED" in sql, sql


def test_claim_stmt_selects_only_id() -> None:
    """엔티티를 통째로 뽑으면 쓰지도 않을 객체를 batch 만큼 만든다."""
    sql = _sql(build_claim_stmt(PostMedia.upload_status, 5))
    selected = sql.split("FROM")[0]
    assert "post_media_id" in selected
    assert "ocr_text" not in selected, f"id 외의 컬럼까지 뽑는다: {selected}"


def test_claim_stmt_orders_by_pk() -> None:
    """워커들이 같은 순서로 집어야 잠금 순서가 어긋나지 않는다."""
    sql = _sql(build_claim_stmt(PostMedia.upload_status, 5))
    assert "ORDER BY post_media.post_media_id" in sql, sql


def test_claim_stmt_applies_limit() -> None:
    sql = _sql(build_claim_stmt(PostMedia.upload_status, 7))
    assert "LIMIT 7" in sql, sql


def test_claim_stmt_filters_pending_only() -> None:
    """PROCESSING 을 다시 집으면 중복 처리가 된다."""
    sql = _sql(build_claim_stmt(PostMedia.upload_status, 5))
    assert f"upload_status = '{ProcessStatus.PENDING.value}'" in sql, sql
    assert ProcessStatus.PROCESSING.value not in sql, sql


def test_ocr_claim_requires_uploaded_image() -> None:
    """OCR 은 S3 에 올라간 이미지만 대상이다.

    upload 가 끝나지 않은 행을 집으면 media_key 가 없어 S3 조회에서 죽고,
    영상을 집으면 키프레임 추출 없이 바이트를 이미지로 읽어 실패한다.
    """
    sql = _sql(
        build_claim_stmt(
            PostMedia.ocr_status,
            5,
            PostMedia.upload_status == ProcessStatus.DONE,
            PostMedia.media_key.is_not(None),
            PostMedia.media_type == MediaType.image,
        )
    )
    assert f"ocr_status = '{ProcessStatus.PENDING.value}'" in sql, sql
    assert f"upload_status = '{ProcessStatus.DONE.value}'" in sql, sql
    assert "media_key IS NOT NULL" in sql, sql
    assert f"media_type = '{MediaType.image.value}'" in sql, sql
