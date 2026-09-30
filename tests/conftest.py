"""실제 MySQL 에 붙어서 도는 테스트의 공통 준비.

이 디렉터리의 테스트는 목(mock)을 쓰지 않는다. 검증 대상이
'SKIP LOCKED 가 실제로 행을 건너뛰는가', '커넥션 타임존이 UTC 인가' 처럼
DB 엔진의 실제 동작이기 때문이다. SQLite 로 대신하면 아무것도 검증되지 않는다.

DB 에 붙을 수 없으면 실패가 아니라 skip 이다. 설정이 없는 환경(CI 초기 등)에서
빨간 줄이 뜨는 것과 '검증하지 못했음'은 구분되어야 한다.
"""

from __future__ import annotations

import pytest
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError

from app.core.config import ConfigError, get_settings
from app.db.session import dispose_engine, get_engine, session_scope


@pytest.fixture(scope="session", autouse=True)
def _require_db() -> None:
    """DB 에 실제로 붙는지 한 번만 확인하고, 안 되면 이 디렉터리 전체를 skip."""
    try:
        get_settings()
        with session_scope() as db:
            db.execute(select(1))
    except ConfigError as e:
        pytest.skip(f"DB 설정 없음 — {e}", allow_module_level=True)
    except SQLAlchemyError as e:
        cause = e.__cause__ or e
        pytest.skip(
            f"DB 연결 실패 — {type(cause).__name__}: {str(cause).splitlines()[0]}",
            allow_module_level=True,
        )


@pytest.fixture(scope="session", autouse=True)
def _dispose_after_all(_require_db: None):
    """테스트가 끝나면 풀을 정리한다. 안 하면 MySQL 에 aborted connection 로그가 남는다."""
    yield
    dispose_engine()


@pytest.fixture(scope="session")
def engine():
    return get_engine()
