"""SQLAlchemy 엔진 / 세션 — 전부 지연 생성.

모듈 임포트 시점에 create_engine 을 호출하면 두 가지가 깨진다.
 1) Airflow 스케줄러가 DAG 파일을 재파싱할 때마다 엔진이 생기고,
    DB_ENDPOINT 가 비어 있으면 파싱 자체가 실패한다.
 2) 커넥션 풀이 fork 이전에 만들어지면 자식 프로세스가 같은 소켓을 물려받아
    'MySQL server has gone away' / 'Packet sequence number wrong' 이 터진다.

그래서 엔진은 get_engine() 첫 호출 시점(= 태스크가 도는 프로세스 안)에 만든다.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from functools import lru_cache

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from app.core.config import get_settings
from app.db.base import Base

__all__ = [
    "Base",
    "get_engine",
    "get_session",
    "get_sessionmaker",
    "session_scope",
    "dispose_engine",
]


@lru_cache(maxsize=1)
def get_engine() -> Engine:
    """프로세스당 1개. 호출만으로는 커넥션을 열지 않는다(첫 쿼리 때 연결)."""
    s = get_settings()
    return create_engine(
        s.database_url,
        pool_pre_ping=True,  # 죽은 커넥션 사용 전에 걸러낸다
        pool_size=s.db_pool_size,
        max_overflow=s.db_max_overflow,
        pool_recycle=s.db_pool_recycle,  # MySQL wait_timeout 보다 짧게
        echo=s.sql_echo,
        echo_pool=s.sql_echo,
        future=True,
    )


@lru_cache(maxsize=1)
def get_sessionmaker() -> sessionmaker[Session]:
    return sessionmaker(
        bind=get_engine(),
        autocommit=False,
        autoflush=False,
        expire_on_commit=False,  # commit 후에도 객체 속성 접근 가능
    )


def get_session() -> Session:
    """세션 하나를 직접 받는다. 닫는 책임은 호출자에게 있다."""
    return get_sessionmaker()()


@contextmanager
def session_scope() -> Iterator[Session]:
    """정상 종료면 commit, 예외면 rollback, 어느 쪽이든 close.

        with session_scope() as db:
            db.add(row)
    """
    session = get_session()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def dispose_engine() -> None:
    """커넥션 풀을 버리고 캐시를 비운다.

    부모 프로세스에서 이미 엔진을 만들어버린 뒤 fork 하는 경우의 탈출구.
    (Airflow worker_process_init, Celery worker fork 훅 등에서 호출)
    """
    if get_engine.cache_info().currsize:
        get_engine().dispose()
    get_engine.cache_clear()
    get_sessionmaker.cache_clear()
