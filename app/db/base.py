"""선언적 매핑의 루트.

session.py(엔진)와 분리해 둔다. 모델이 엔진 모듈을 끌어오지 않게 해서
순환 import 를 막고, 모델만 import 하는 쪽(마이그레이션 등)이 가벼워진다.
"""

from __future__ import annotations

from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass
