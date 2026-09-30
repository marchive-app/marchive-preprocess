"""post_media 테이블 매핑.

DB 에 이미 존재하는 테이블이므로 정의는 실제 스키마를 그대로 따라간다.
(모델을 바꿔서 DB 를 바꾸는 게 아니라, DB 를 반영한 것이 모델이다)

claimed_at 은 대기열 회수용으로 나중에 추가된 컬럼이다. 이 모델을 쓰기 전에
DB 쪽에 실제로 있어야 한다(스키마 주인이 먼저 반영한 뒤 여기를 맞춘다).

    ALTER TABLE post_media ADD COLUMN claimed_at DATETIME NULL;

post_id 는 DB 에서 post 를 참조하지만 ForeignKey 를 선언하지 않는다.
post 를 매핑하지 않은 상태에서 FK 를 걸면 metadata 안에서 대상 테이블을
못 찾아 NoReferencedTableError 가 난다. 무결성은 DB 제약이 이미 강제한다.
"""

from __future__ import annotations

import enum
from datetime import datetime

from sqlalchemy import BigInteger, DateTime, Enum, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class MediaType(str, enum.Enum):
    image = "image"
    video = "video"


class ProcessStatus(str, enum.Enum):
    """upload_status / ocr_status 공용.

    멤버 순서는 DB DDL 의 enum 순서와 맞춰 둔다.
    MySQL 은 enum 을 내부 정수로 저장해 ORDER BY 가 이 순서를 따른다.
    """

    DONE = "DONE"
    FAILED = "FAILED"
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"


# 파이썬 enum 을 그대로 쓰면 SQLAlchemy 가 멤버 '이름'을 저장한다.
# 여기서는 이름과 값이 같지만, 의도를 명시하려고 값 기준으로 고정한다.
_STATUS = Enum(
    ProcessStatus,
    name="process_status",
    values_callable=lambda x: [m.value for m in x],
)


class PostMedia(Base):
    __tablename__ = "post_media"

    post_media_id: Mapped[int] = mapped_column(
        BigInteger, primary_key=True, autoincrement=True
    )
    post_id: Mapped[int] = mapped_column(BigInteger, index=True)
    order_index: Mapped[int] = mapped_column(Integer)

    media_type: Mapped[MediaType] = mapped_column(
        Enum(
            MediaType, name="media_type", values_callable=lambda x: [m.value for m in x]
        )
    )

    # 원본 CDN 주소 → S3 키. varchar(2000) 이라 인덱스는 걸 수 없다.
    ig_cdn_url: Mapped[str | None] = mapped_column(String(2000))
    media_key: Mapped[str | None] = mapped_column(String(2000))
    ocr_text: Mapped[str | None] = mapped_column(Text)

    # DB 에 DEFAULT 가 없고 NOT NULL 이므로 INSERT 시 반드시 값이 필요하다.
    # 파이썬 쪽 default 로 채워 준다(서버 기본값이 아니라 애플리케이션 기본값).
    #
    # index=True 는 DB 에 이미 있는 인덱스를 모델에 적어 둔 것이다(우리가 만드는 게 아니다).
    # 이 테이블은 상태판이자 대기열이라, 워커의 선점 쿼리가
    #   WHERE upload_status = 'PENDING' ... FOR UPDATE SKIP LOCKED
    # 로 상태 컬럼만 훑는다. 인덱스가 없으면 풀 스캔이 되고, FOR UPDATE 가 스쳐 간
    # 행마다 잠금을 시도해 워커를 늘릴수록 서로 밟는다.
    upload_status: Mapped[ProcessStatus] = mapped_column(
        _STATUS, default=ProcessStatus.PENDING, index=True
    )
    ocr_status: Mapped[ProcessStatus] = mapped_column(
        _STATUS, default=ProcessStatus.PENDING, index=True
    )

    # 워커가 이 행을 선점한 시각. NULL 이면 아직 아무도 집지 않았다는 뜻이다.
    #
    # 상태를 PROCESSING 으로 바꾼 직후 워커가 죽으면 그 행은 영원히 PROCESSING 에
    # 갇힌다. PENDING 이 아니니 아무도 다시 집지 않아 조용히 사라진다.
    # 이 컬럼이 있어야 "N 분 넘게 PROCESSING 이면 PENDING 으로 되돌린다"는 회수가 된다.
    #
    # DATETIME 이라 타임존이 붙지 않는다. 값은 UTC 로만 넣고, 커넥션에서
    # time_zone = '+00:00' 을 고정해 NOW() 비교가 서버 로캘에 흔들리지 않게 한다.
    claimed_at: Mapped[datetime | None] = mapped_column(DateTime)

    def __repr__(self) -> str:
        return (
            f"PostMedia(id={self.post_media_id}, post_id={self.post_id}, "
            f"type={self.media_type}, upload={self.upload_status}, ocr={self.ocr_status})"
        )
