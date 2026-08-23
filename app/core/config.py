"""애플리케이션 설정 한 곳 모음.

.env(또는 실제 환경변수) → Settings 객체로 한 번에 옮겨 담는다.
pydantic 의존성 없이, 표준 dataclass + 작은 캐스팅 헬퍼만 쓴다.

원칙 3가지
 1) os.getenv 는 이 파일 밖에서 절대 호출하지 않는다. 설정 출처는 여기 하나.
 2) 문자열 → int/float/bool 변환과 필수값 검증은 읽을 때 한 번만 한다.
 3) Settings() 를 모듈 임포트 시점에 만들지 않는다. get_settings() 로 지연 생성한다.
    (Airflow 스케줄러가 DAG 파일을 재파싱할 때마다 필수값 누락으로 죽는 걸 막는다)
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from dotenv import load_dotenv

# 프로젝트 루트(= app/ 의 부모). cwd 에 의존하지 않도록 파일 기준으로 고정한다.
# Airflow 워커는 cwd 가 어디일지 보장되지 않는다.
PROJECT_ROOT = Path(__file__).resolve().parents[2]

DEFAULT_ENV = "dev"


class ConfigError(RuntimeError):
    """필수 설정이 없거나 형식이 잘못됨."""


# --- .env → 파이썬 값으로 바꾸는 헬퍼 --------------------------------------
# pydantic 이 대신 해주던 일이 사실상 이 아래 5개가 전부다.

_MISSING = object()


def _str(key: str, default=_MISSING) -> str:
    raw = os.getenv(key)
    if raw is None or raw == "":
        if default is _MISSING:
            raise ConfigError(f"필수 환경변수 누락: {key}")
        return default
    return raw


def _int(key: str, default: int) -> int:
    raw = os.getenv(key)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError as e:
        raise ConfigError(f"{key} 는 정수여야 함 (받은 값: {raw!r})") from e


def _float(key: str, default: float) -> float:
    raw = os.getenv(key)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except ValueError as e:
        raise ConfigError(f"{key} 는 실수여야 함 (받은 값: {raw!r})") from e


def _bool(key: str, default: bool) -> bool:
    raw = os.getenv(key)
    if raw is None or raw == "":
        return default
    if raw.lower() in ("1", "true", "yes", "on"):
        return True
    if raw.lower() in ("0", "false", "no", "off"):
        return False
    raise ConfigError(f"{key} 는 bool 이어야 함 (받은 값: {raw!r})")


def _tuple(key: str, default: tuple[str, ...]) -> tuple[str, ...]:
    """콤마 구분 문자열 → 튜플. 예) OCR_LANGS=ko,en"""
    raw = os.getenv(key)
    if raw is None or raw == "":
        return default
    return tuple(item.strip() for item in raw.split(",") if item.strip())


# --- 설정 본체 ---------------------------------------------------------------


@dataclass(frozen=True)
class Settings:
    # 실행 환경: dev | prod (파일에 DEV/PROD 로 써도 소문자로 정규화)
    env: str
    # 실제로 읽은 env 파일 — 디버깅용
    env_file: Path

    # AWS S3
    region: str
    bucket: str
    aws_access_key_id: str
    aws_secret_access_key: str
    presign_expires: int

    # Embedding / OCR
    gemini_api_key: str
    voyage_api_key: str
    embed_model: str
    embed_dim: int
    ocr_langs: tuple[str, ...]
    ocr_min_confidence: float

    # Database
    db_endpoint: str
    db_user: str
    db_password: str
    db_name: str
    sql_echo: bool
    db_pool_size: int
    db_max_overflow: int
    db_pool_recycle: int

    # 미디어 처리 한계
    image_max_bytes: int
    video_max_bytes: int
    download_timeout: float

    staging_dir: Path

    # -- .env 를 읽어 Settings 를 만드는 유일한 자리 --
    @classmethod
    def from_env(cls, env_file: Path | None = None) -> Settings:
        return cls(
            env=_str("ENV", DEFAULT_ENV).strip().lower(),
            env_file=env_file or Path("<environ>"),
            region=_str("REGION", "ap-northeast-2"),
            bucket=_str("BUCKET"),
            aws_access_key_id=_str("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=_str("AWS_SECRET_ACCESS_KEY"),
            presign_expires=_int("PRESIGN_EXPIRES", 3600),
            gemini_api_key=_str("GEMINI_API_KEY"),
            voyage_api_key=_str("VOYAGE_API_KEY", ""),
            embed_model=_str("EMBED_MODEL", "gemini-embedding-2"),
            embed_dim=_int("EMBED_DIM", 1536),
            ocr_langs=_tuple("OCR_LANGS", ("ko", "en")),
            ocr_min_confidence=_float("OCR_MIN_CONFIDENCE", 0.4),
            db_endpoint=_str("DB_ENDPOINT", ""),
            db_user=_str("DB_USER", "root"),
            db_password=_str("DB_PASSWORD", ""),
            db_name=_str("DB_NAME", "marchive"),
            sql_echo=_bool("SQL_ECHO", False),
            db_pool_size=_int("DB_POOL_SIZE", 5),
            db_max_overflow=_int("DB_MAX_OVERFLOW", 10),
            # MySQL 기본 wait_timeout(8h)보다 짧아야 끊긴 커넥션을 안 잡는다
            db_pool_recycle=_int("DB_POOL_RECYCLE", 3600),
            image_max_bytes=_int("IMAGE_MAX_BYTES", 5 * 1024 * 1024),
            video_max_bytes=_int("VIDEO_MAX_BYTES", 20 * 1024 * 1024),
            download_timeout=_float("DOWNLOAD_TIMEOUT", 30.0),
            staging_dir=Path(_str("STAGING_DIR", "./_staging")),
        )

    # -- 파생값은 property 로. .env 에 중복해서 넣지 않는다 --
    @property
    def database_url(self) -> str:
        if not self.db_endpoint:
            raise ConfigError("DB_ENDPOINT 가 비어 있어 DB 에 연결할 수 없음")
        return (
            f"mysql+pymysql://{self.db_user}:{self.db_password}"
            f"@{self.db_endpoint}/{self.db_name}"
        )

    def max_bytes_for(self, media_type: str) -> int:
        try:
            return {
                "image": self.image_max_bytes,
                "video": self.video_max_bytes,
            }[media_type]
        except KeyError:
            raise ConfigError(f"지원하지 않는 media_type: {media_type}") from None

    def __repr__(self) -> str:  # 로그에 시크릿이 찍히지 않게
        return (
            f"Settings(env={self.env!r}, env_file={str(self.env_file)!r}, "
            f"region={self.region!r}, bucket={self.bucket!r})"
        )


def resolve_env_file() -> Path:
    """어떤 env 파일을 읽을지 정한다.

    우선순위
      1) ENV_FILE 로 직접 지정한 경로
      2) .env.{ENV}   — ENV 는 '진짜' 환경변수여야 한다.
         (파일 안의 ENV= 로는 파일을 고를 수 없다. 닭이 먼저인 문제)
      3) .env         — 위가 없을 때의 폴백
    """
    explicit = os.getenv("ENV_FILE")
    if explicit:
        return Path(explicit)

    env = (os.getenv("ENV") or DEFAULT_ENV).strip().lower()
    candidate = PROJECT_ROOT / f".env.{env}"
    if candidate.exists():
        return candidate
    return PROJECT_ROOT / ".env"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """프로세스당 1회만 env 파일을 읽어 Settings 를 만든다.

    이미 설정된 실제 환경변수를 덮어쓰지 않는다(override=False 기본).
    → 로컬은 .env.dev, 컨테이너/Airflow 는 주입된 환경변수가 그대로 이긴다.
    파일이 없어도 예외를 내지 않는다(전부 환경변수로 주입하는 배포 형태 지원).
    """
    env_file = resolve_env_file()
    load_dotenv(env_file)
    return Settings.from_env(env_file)
