"""프로세스 단위로 1회만 생성되는 무거운 리소스들.

Airflow에서는 DAG 파일이 스케줄러에 의해 수십 초마다 재파싱되고,
태스크는 매번 새 프로세스(fork 또는 새 Pod)에서 실행된다.
따라서 모듈 import 시점에 클라이언트/모델을 만들면
 - 스케줄러가 DAG를 파싱할 때마다 easyocr 모델을 로드하고
 - fork 이전에 만들어진 boto3 client는 fork-safe 하지 않아 간헐적으로 깨진다.

lru_cache 로 "지연 싱글턴"을 만들어
실제로 처음 쓰이는 순간(= 태스크가 도는 프로세스 안)에 한 번만 생성한다.
설정값은 전부 core.config.get_settings() 에서 온다.
"""

from functools import lru_cache

import boto3
from botocore.config import Config

from app.core.config import get_settings


@lru_cache(maxsize=1)
def get_s3():
    s = get_settings()
    return boto3.client(
        "s3",
        region_name=s.region,
        aws_access_key_id=s.aws_access_key_id,
        aws_secret_access_key=s.aws_secret_access_key,
        config=Config(
            signature_version="s3v4",
            retries={"max_attempts": 3, "mode": "standard"},
            max_pool_connections=50,  # 캐러셀 병렬 업로드용
        ),
    )


@lru_cache(maxsize=1)
def get_reader():
    import easyocr  # import 자체가 무거우므로 함수 안에서

    return easyocr.Reader(list(get_settings().ocr_langs))


@lru_cache(maxsize=1)
def get_genai_client():
    from google import genai

    return genai.Client(api_key=get_settings().gemini_api_key)


def warmup() -> None:
    """워커 프로세스 기동 직후 미리 로드하고 싶을 때만 호출 (선택)."""
    get_s3()
    get_reader()
    get_genai_client()
