# syntax=docker/dockerfile:1

# marchive-preprocess — 미디어 다운로드 / OCR / 임베딩 파이프라인.
# 서버가 아니라 Airflow 태스크(또는 CLI)로 도는 배치 패키지라
# 기본 CMD 는 main.py 이고, 실제 실행은 `docker run ... python -m ...` 로 덮어쓴다.

# ---- builder: 의존성만 설치해서 /app/.venv 를 만든다 ------------------------
FROM python:3.11-slim-bookworm AS builder

COPY --from=ghcr.io/astral-sh/uv:0.9 /uv /uvx /bin/

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never

WORKDIR /app

# 1) 락 파일만 먼저 복사 → 소스만 바뀌면 이 레이어는 캐시에서 재사용된다.
#    (torch/easyocr 가 무거워 캐시 적중 여부가 빌드 시간을 좌우한다)
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-install-project --no-dev

# 2) 프로젝트 자체 설치 (소스가 바뀌면 여기부터만 다시 돈다)
#    README.md 는 pyproject 의 readme 필드가 가리키므로 빌드에 필요하다.
COPY app ./app
COPY main.py README.md ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev


# ---- runtime ----------------------------------------------------------------
FROM python:3.11-slim-bookworm AS runtime

# easyocr 가 끌고 오는 opencv 의 런타임 공유 라이브러리.
# libgl1 / libglib2.0-0 이 없으면 import 시점에 ImportError 로 죽는다.
RUN apt-get update && apt-get install -y --no-install-recommends \
        libgl1 \
        libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# 모델 가중치 캐시. /cache 를 볼륨으로 붙여야 컨테이너마다 재다운로드하지 않는다.
ENV EASYOCR_MODULE_PATH=/cache/easyocr \
    HF_HOME=/cache/huggingface \
    TORCH_HOME=/cache/torch

# config.py 가 읽는 값. STAGING_DIR 은 쓰기 가능한 절대경로로 고정한다.
ENV STAGING_DIR=/app/_staging \
    ENV=prod

WORKDIR /app

COPY --from=builder /app/.venv /app/.venv
COPY app ./app
COPY main.py ./

# 루트로 돌리지 않는다. 캐시/스테이징 디렉터리는 미리 만들어 소유권을 넘긴다.
RUN useradd --create-home --uid 10001 app \
    && mkdir -p /cache/easyocr /cache/huggingface /cache/torch /app/_staging \
    && chown -R app:app /cache /app/_staging
USER app

VOLUME ["/cache"]

CMD ["python", "main.py"]
