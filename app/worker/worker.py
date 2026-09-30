"""폴링 워커 — 대기열을 지켜보다 집어서 처리한다.

    python -m app.worker.worker --role upload --batch 5
    python -m app.worker.worker --role both --once      # 한 바퀴만 (수동 확인용)

Airflow 배치와 달리 이 프로세스는 계속 떠 있는다. 그래서 얻는 게 둘이다.
  - 지연이 폴링 주기(기본 1초)로 묶인다. cron 5분 주기면 최악 5분이다.
  - 무거운 초기화를 프로세스 수명 전체에 상각한다.

루프의 규칙 두 가지.

 1) **일감이 있으면 쉬지 않는다.** 비었을 때만 잔다.
    그래서 큐가 밀려 있을 때의 처리량은 폴링 주기와 무관하고,
    최대 1초 지연은 "큐가 비어 있다가 새 건이 들어온" 경우에만 생긴다.
    (유휴 시 지수 백오프로 주기를 늘리면 그 첫 건이 그만큼 늦어진다. 안 한다)

 2) **sleep 이 아니라 Event.wait 로 잔다.**
    time.sleep 중에는 종료 플래그를 봐도 깨어나지 못한다. 컨테이너가 SIGTERM
    이후 유예 시간 안에 안 죽으면 SIGKILL 을 맞고, 처리 중이던 행은
    PROCESSING 인 채 남는다.

한 건의 실패가 배치를 죽이지 않는다. 행 단위로 FAILED 를 기록하고 계속 흐른다.
"""

from __future__ import annotations

import argparse
import logging
import signal
import threading
from collections.abc import Callable

from app.db.session import dispose_engine
from app.models import PostMedia, ProcessStatus
from app.worker import claim

logger = logging.getLogger("worker")

# 유휴 시 폴링 주기(초). 큐가 비었을 때만 쓰인다.
IDLE_SECONDS = 1.0
# 좀비 회수를 시도하는 간격(초)과 기준(분).
REAP_EVERY_SECONDS = 300.0
REAP_OLDER_THAN_MINUTES = 30


def _install_signal_handlers() -> threading.Event:
    """SIGTERM/SIGINT 를 받으면 세워지는 플래그를 돌려준다.

    핸들러 안에서는 플래그만 세운다. 여기서 DB 를 만지거나 로깅을 하면
    시그널이 아무 때나 끼어드는 특성 때문에 재진입 문제가 생긴다.
    """
    stop = threading.Event()

    def _handler(_signum: int, _frame: object) -> None:
        stop.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, _handler)
    return stop


# --- 스테이지 실행 -----------------------------------------------------------


def _process_upload(row: PostMedia) -> None:
    # 지연 임포트 — role=ocr 로만 띄운 프로세스가 다운로드 쪽 의존성을
    # (그리고 그 반대도) 메모리에 올리지 않게 한다. resources.py 와 같은 이유다.
    from app.pipeline.stages import stage_download

    key = stage_download(row.media_type.value, row.ig_cdn_url)
    claim.finish_upload(row.post_media_id, key)


def _process_ocr(row: PostMedia) -> None:
    from app.pipeline.stages import stage_ocr

    lines = stage_ocr(row.media_key)
    claim.finish_ocr(row.post_media_id, lines)


def _run_stage(
    name: str,
    claim_fn: Callable[[int], list[int]],
    process_fn: Callable[[PostMedia], None],
    fail_fn: Callable[[int, BaseException], None],
    batch: int,
) -> int:
    """한 스테이지를 batch 만큼 처리하고 처리한 건수를 돌려준다.

    선점 트랜잭션은 이미 커밋됐고, 처리는 그 밖에서 한다.
    """
    ids = claim_fn(batch)
    if not ids:
        return 0

    logger.info("%s 선점 %s건: %s", name, len(ids), ids)
    for row in claim.load(ids):
        try:
            process_fn(row)
        except Exception as e:  # 한 건의 실패로 루프를 멈추지 않는다
            fail_fn(row.post_media_id, e)
    return len(ids)


def run_once(role: str, batch: int) -> int:
    """한 바퀴. 처리한 총 건수를 돌려준다(0 이면 큐가 비었다는 뜻)."""
    done = 0
    if role in ("upload", "both"):
        done += _run_stage(
            "upload", claim.claim_upload, _process_upload, claim.fail_upload, batch
        )
    if role in ("ocr", "both"):
        done += _run_stage("ocr", claim.claim_ocr, _process_ocr, claim.fail_ocr, batch)
    return done


# --- 루프 --------------------------------------------------------------------


def _reap(role: str) -> None:
    """좀비 회수. claimed_at 이 없으면 조용히 건너뛴다(기동 시 한 번만 경고)."""
    if not claim.HAS_CLAIMED_AT:
        return
    if role in ("upload", "both"):
        claim.reap_zombies(PostMedia.upload_status, REAP_OLDER_THAN_MINUTES)
    if role in ("ocr", "both"):
        claim.reap_zombies(PostMedia.ocr_status, REAP_OLDER_THAN_MINUTES)


def run(role: str, batch: int, idle: float = IDLE_SECONDS) -> None:
    stop = _install_signal_handlers()

    if not claim.HAS_CLAIMED_AT:
        logger.warning(
            "claimed_at 컬럼이 없어 좀비 회수를 하지 않는다. "
            "이 워커가 처리 중에 죽으면 그 행은 %s 인 채로 갇힌다.",
            ProcessStatus.PROCESSING.value,
        )

    logger.info("워커 시작 role=%s batch=%s idle=%ss", role, batch, idle)
    reap_timer = 0.0
    try:
        while not stop.is_set():
            try:
                processed = run_once(role, batch)
            except Exception:
                # DB 가 잠깐 끊긴 경우 등. 루프 자체는 살려 둔다.
                logger.exception("루프에서 예외 — %s초 후 계속", idle)
                processed = 0

            if processed:
                reap_timer = 0.0
                continue  # 일감이 있으면 쉬지 않고 다음 바퀴

            stop.wait(idle)  # 비었을 때만 잔다. SIGTERM 이면 즉시 깬다.

            reap_timer += idle
            if reap_timer >= REAP_EVERY_SECONDS:
                reap_timer = 0.0
                try:
                    _reap(role)
                except Exception:
                    logger.exception("좀비 회수 실패")
    finally:
        # 처리 중이던 건은 위 루프에서 이미 끝났다(조건 검사 지점에서만 빠져나온다).
        logger.info("워커 종료")
        dispose_engine()


def main() -> None:
    parser = argparse.ArgumentParser(prog="worker", description="post_media 폴링 워커")
    parser.add_argument(
        "--role",
        choices=("upload", "ocr", "both"),
        default="both",
        help="처리할 스테이지. 자원 성격이 달라 나눠 띄우는 것을 권한다 "
        "(upload=I/O 바운드, ocr=CPU 바운드)",
    )
    parser.add_argument("--batch", type=int, default=5, help="한 번에 선점할 행 수")
    parser.add_argument("--idle", type=float, default=IDLE_SECONDS, help="유휴 폴링 주기(초)")
    parser.add_argument("--once", action="store_true", help="한 바퀴만 돌고 끝낸다")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    if args.once:
        try:
            print(f"처리 {run_once(args.role, args.batch)}건")
        finally:
            dispose_engine()
        return

    run(args.role, args.batch, args.idle)


if __name__ == "__main__":
    main()
