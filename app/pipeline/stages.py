import hashlib
import io
import tempfile

import httpx
import numpy as np
from PIL import Image
from google.genai import types

from app.core.config import get_settings
from app.pipeline.resources import get_genai_client, get_reader, get_s3


def _content_key(data: bytes, ext: str = "jpg") -> str:
    h = hashlib.sha256(data).hexdigest()
    return f"media/ig/{h[:2]}/{h[2:4]}/{h}.{ext}"


def create_presigned_url(key: str) -> str:
    settings = get_settings()
    return get_s3().generate_presigned_url(
        "get_object",
        Params={"Bucket": settings.bucket, "Key": key},
        ExpiresIn=settings.presign_expires,
    )


def stage_download(media_type: str, url: str) -> str:
    """미디어 다운로드 후 S3 업로드. 콘텐츠 해시 키를 돌려준다."""
    settings = get_settings()
    max_bytes = settings.max_bytes_for(media_type)  # 지원하지 않는 타입이면 여기서 실패
    ext = "mp4" if media_type == "video" else "jpg"

    key = _content_key(url.encode(), ext)
    s3 = get_s3()

    with tempfile.SpooledTemporaryFile(max_size=8 * 1024 * 1024) as tmp:
        with httpx.stream(
            "GET", url, timeout=settings.download_timeout, follow_redirects=True
        ) as r:
            r.raise_for_status()
            ctype = r.headers.get("content-type", "application/octet-stream")
            total = 0
            for chunk in r.iter_bytes(65536):
                total += len(chunk)
                if total > max_bytes:
                    raise ValueError(f"too large: >{max_bytes} bytes")
                tmp.write(chunk)
        tmp.seek(0)
        s3.upload_fileobj(tmp, settings.bucket, key, ExtraArgs={"ContentType": ctype})
    print(f"{key} uploaded")
    print(create_presigned_url(key))
    return key


def stage_ocr(key: str) -> list[str]:
    settings = get_settings()
    with get_s3().get_object(Bucket=settings.bucket, Key=key)["Body"] as body:
        data = body.read()
    result = get_reader().readtext(data)
    lines = [
        text for (_bbox, text, conf) in result if conf >= settings.ocr_min_confidence
    ]
    print(result)
    return lines


def stage_embed(key: str, query: str):
    settings = get_settings()
    with get_s3().get_object(Bucket=settings.bucket, Key=key)["Body"] as body:
        img_bytes = body.read()
    Image.open(io.BytesIO(img_bytes)).convert("RGB")  # 디코딩 가능 여부 검증
    print(create_presigned_url(key))

    # 이미지 1장당 1회 호출 → (N, DIM)
    img_bytes_list = [img_bytes]
    image_embeddings = np.stack([embed_image(b) for b in img_bytes_list])
    query_embedding = embed_query(query)  # (DIM,)

    sims = image_embeddings @ query_embedding  # (N,)
    ranked = np.argsort(-sims)

    print(f"Query: '{query}'\n")
    print("Search with image-only:")
    for rank, idx in enumerate(ranked, 1):
        print(f"{rank}. ({sims[idx]:.4f})")
    return sims


def _vec(resp) -> np.ndarray:
    v = np.array(resp.embeddings[0].values, dtype=np.float32)
    return v / np.linalg.norm(v)  # MRL 축소 시 필수


def embed_image(img_bytes: bytes, mime: str = "image/jpeg") -> np.ndarray:
    settings = get_settings()
    return _vec(
        get_genai_client().models.embed_content(
            model=settings.embed_model,
            contents=[types.Part.from_bytes(data=img_bytes, mime_type=mime)],
            config=types.EmbedContentConfig(
                task_type="RETRIEVAL_DOCUMENT",
                output_dimensionality=settings.embed_dim,
            ),
        )
    )


def embed_query(text: str) -> np.ndarray:
    settings = get_settings()
    return _vec(
        get_genai_client().models.embed_content(
            model=settings.embed_model,
            contents=[text],
            config=types.EmbedContentConfig(
                task_type="RETRIEVAL_QUERY",
                output_dimensionality=settings.embed_dim,
            ),
        )
    )
