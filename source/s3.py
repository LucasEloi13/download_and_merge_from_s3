from typing import Any
import json
import os
import logging
import sys
from pathlib import Path
import boto3
from botocore.exceptions import BotoCoreError, ClientError

from source.base import DataSource

logger = logging.getLogger(__name__)

from dotenv import load_dotenv
load_dotenv()

class S3Source(DataSource[Any]):

    def __init__(self):
        self._profile_name = os.getenv('AWS_PROFILE')
        self._client = None

    def connect(self) -> Any:
        try:
            session = boto3.Session(profile_name=self._profile_name) if self._profile_name else boto3.Session()
            self._client = session.client('s3')
            logger.debug(f"Connected to S3 with profile: {self._profile_name or 'default'}")
            return self._client
        except (BotoCoreError, ClientError) as e:
            logger.error(f"Failed to connect to S3: {e}")
            raise ConnectionError(f"Failed to connect to S3: {e}") from e

    def execute_query(self, query: str, params: dict = None) -> Any:
        raise NotImplementedError("execute_query is not applicable for S3")

    def list_keys(self, bucket: str, prefix: str, suffix: str | None = None) -> list[str]:
        if not self._client:
            self.connect()

        keys: list[str] = []
        normalized_prefix = prefix.rstrip("/") + "/" if prefix else ""

        paginator = self._client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket, Prefix=normalized_prefix)

        for page in pages:
            for item in page.get("Contents", []):
                key = item.get("Key")
                if not key:
                    continue
                if key.endswith("/"):
                    continue
                if suffix and not key.endswith(suffix):
                    continue
                keys.append(key)

        return keys

    @staticmethod
    def _print_progress(key: str, bytes_read: int, total_bytes: int) -> None:
        bar_width = 30
        ratio = min(bytes_read / total_bytes, 1) if total_bytes else 0
        filled = int(bar_width * ratio)
        bar = "=" * filled + "-" * (bar_width - filled)
        percent = int(ratio * 100)
        mb_read = bytes_read / (1024 * 1024)
        mb_total = total_bytes / (1024 * 1024)
        sys.stdout.write(
            f"\r[{bar}] {percent:3d}% ({mb_read:.2f}/{mb_total:.2f} MB) {key}"
        )
        sys.stdout.flush()

    def fetch_object(self, key: str, bucket: str = None, show_progress: bool = True) -> Any:
        if not self._client:
            self.connect()

        try:
            response = self._client.get_object(Bucket=bucket, Key=key)
            body = response['Body']
            total_bytes = response.get('ContentLength', 0)

            if not show_progress or total_bytes <= 0:
                content = body.read()
            else:
                bytes_read = 0
                last_percent = -1
                chunks: list[bytes] = []

                for chunk in body.iter_chunks(chunk_size=1024 * 256):
                    if not chunk:
                        continue

                    chunks.append(chunk)
                    bytes_read += len(chunk)
                    percent = int((bytes_read / total_bytes) * 100)

                    if percent != last_percent:
                        self._print_progress(key=key, bytes_read=bytes_read, total_bytes=total_bytes)
                        last_percent = percent

                content = b"".join(chunks)
                sys.stdout.write("\n")
                sys.stdout.flush()
            
            try:
                data = json.loads(content)
            except json.JSONDecodeError:
                data = content.decode('utf-8')
            
            logger.debug(f"Object fetched from S3: s3://{bucket}/{key}")
            return data
        except ClientError as e:
            logger.error(f"Failed to fetch object from S3: {e}")
            raise RuntimeError(f"Failed to fetch object from S3: {e}") from e

    def download_object_to_file(
        self,
        key: str,
        destination_path: str | Path,
        bucket: str = None,
        show_progress: bool = True,
    ) -> Path:
        if not self._client:
            self.connect()

        destination = Path(destination_path)
        destination.parent.mkdir(parents=True, exist_ok=True)

        try:
            response = self._client.get_object(Bucket=bucket, Key=key)
            body = response["Body"]
            total_bytes = response.get("ContentLength", 0)

            bytes_read = 0
            last_percent = -1

            with destination.open("wb") as output_file:
                for chunk in body.iter_chunks(chunk_size=1024 * 256):
                    if not chunk:
                        continue

                    output_file.write(chunk)
                    bytes_read += len(chunk)

                    if show_progress and total_bytes > 0:
                        percent = int((bytes_read / total_bytes) * 100)
                        if percent != last_percent:
                            self._print_progress(key=key, bytes_read=bytes_read, total_bytes=total_bytes)
                            last_percent = percent

            if show_progress and total_bytes > 0:
                sys.stdout.write("\n")
                sys.stdout.flush()

            logger.debug(f"Object downloaded from S3 to file: s3://{bucket}/{key} -> {destination}")
            return destination
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code")
            if error_code == "NoSuchKey":
                logger.warning(f"Key não encontrada, pulando: s3://{bucket}/{key}")
                raise FileNotFoundError(f"S3 key não encontrada: s3://{bucket}/{key}") from e
            logger.error(f"Failed to download object from S3: {e}")
            raise RuntimeError(f"Failed to download object from S3: {e}") from e

    def close(self) -> None:
        if self._client:
            self._client.close()
            logger.debug("S3 connection closed")
