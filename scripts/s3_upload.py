"""Upload a run's artifact tree to S3-compatible object storage (MinIO locally,
Nebius Object Storage in the cloud). Runs in the PROJECT venv via `uv run`.

Usage: python scripts/s3_upload.py <run_dir> <bucket> <endpoint_url> <region>
Credentials come from AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY in the env.
"""
import sys
from pathlib import Path

import boto3
from botocore.config import Config


def main(run_dir: str, bucket: str, endpoint_url: str, region: str) -> None:
    run_dir = Path(run_dir)
    run_id = run_dir.name

    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        region_name=region,
        config=Config(signature_version="s3v4"),
    )

    existing = {b["Name"] for b in s3.list_buckets().get("Buckets", [])}
    if bucket not in existing:
        s3.create_bucket(Bucket=bucket)

    uploaded = 0
    for path in sorted(run_dir.rglob("*")):
        if path.is_file():
            key = f"{run_id}/{path.relative_to(run_dir)}"
            s3.upload_file(str(path), bucket, key)
            uploaded += 1
            print(f"  uploaded {key}")

    print(f"uploaded {uploaded} files to s3://{bucket}/{run_id}/")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
