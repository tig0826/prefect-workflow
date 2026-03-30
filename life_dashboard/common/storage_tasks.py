import json
import logging
import boto3
from botocore.exceptions import ClientError
from prefect import task
from prefect.variables import Variable
from prefect.blocks.system import Secret


def connect_s3_client():
    endpoint_url = Variable.get("minio-endpoint", default="http://minio.mynet")
    bucket = Variable.get("datalake-bronze-bucket", default="bronze-zone")
    access_key = Secret.load("minio-life-dashboard-access-key").get()
    secret_key = Secret.load("minio-life-dashboard-secret-key").get()
    client = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )
    return client, bucket


@task(retries=3, retry_delay_seconds=30, name="Save JSON to Datalake (MinIO)")
def save_json_to_s3(data: dict, prefix: str, file_name: str):
    """
    取得した辞書データをMinIO (S3) のBronze層に保存する共通タスク。
    Args:
        data: 保存する辞書データ
        prefix: S3のキーのプレフィックス (例: "asken/raw" や "fitbit/raw")
        file_name: 保存するファイル名 (例: "asken_2026-02-18.json")
    """
    try:
        s3_client, bucket = connect_s3_client()
        clean_prefix = prefix.strip("/")
        object_key = f"{clean_prefix}/{file_name}"
        if isinstance(data, str):
            # 文字列が渡された場合（FitbitのJSONLなど）は、余計な加工をせずそのまま使う
            body_data = data
        else:
            # 辞書が渡された場合（既存のパイプライン）は、従来の Pretty Print で処理
            body_data = json.dumps(data, ensure_ascii=False, indent=2)
        s3_client.put_object(
            Bucket=bucket, Key=object_key, Body=body_data.encode("utf-8")
        )
        logging.info(f"✅ データを S3 に保存しました: s3://{bucket}/{object_key}")
    except ClientError as e:
        logging.error(f"❌ MinIOへのアップロードに失敗しました: {e}")
        raise


@task(retries=3, retry_delay_seconds=30, name="Save Bytes to Datalake (MinIO)")
def save_bytes_to_s3(
    data: bytes,
    prefix: str,
    file_name: str,
    content_type: str = "application/octet-stream",
    content_encoding: str = "",
):
    """
    バイナリデータ (Gzipなど) をMinIO (S3) の指定パスに保存する共通タスク。
    """
    try:
        s3_client, bucket = connect_s3_client()
        clean_prefix = prefix.strip("/")
        object_key = f"{clean_prefix}/{file_name}"
        kwargs = {
            "Bucket": bucket,
            "Key": object_key,
            "Body": data,
            "ContentType": content_type,
        }
        if content_encoding:
            kwargs["ContentEncoding"] = content_encoding
        s3_client.put_object(**kwargs)
        logging.info(
            f"✅ バイナリデータを S3 に保存しました: s3://{bucket}/{object_key}"
        )
    except ClientError as e:
        logging.error(f"❌ MinIOへのバイナリアップロードに失敗しました: {e}")
        raise
