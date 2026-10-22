"""AWS-worker-only HTTPS-to-S3 copy with bounded memory and SHA-256 readback.

SOURCE_FILES_JSON is a secret JSON list of {name, url, size}; never log URLs.
A four-part sliding window overlaps cross-region reads without a local file.
"""
from collections import deque
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
import signal
import threading
from urllib.parse import urlsplit

import boto3
import requests

PART_SIZE = 32 * 1024 * 1024
WORKERS = int(os.getenv('TRANSFER_WORKERS', '4'))
LOCAL = threading.local()


def copy_one(s3, bucket, item):
    name = item['name']
    if '/' in name or name not in {'gen_wot_2.5m.parquet', 'wot_retrieval_queries.parquet'}:
        raise ValueError('Unexpected dataset filename')
    if urlsplit(item['url']).scheme != 'https':
        raise ValueError('HTTPS required')
    size = int(item['size'])
    if size <= 0 or WORKERS < 1 or WORKERS > 8:
        raise ValueError('Invalid source size or worker count')
    key = 'datasets/' + name
    upload = s3.create_multipart_upload(Bucket=bucket, Key=key, ServerSideEncryption='AES256')
    upload_id = upload['UploadId']
    parts = []
    digest = hashlib.sha256()
    received = 0

    def copy_part(number, start):
        if not hasattr(LOCAL, 'session'):
            LOCAL.session = requests.Session()
        end = min(start + PART_SIZE, size) - 1
        for attempt in range(5):
            response = None
            try:
                response = LOCAL.session.get(item['url'], headers={'Range': f'bytes={start}-{end}'},
                                             timeout=(30, 300), stream=True)
                if response.status_code != 206 and not (start == 0 and end + 1 == size and response.status_code == 200):
                    raise RuntimeError(f'Source HTTP status {response.status_code}')
                # Never buffer an unbounded 200 response if a server ignores Range.
                data = response.raw.read(end - start + 2)
                if len(data) != end - start + 1:
                    raise RuntimeError('Source length mismatch')
                break
            except Exception:
                if attempt == 4:
                    # Requests errors can include signed URLs; redact the chain.
                    raise RuntimeError(f'Source read failed for {name}, part {number}') from None
            finally:
                if response is not None:
                    response.close()
        result = s3.upload_part(Bucket=bucket, Key=key, UploadId=upload_id, PartNumber=number, Body=data)
        return number, data, result['ETag']

    try:
        jobs = iter(enumerate(range(0, size, PART_SIZE), 1))
        with ThreadPoolExecutor(max_workers=WORKERS) as executor:
            pending = deque()
            for number, start in jobs:
                pending.append(executor.submit(copy_part, number, start))
                if len(pending) == WORKERS:
                    break
            while pending:
                # Consume in byte order for SHA-256; at most WORKERS parts in memory.
                number, data, etag = pending.popleft().result()
                digest.update(data)
                received += len(data)
                parts.append({'PartNumber': number, 'ETag': etag})
                del data
                if number % 16 == 0 or received == size:
                    print(f'{name}: {received}/{size} bytes uploaded', flush=True)
                try:
                    number, start = next(jobs)
                    pending.append(executor.submit(copy_part, number, start))
                except StopIteration:
                    pass
        s3.complete_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id, MultipartUpload={'Parts': parts})
    except BaseException:
        s3.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
        raise
    if s3.head_object(Bucket=bucket, Key=key)['ContentLength'] != size:
        raise RuntimeError('S3 object length mismatch')
    check = hashlib.sha256()
    body = s3.get_object(Bucket=bucket, Key=key)['Body']
    try:
        for chunk in iter(lambda: body.read(8 * 1024 * 1024), b''):
            check.update(chunk)
    finally:
        body.close()
    if check.digest() != digest.digest():
        raise RuntimeError('S3 SHA-256 mismatch')
    print(f'VERIFIED {name} bytes={size} sha256={digest.hexdigest()}', flush=True)
    return f'{digest.hexdigest()}  {name}\n'


def main():
    def terminate(signum, frame):
        raise SystemExit('Transfer interrupted')
    signal.signal(signal.SIGTERM, terminate)
    s3 = boto3.client('s3', region_name=os.getenv('AWS_DEFAULT_REGION', 'ap-south-1'))
    bucket = os.environ['DATASET_BUCKET']
    files = json.loads(os.environ['SOURCE_FILES_JSON'])
    checksums = ''.join(copy_one(s3, bucket, item) for item in files)
    s3.put_object(Bucket=bucket, Key='datasets/SHA256SUMS', Body=checksums.encode(), ServerSideEncryption='AES256')
    print('All dataset objects verified', flush=True)


if __name__ == '__main__':
    main()
