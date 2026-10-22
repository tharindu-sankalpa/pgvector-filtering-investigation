"""Verify bounded transfer, integrity checking and failed-upload cleanup offline."""
import hashlib
import importlib.util
import io
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch

# The transfer worker installs SDKs at runtime. Stubs keep these checks offline.
sys.modules.setdefault('boto3', types.ModuleType('boto3'))
sys.modules.setdefault('requests', types.ModuleType('requests'))
spec = importlib.util.spec_from_file_location('stream_to_s3', Path(__file__).parents[1] / 'scripts/aws/stream_to_s3.py')
transfer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(transfer)


class S3:
    def __init__(self, corrupt=False):
        self.parts = {}
        self.aborted = False
        self.corrupt = corrupt

    def create_multipart_upload(self, **kwargs):
        return {'UploadId': 'upload'}

    def upload_part(self, **kwargs):
        self.parts[kwargs['PartNumber']] = kwargs['Body']
        return {'ETag': str(kwargs['PartNumber'])}

    def complete_multipart_upload(self, **kwargs):
        self.data = b''.join(self.parts[k] for k in sorted(self.parts))

    def abort_multipart_upload(self, **kwargs):
        self.aborted = True

    def head_object(self, **kwargs):
        return {'ContentLength': len(self.data)}

    def get_object(self, **kwargs):
        return {'Body': io.BytesIO(b'bad' if self.corrupt else self.data)}


class Session:
    data = b'abcdefghijk'
    short = False

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def get(self, url, headers, timeout, stream):
        assert stream is True
        start, end = map(int, headers['Range'].split('=')[1].split('-'))
        data = self.data[start:end + 1]
        response = types.SimpleNamespace(status_code=206, raw=io.BytesIO(data[:-1] if self.short else data), close=lambda: None)
        return response


class TransferTests(unittest.TestCase):
    item = {'name': 'gen_wot_2.5m.parquet', 'url': 'https://example.invalid/file?private-token', 'size': 11}

    def run_copy(self, s3):
        transfer.LOCAL = __import__('threading').local()
        with patch.object(transfer, 'PART_SIZE', 4), patch.object(transfer.requests, 'Session', Session, create=True):
            return transfer.copy_one(s3, 'bucket', self.item)

    def test_multipart_and_independent_readback(self):
        s3 = S3()
        line = self.run_copy(s3)
        self.assertEqual(s3.data, Session.data)
        self.assertEqual(len(s3.parts), 3)
        self.assertEqual(line, hashlib.sha256(Session.data).hexdigest() + '  gen_wot_2.5m.parquet\n')

    def test_short_source_aborts_and_redacts_url(self):
        s3 = S3()
        with patch.object(Session, 'short', True):
            with self.assertRaises(RuntimeError) as error:
                self.run_copy(s3)
        self.assertTrue(s3.aborted)
        self.assertNotIn('private-token', str(error.exception))

    def test_s3_corruption_is_rejected(self):
        with self.assertRaisesRegex(RuntimeError, 'SHA-256 mismatch'):
            self.run_copy(S3(corrupt=True))


if __name__ == '__main__':
    unittest.main()
