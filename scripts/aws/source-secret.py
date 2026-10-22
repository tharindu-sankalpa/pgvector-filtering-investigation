"""Collect read-only HTTPS source URLs without echoing them or using history."""
import getpass
import json
from pathlib import Path
from urllib.parse import urlsplit

files = []
for name, default_size in [('gen_wot_2.5m.parquet', 30940143579), ('wot_retrieval_queries.parquet', 40132555)]:
    url = getpass.getpass(f'Read-only HTTPS URL for {name}: ')
    if urlsplit(url).scheme != 'https':
        raise SystemExit('An HTTPS URL is required')
    size = int(input(f'Exact source bytes [{default_size}]: ') or default_size)
    files.append({'name': name, 'url': url, 'size': size})
secret = {'apiVersion': 'v1', 'kind': 'Secret', 'metadata': {'name': 'dataset-source-urls', 'namespace': 'benchmarks'},
          'type': 'Opaque', 'stringData': {'SOURCE_FILES_JSON': json.dumps(files)}}
path = Path('.local/source-files-secret.json')
path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
# Create with private permissions before writing URL contents.
import os
fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
os.fchmod(fd, 0o600)
with os.fdopen(fd, 'w') as stream:
    json.dump(secret, stream)
print('Private source Secret prepared; run deploy.py copy to submit the AWS worker.')
