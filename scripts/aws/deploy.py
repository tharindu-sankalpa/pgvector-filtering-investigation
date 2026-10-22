"""Deploy the single-cluster benchmark. Run with python3 scripts/aws/deploy.py.

Requires aws, eksctl, kubectl, helm, PyYAML; defaults to .local/kubeconfig.
Only private deployment state contains account identifiers and image digests.
"""
import argparse
import base64
import json
import os
from pathlib import Path
import secrets
import subprocess

import yaml

ROOT = Path(__file__).resolve().parents[2]
REGION = 'ap-south-1'
CLUSTER = 'pgvector-investigation'
KUBECONFIG = str(ROOT / '.local/kubeconfig')
os.environ['KUBECONFIG'] = KUBECONFIG


def run(args, data=None, capture=True):
    result = subprocess.run(args, input=data, text=True, check=True,
                            stdout=subprocess.PIPE if capture else None)
    return result.stdout.strip() if capture else ''


def aws(*args):
    return json.loads(run(['aws', '--region', REGION, *args, '--output', 'json']) or '{}')


def kube(*args, data=None):
    return run(['kubectl', '--kubeconfig', KUBECONFIG, *args], data=data)


def apply(obj):
    return kube('apply', '-f', '-', data=json.dumps(obj))


def apply_file(path, replacements=None):
    value = (ROOT / path).read_text()
    for old, new in (replacements or {}).items():
        value = value.replace(old, new)
    return kube('apply', '-f', '-', data=value)


def create_data_account(name, bucket, upload=False):
    policy = {'Version': '2012-10-17', 'Statement': [
        {'Effect': 'Allow', 'Action': ['s3:ListBucket'], 'Resource': f'arn:aws:s3:::{bucket}'},
        {'Effect': 'Allow', 'Action': ['s3:GetObject'] + (['s3:PutObject', 's3:AbortMultipartUpload', 's3:ListMultipartUploadParts'] if upload else []),
         'Resource': f'arn:aws:s3:::{bucket}/datasets/*'}]}
    path = ROOT / '.local' / f'{name}-policy.json'
    path.write_text(json.dumps(policy))
    # eksctl creates a role restricted to this service account's OIDC subject.
    run(['eksctl', 'create', 'iamserviceaccount', '--cluster', CLUSTER,
         '--region', REGION, '--namespace', 'benchmarks', '--name', name,
         '--role-name', f'{CLUSTER}-{name}', '--attach-policy-arn',
         ensure_policy(name, policy), '--approve', '--override-existing-serviceaccounts',
         ], capture=False)


def ensure_policy(name, document):
    account = aws('sts', 'get-caller-identity')['Account']
    arn = f'arn:aws:iam::{account}:policy/{CLUSTER}-{name}'
    try:
        aws('iam', 'get-policy', '--policy-arn', arn)
    except subprocess.CalledProcessError:
        arn = aws('iam', 'create-policy', '--policy-name', f'{CLUSTER}-{name}',
                  '--policy-document', json.dumps(document))['Policy']['Arn']
    return arn


def bootstrap_storage(bucket):
    try:
        run(['aws', 's3api', 'head-bucket', '--bucket', bucket, '--region', REGION])
    except subprocess.CalledProcessError:
        aws('s3api', 'create-bucket', '--bucket', bucket,
            '--create-bucket-configuration', f'LocationConstraint={REGION}')
    aws('s3api', 'put-public-access-block', '--bucket', bucket,
        '--public-access-block-configuration', 'BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true')
    aws('s3api', 'put-bucket-encryption', '--bucket', bucket,
        '--server-side-encryption-configuration', json.dumps({'Rules': [{'ApplyServerSideEncryptionByDefault': {'SSEAlgorithm': 'AES256'}}]}))
    aws('s3api', 'put-bucket-versioning', '--bucket', bucket, '--versioning-configuration', 'Status=Enabled')
    aws('s3api', 'put-bucket-lifecycle-configuration', '--bucket', bucket,
        '--lifecycle-configuration', json.dumps({'Rules': [{'ID': 'abort-incomplete', 'Status': 'Enabled', 'Filter': {'Prefix': ''}, 'AbortIncompleteMultipartUpload': {'DaysAfterInitiation': 1}}]}))


def sync_secret(namespace, cluster, target, results=False):
    value = json.loads(kube('-n', namespace, 'get', 'secret', cluster + '-app', '-o', 'json'))
    decode = lambda key: base64.b64decode(value['data'][key]).decode()
    prefix = 'RESULTS_PG_' if results else 'PG_'
    data = {prefix+'HOST': f'{cluster}-rw.{namespace}.svc.cluster.local',
            prefix+'USER': decode('username'), prefix+'PASSWORD': decode('password')}
    apply({'apiVersion': 'v1', 'kind': 'Secret', 'metadata': {'name': target, 'namespace': 'benchmarks'},
           'type': 'Opaque', 'stringData': data})


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('phase', choices=['storage', 'addons', 'workloads', 'secrets', 'copy', 'stage', 'smoke', 'full'])
    parser.add_argument('--monitoring-chart-version')
    args = parser.parse_args()
    account = aws('sts', 'get-caller-identity')['Account']
    registry = f'{account}.dkr.ecr.{REGION}.amazonaws.com'
    bucket = f'pgvector-investigation-{account}-{REGION}'
    if args.phase == 'storage':
        bootstrap_storage(bucket)
        print('Dataset bucket configured (private, encrypted, versioned)')
        return
    run(['aws', 'eks', 'update-kubeconfig', '--region', REGION, '--name', CLUSTER,
         '--kubeconfig', KUBECONFIG, '--alias', CLUSTER])
    if args.phase == 'addons':
        apply_file('infrastructure/aws/namespaces.yaml')
        run(['eksctl','create','iamserviceaccount','--cluster',CLUSTER,'--region',REGION,
             '--namespace','kube-system','--name','ebs-csi-controller-sa',
             '--attach-policy-arn','arn:aws:iam::aws:policy/service-role/AmazonEBSCSIDriverPolicy',
             '--approve','--override-existing-serviceaccounts'],capture=False)
        service_account = json.loads(kube('-n','kube-system','get','sa','ebs-csi-controller-sa','-o','json'))
        role = service_account['metadata']['annotations']['eks.amazonaws.com/role-arn']
        addons = aws('eks','list-addons','--cluster-name',CLUSTER)['addons']
        if 'aws-ebs-csi-driver' not in addons:
            aws('eks','create-addon','--cluster-name',CLUSTER,'--addon-name','aws-ebs-csi-driver',
                '--service-account-role-arn',role,'--resolve-conflicts','OVERWRITE')
        run(['aws','eks','wait','addon-active','--region',REGION,'--cluster-name',CLUSTER,'--addon-name','aws-ebs-csi-driver'],capture=False)
        apply_file('infrastructure/aws/storage.yaml')
        create_data_account('benchmark-data-reader',bucket)
        create_data_account('dataset-uploader',bucket,upload=True)
        return
    if args.phase == 'workloads':
        # Chart versions resolved explicitly, retained in private deployment evidence.
        run(['helm','repo','add','cnpg','https://cloudnative-pg.github.io/charts'],capture=False)
        run(['helm','repo','add','prometheus-community','https://prometheus-community.github.io/helm-charts'],capture=False)
        run(['helm','repo','update'],capture=False)
        charts = json.loads(run(['helm','search','repo','cnpg/cloudnative-pg','--versions','-o','json']))
        cnpg_chart = next(c['version'] for c in charts if c['app_version'] in ['1.30.1','v1.30.1'])
        prom_chart = args.monitoring_chart_version or json.loads(run(['helm','search','repo','prometheus-community/kube-prometheus-stack','-o','json']))[0]['version']
        run(['helm','upgrade','--install','cnpg-operator','cnpg/cloudnative-pg','--version',cnpg_chart,
             '--namespace','cnpg-system','--create-namespace','--kubeconfig',KUBECONFIG,
             '--set','nodeSelector.workload=services','--wait','--timeout','10m'],capture=False)
        apply_file('infrastructure/cnpg-pgvector/monitoring/cnpg-extra-monitoring.yaml')
        image = registry+'/pgvector-investigation/postgresql:18.6-pgvector0.8.7'
        apply_file('infrastructure/cnpg-pgvector/cluster.yaml',{'<account-id>.dkr.ecr.ap-south-1.amazonaws.com/pgvector-investigation/postgresql:18.6-pgvector0.8.7':image})
        apply_file('infrastructure/aws/results-cluster.yaml')
        apply_file('infrastructure/aws/dataset-pvc.yaml')
        try:
            kube('-n','monitoring','get','secret','grafana-admin')
        except subprocess.CalledProcessError:
            apply({'apiVersion':'v1','kind':'Secret','metadata':{'name':'grafana-admin','namespace':'monitoring'},
                   'stringData':{'admin-user':'admin','admin-password':secrets.token_urlsafe(32)}})
        run(['helm','upgrade','--install','prometheus','prometheus-community/kube-prometheus-stack',
             '--version',prom_chart,'--namespace','monitoring','--kubeconfig',KUBECONFIG,
             '-f',str(ROOT/'infrastructure/cnpg-pgvector/monitoring/values-prometheus-grafana.yaml'),
             '--wait','--timeout','15m'],capture=False)
        apply_file('infrastructure/cnpg-pgvector/monitoring/cnpg-podmonitor.yaml')
        dashboard = json.loads(kube('-n','monitoring','create','configmap','cnpg-dashboard',
            '--from-file='+str(ROOT/'infrastructure/cnpg-pgvector/monitoring/dashboards/cnpg-pgvector-overview.json'),
            '--dry-run=client','-o','json'))
        dashboard['metadata']['labels']={'grafana_dashboard':'1'}
        apply(dashboard)
        (ROOT/'.local/chart-versions.json').write_text(json.dumps({'cnpg':cnpg_chart,'kube-prometheus-stack':prom_chart},indent=2))
        return
    if args.phase == 'secrets':
        for ns, name in [('cnpg-pgvector','ml-pgvector-benchmark'),('benchmark-results','benchmark-results')]:
            kube('-n',ns,'wait','cluster/'+name,'--for=condition=Ready','--timeout=15m')
        sync_secret('cnpg-pgvector','ml-pgvector-benchmark','cnpg-pgvector-secrets')
        sync_secret('benchmark-results','benchmark-results','pg-results-secrets',results=True)
        print('Benchmark connection secrets configured')
        return
    if args.phase == 'copy':
        value = json.loads((ROOT / '.local/source-files-secret.json').read_text())
        apply(value)
        script = json.loads(kube('-n', 'benchmarks', 'create', 'configmap', 'dataset-copy-script',
            '--from-file=' + str(ROOT / 'scripts/aws/stream_to_s3.py'), '--dry-run=client', '-o', 'json'))
        apply(script)
        apply_file('infrastructure/aws/source-copy-job.yaml', {'<dataset-bucket>': bucket})
        print('Temporary dataset copy job submitted; source URLs are not logged')
        return
    if args.phase == 'full':
        from datetime import datetime, timezone
        kube('-n', 'benchmarks', 'wait', 'job/eks-stack-smoke', '--for=condition=complete', '--timeout=30m')
        active = [j['metadata']['name'] for j in json.loads(kube('-n', 'benchmarks', 'get', 'jobs', '-o', 'json'))['items']
                  if j.get('status', {}).get('active', 0)]
        if active:
            raise RuntimeError('Wait for active Jobs before starting a full study: ' + ', '.join(active))
        run_id = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
        name = 'pgvector-study-' + run_id.lower()
        script = json.loads(kube('-n', 'benchmarks', 'create', 'configmap', name,
            '--from-file=' + str(ROOT / 'scripts/aws/full_study.py'), '--dry-run=client', '-o', 'json'))
        script['immutable'] = True
        apply(script)
        image_digest = aws('ecr', 'describe-images', '--repository-name', 'pgvector-investigation/benchmark-engine',
            '--image-ids', 'imageTag=eks-20261022.1')['imageDetails'][0]['imageDigest']
        postgres_digest = aws('ecr', 'describe-images', '--repository-name', 'pgvector-investigation/postgresql',
            '--image-ids', 'imageTag=18.6-pgvector0.8.7')['imageDetails'][0]['imageDigest']
        apply_file('infrastructure/aws/full-study-job.yaml', {'<account-id>': account, '<study-name>': name,
            '<run-id>': run_id, '<benchmark-digest>': image_digest, '<postgres-digest>': postgres_digest})
        (ROOT / '.local/full-study.json').write_text(json.dumps({'job': name, 'run_id': run_id, 'image_digest': image_digest}, indent=2))
        print('Full study submitted: ' + name + '; progress is stored in benchmark_study_runs')
        return
    if args.phase == 'smoke':
        kube('-n', 'benchmarks', 'wait', 'job/stage-datasets', '--for=condition=complete', '--timeout=1h')
        script = json.loads(kube('-n', 'benchmarks', 'create', 'configmap', 'eks-smoke-script',
            '--from-file=' + str(ROOT / 'scripts/aws/smoke.py'), '--dry-run=client', '-o', 'json'))
        apply(script)
        apply_file('infrastructure/aws/smoke-job.yaml', {'<account-id>': account})
        print('Smoke Job submitted: 1,000 rows only; source Parquet is read-only')
        return
    if args.phase == 'stage':
        apply_file('infrastructure/aws/dataset-stage.yaml',{'<dataset-bucket>':bucket})
        print('Dataset cache staging job submitted; wait for completion before benchmarking')


if __name__ == '__main__':
    main()
