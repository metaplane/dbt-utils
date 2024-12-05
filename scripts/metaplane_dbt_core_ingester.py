import base64
import gzip
import json
import time
import sys
import argparse
from urllib.request import Request, urlopen

def compress_and_encode_artifact(artifact_path: str) -> str:
    try:
        with open(artifact_path, mode="r") as f_in:
            compressed_artifact = gzip.compress(f_in.read().encode('utf-8'))
            encoded_artifact = base64.b64encode(compressed_artifact).decode('ascii')
            return encoded_artifact
    except IOError as e:
        print(f"Error reading artifact file {artifact_path}: {e}")
        sys.exit(1)

def package_artifacts_for_metaplane(connection_id: str, project_name: str, job_name: str) -> dict:
    try:
        encoded_manifest = compress_and_encode_artifact('target/manifest.json')
        encoded_run_results = compress_and_encode_artifact('target/run_results.json')
        
        payload = {
            "connectionId": connection_id,
            "projectName": project_name,
            "jobName": job_name,
            "encodedManifest": encoded_manifest,
            "encodedRunResult": encoded_run_results,
            "idempotencyId": f'{connection_id}-{time.perf_counter_ns()}'
        }
        return payload
    except Exception as e:
        print(f"Error packaging artifacts: {e}")
        sys.exit(1)

def push_artifacts_to_metaplane(connection_id: str, api_key: str, project_name: str, job_name: str):
    try:
        payload = package_artifacts_for_metaplane(connection_id, project_name, job_name)
        
        req = Request(
            url="https://dev.api.metaplane.dev/dbt-core/artifacts",
            data=json.dumps(payload).encode('utf-8'),
            headers={
                "Accept": "*/*",
                "Content-Type": "application/json",
                "Authorization": f"Bearer {api_key}",
                # cloudflare WAF really hates the urllib user agent
                "User-Agent": "python-requests/2.28.2",
            },
            method='POST'
        )
        
        with urlopen(req) as response:
            print(response.read().decode('utf-8'))
    except Exception as e:
        print(f"Error pushing artifacts to Metaplane: {e}")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description='Upload dbt artifacts to Metaplane')
    parser.add_argument('--connection-id', required=True, help='Metaplane Connection ID')
    parser.add_argument('--api-key', required=True, help='Metaplane API Key')
    parser.add_argument('--project-name', required=True, help='dbt Project Name')
    parser.add_argument('--job-name', required=True, help='dbt Job Name')
    
    args = parser.parse_args()
    
    push_artifacts_to_metaplane(
        connection_id=args.connection_id,
        api_key=args.api_key,
        project_name=args.project_name,
        job_name=args.job_name
    )

if __name__ == '__main__':
    main()