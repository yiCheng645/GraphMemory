"""Verify the clean source release without creating users or loading credentials."""
from pathlib import Path
import argparse,hashlib,json

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--runtime',action='store_true');args=parser.parse_args()
    root=Path(__file__).resolve().parent
    manifest=json.loads((root/'versions/r2-graph-degrade-20261030/manifest.json').read_text())
    for name,expected in manifest['files_sha256'].items():
        path=(root/name).resolve()
        if not path.is_relative_to(root) or hashlib.sha256(path.read_bytes()).hexdigest()!=expected:
            raise ValueError('Clean release differs: '+name)
    from serve_online_graph_v2_r2 import check_config
    cfg=check_config()
    if cfg['revision']!=manifest['revision'] or cfg['credentials_loaded'] or cfg['new_model_calls']:
        raise ValueError('Unexpected default configuration')
    if args.runtime:
        from online_graph_r2_runtime import encoder
        if encoder().name!='o200k_base':raise ValueError('Original R2 tokenizer required')
    print(json.dumps({'revision':cfg['revision'],'files_verified':len(manifest['files_sha256']),
        'tokenizer_loaded':args.runtime,'credentials_loaded':False,'new_model_calls':0}))

if __name__=='__main__':main()
