"""Provision only the public, frozen o200k_base vocabulary; never load credentials."""
from pathlib import Path
import argparse
import hashlib
import os
import urllib.request

ROOT=Path(__file__).resolve().parents[1]
FILENAME='fb374d419588a4632f3f557e76b4b70aebbca790'
EXPECTED='446a9538cb6c348e3516120d7c08b09f57c36495e2acfffe59a5bf8b0cfb1a2d'
URL='https://openaipublic.blob.core.windows.net/encodings/o200k_base.tiktoken'

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--from-file',type=Path);args=parser.parse_args()
    directory=ROOT/'data/tokenizer-cache';target=directory/FILENAME
    if target.exists():
        if hashlib.sha256(target.read_bytes()).hexdigest()!=EXPECTED:raise ValueError('Existing tokenizer differs; refusing to overwrite it')
        print('Verified existing o200k_base cache');return
    if args.from_file:
        raw=args.from_file.read_bytes()
    else:
        request=urllib.request.Request(URL,headers={'User-Agent':'GraphMemory/1.0 tokenizer-bootstrap'})
        with urllib.request.urlopen(request,timeout=30) as response:raw=response.read(5*1024*1024+1)
    if len(raw)>5*1024*1024 or hashlib.sha256(raw).hexdigest()!=EXPECTED:raise ValueError('Public tokenizer hash mismatch')
    directory.mkdir(parents=True,exist_ok=True)
    pending=target.with_suffix('.pending')
    with pending.open('xb') as stream:stream.write(raw);stream.flush();os.fsync(stream.fileno())
    os.replace(pending,target)
    print('Installed verified public o200k_base cache; paid API calls: 0')

if __name__=='__main__':main()
