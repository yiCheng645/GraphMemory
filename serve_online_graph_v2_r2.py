"""Original online R2 / port 8005 with persistent platform users and portable runtime."""
from pathlib import Path
import argparse,json,os
from entity_identity_v1 import ROOT,digest
from online_graph_r2_http_client import OnlineRouter,safe_root,VERSION,RETRIEVAL_POLICY,REVISION,user_settings
from online_graph_r2_http_provider import (DeclaredUserAgentProvider as OnlineProvider, validate_user_agent)
from online_graph_r2_retry_provider import ReconnectJournal as BoundedCallJournal, retry_policy
from online_graph_providers_v3 import CallJournal,JsonProvider,LiveExtraction,live_judge,BudgetedEmbedding,BudgetedReranker
from online_graph_r2_runtime import create_server,encoder
from online_graph_r2_runtime import load_env

DEFAULT_CONFIG=ROOT/'online_graph_service_v2_r2.example.json'


def settings(path):
    path=Path(path).resolve()
    if not path.is_relative_to(ROOT):
        raise ValueError('configuration must be inside the active project')
    config=json.loads(path.read_text(encoding='utf-8-sig'))
    if config.get('route')!=VERSION or config.get('retrieval_policy')!=RETRIEVAL_POLICY or config.get('mode') not in ('disabled','live'):
        raise ValueError('explicit online R2 configuration and retrieval policy required')
    safe_root(ROOT/config['data_root'])
    if config.get('revision')!=REVISION:
        raise ValueError('use the patched R2 example and a new isolated instance')
    if type(config.get('request_seconds')) is not int or not 30<=config['request_seconds']<=1740:
        raise ValueError('bounded request_seconds required')
    user_settings(config.get('users'),config.get('user_policy'))
    if set(config.get('providers',{}))!={'chat','embedding','reranker'}:
        raise ValueError('explicit extraction/identity, embedding and rerank providers required')
    for name,provider in config['providers'].items():
        if type(provider.get('timeout_seconds')) is not int or not 1<=provider['timeout_seconds']<=60:
            raise ValueError('bounded provider timeout required')
        retry_policy(provider)
        validate_user_agent(provider.get('user_agent','MemoryR2/1.0'))
        if not provider.get('api_key_env') or 'api_key' in provider:
            raise ValueError('credentials must use named environment variables')
        if name=='chat' and (not provider.get('model') or type(provider.get('max_output_tokens')) is not int or not 1<=provider['max_output_tokens']<=8192):
            raise ValueError('explicit bounded extraction model output configuration required')
    return config


def check_config(path=DEFAULT_CONFIG):
    config=settings(path)
    return {'route':VERSION,'retrieval_policy':RETRIEVAL_POLICY,'revision':REVISION,'mode':config['mode'],
            'max_query_characters':131072,'request_seconds':config['request_seconds'],
            'users':len(config['users']),'user_registration':config['user_policy']['mode'],
            'max_users':config['user_policy']['max_users'],'automatic_graph_extraction':config['mode']=='live',
            'new_model_calls':0,'credentials_loaded':False,'bounded_automatic_reconnect':True,
            'max_model_attempts':{name:retry_policy(p)['max_model_attempts'] for name,p in config['providers'].items()},
            'seed_policy':'merged_message_seeds_refill_to5_v1_preserving','live_start_requires_explicit_live_flag':True}


def create_live_router(config,enc,*,strict_graph=False):
    if config['mode']!='live':
        raise ValueError('explicit live mode required')
    root=safe_root(ROOT/config['data_root']);shared={}
    def factory(uid):
        if not shared:
            journal=BoundedCallJournal(root/'provider_calls.sqlite3')
            for name,provider in config['providers'].items():
                shared[name]=OnlineProvider(provider,journal,name,api_key=os.environ.get(provider['api_key_env'],''))
        return {'embedding':BudgetedEmbedding(shared['embedding'],uid),'reranker':BudgetedReranker(shared['reranker'],uid),
                'extractor':LiveExtraction(shared['chat'],uid),'judge':live_judge(shared['chat'],uid)}
    router=OnlineRouter(root,config['users'],factory,enc,policy_id=digest(config),
                        strict_graph=strict_graph,request_seconds=config['request_seconds'],user_policy=config['user_policy'])
    try:
        # Validate credentials, native endpoint adapters and persistent budgets
        # under the instance lock even when no platform users have arrived yet.
        # This creates no user, does not reserve a paid call, and sends nothing.
        factory('__startup_preflight__')
    except BaseException:
        router.close()
        raise
    return router


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--config',default=str(DEFAULT_CONFIG));parser.add_argument('--serve',action='store_true')
    parser.add_argument('--live',action='store_true');parser.add_argument('--port',type=int,default=8005)
    parser.add_argument('--strict-graph',action='store_true');args=parser.parse_args()
    if Path.cwd().resolve()!=ROOT:
        parser.error('run from the extracted project directory')
    if not args.serve:
        print(json.dumps(check_config(args.config)));return
    if not args.live:
        parser.error('explicit --live and mode=live configuration are both required')
    config=settings(args.config);load_env(ROOT/'.env');api_key=os.environ.get('MEMORY_API_KEY','')
    if len(api_key)<24:
        parser.error('MEMORY_API_KEY must contain at least 24 characters')
    router=create_live_router(config,encoder(),strict_graph=args.strict_graph)
    try:
        server=create_server(router,api_key,args.port)
        print(json.dumps({'listening':'http://127.0.0.1:'+str(args.port),'route':VERSION,
                          'retrieval_policy':RETRIEVAL_POLICY,'automatic_graph_extraction':True,
                          'requests_may_call_paid_providers':True}),flush=True)
        try:server.serve_forever()
        finally:server.server_close()
    finally:router.close()


if __name__=='__main__':main()
