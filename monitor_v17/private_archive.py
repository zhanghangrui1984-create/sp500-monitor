"""Save complete report bundles to the dedicated private repository before delivery."""
import hashlib,json,os,re,shlex,shutil,subprocess,tempfile,zipfile
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request,urlopen

ARCHIVE_REPOSITORY='zhanghangrui1984-create/sp500-monitor-archive'


def api_json(path,allow_404=False):
    headers={'Accept':'application/vnd.github+json','User-Agent':'SP500-private-archive'}
    token=os.environ.get('SP500_ARCHIVE_CHECK_TOKEN','')
    if token:headers['Authorization']='Bearer '+token
    try:
        with urlopen(Request('https://api.github.com'+path,headers=headers),timeout=30) as response:
            return json.load(response)
    except HTTPError as exc:
        if allow_404 and exc.code==404:return None
        raise RuntimeError(f'Archive metadata check failed: HTTP {exc.code}') from None


def require_private_repository():
    info=api_json('/repos/'+ARCHIVE_REPOSITORY,allow_404=True)
    # The public monitor's GITHUB_TOKEN cannot read the private archive metadata.
    # A 404 is accepted only together with the subsequent successful SSH clone.
    # If the archive is ever made public, the API exposes it and saving stops.
    if info is not None and (info.get('private') is not True or info.get('full_name')!=ARCHIVE_REPOSITORY):
        raise RuntimeError('Archive repository is not private; refusing to upload the data bundle')


def _git(args,cwd,env):
    result=subprocess.run(['git',*args],cwd=cwd,env=env,capture_output=True,text=True,timeout=90)
    if result.returncode:
        raise RuntimeError('Private archive git operation failed: '+result.stderr[-400:].strip())
    return result.stdout.strip()


def publish(r,docx,archive,out):
    key=os.environ.get('SP500_ARCHIVE_DEPLOY_KEY','')
    run_id=os.environ.get('GITHUB_RUN_ID','');attempt=os.environ.get('GITHUB_RUN_ATTEMPT','1')
    if not key or not re.fullmatch(r'\d+',run_id) or not re.fullmatch(r'\d+',attempt):
        raise RuntimeError('Private archive credentials or workflow run identifier missing')
    require_private_repository()
    with zipfile.ZipFile(archive) as z:
        if json.loads(z.read('snapshot.json'))!=r:raise RuntimeError('Private archive snapshot mismatch')
        if z.read(Path(docx).name)!=Path(docx).read_bytes():raise RuntimeError('Private archive DOCX mismatch')
    host_keys=api_json('/meta')['ssh_keys']
    if not host_keys or any('\n' in k or not k.startswith(('ssh-ed25519 ','ecdsa-sha2-nistp256 ','ssh-rsa ')) for k in host_keys):
        raise RuntimeError('Invalid GitHub SSH host keys')
    metadata={k:r.get(k) for k in ['asof','generated_at','model_version','data_policy_version','report_format_version','code_commit','status']}
    metadata.update(workflow_run_id=run_id,workflow_run_attempt=attempt,
        workflow_url=f'https://github.com/zhanghangrui1984-create/sp500-monitor/actions/runs/{run_id}',
        files={'report.docx':hashlib.sha256(Path(docx).read_bytes()).hexdigest(),
               'data.zip':hashlib.sha256(Path(archive).read_bytes()).hexdigest()})
    with tempfile.TemporaryDirectory(prefix='sp500_private_archive_') as tmp:
        work=Path(tmp);identity=work/'identity';known=work/'known_hosts'
        identity.write_text(key.rstrip()+'\n',encoding='utf-8');identity.chmod(0o600)
        known.write_text(''.join('github.com '+k+'\n' for k in host_keys),encoding='utf-8')
        env=dict(os.environ,GIT_TERMINAL_PROMPT='0',
            GIT_SSH_COMMAND=shlex.join(['ssh','-i',str(identity),'-o','IdentitiesOnly=yes',
                '-o','StrictHostKeyChecking=yes','-o','UserKnownHostsFile='+str(known)]))
        for name in ['SP500_ARCHIVE_DEPLOY_KEY','SP500_ARCHIVE_CHECK_TOKEN','EMAIL_PASSWORD',
                     'EMAIL_ADDRESS','EMAIL_RECEIVER','FRED_API_KEY','GH_TOKEN','GITHUB_TOKEN']:
            env.pop(name,None)
        checkout=work/'archive'
        _git(['clone','--depth','1','git@github.com:'+ARCHIVE_REPOSITORY+'.git',str(checkout)],work,env)
        latest=checkout/'latest';latest.mkdir(exist_ok=True)
        shutil.copy2(docx,latest/'report.docx');shutil.copy2(archive,latest/'data.zip')
        encoded=json.dumps(metadata,ensure_ascii=False,indent=2)+'\n'
        (latest/'metadata.json').write_text(encoded,encoding='utf-8')
        runs=checkout/'runs';runs.mkdir(exist_ok=True)
        (runs/f'{run_id}-{attempt}.json').write_text(encoded,encoding='utf-8')
        _git(['add','latest',f'runs/{run_id}-{attempt}.json'],checkout,env)
        if _git(['diff','--cached','--name-only'],checkout,env):
            _git(['-c','user.name=SP500 Archive Bot','-c','user.email=github-actions@github.com',
                'commit','-m',f"Daily report {r.get('asof')} run {run_id}-{attempt}"],checkout,env)
            _git(['push','origin','HEAD'],checkout,env)
        commit=_git(['rev-parse','HEAD'],checkout,env)
    receipt=dict(metadata,repository=ARCHIVE_REPOSITORY,private=True,archive_commit=commit,
        archive_url=f'https://github.com/{ARCHIVE_REPOSITORY}/tree/{commit}/latest')
    (Path(out)/'archive_receipt.json').write_text(json.dumps(receipt,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    return receipt
