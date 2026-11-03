import hashlib,json,os,tempfile,unittest,zipfile
from pathlib import Path
from unittest.mock import patch
from monitor_v17 import private_archive,main


class PrivateArchiveTests(unittest.TestCase):
    def test_public_destination_is_rejected(self):
        with patch.object(private_archive,'api_json',return_value={'private':False,'full_name':private_archive.ARCHIVE_REPOSITORY}):
            with self.assertRaisesRegex(RuntimeError,'not private'):private_archive.require_private_repository()
        with patch.object(private_archive,'api_json',return_value=None):
            private_archive.require_private_repository()
        with patch.object(private_archive,'api_json',return_value={'private':True,'full_name':private_archive.ARCHIVE_REPOSITORY}):
            private_archive.require_private_repository()

    def test_repository_scoped_publish_and_receipt(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);docx=root/'report.docx';docx.write_bytes(b'example docx')
            bundle=root/'bundle.zip';r={'asof':'2026-11-02','generated_at':'2026-11-03T01:00:00Z','code_commit':'a'*40}
            with zipfile.ZipFile(bundle,'w') as z:
                z.writestr('snapshot.json',json.dumps(r));z.writestr(docx.name,docx.read_bytes())
            commands=[];checkout=[]
            def git(args,cwd,env):
                commands.append(args)
                self.assertNotIn('SP500_ARCHIVE_DEPLOY_KEY',env);self.assertNotIn('EMAIL_PASSWORD',env)
                self.assertIn('StrictHostKeyChecking=yes',env['GIT_SSH_COMMAND'])
                if args[0]=='clone':
                    p=Path(args[-1]);p.mkdir();checkout.append(p)
                    self.assertEqual(args[-2],'git@github.com:'+private_archive.ARCHIVE_REPOSITORY+'.git')
                if args[:3]==['diff','--cached','--name-only']:return 'latest/data.zip'
                if args[0]=='push':
                    self.assertEqual((checkout[0]/'latest/data.zip').read_bytes(),bundle.read_bytes())
                    self.assertEqual((checkout[0]/'latest/report.docx').read_bytes(),docx.read_bytes())
                return 'b'*40 if args[0]=='rev-parse' else ''
            with patch.dict(os.environ,{'SP500_ARCHIVE_DEPLOY_KEY':'test private key','GITHUB_RUN_ID':'12345','GITHUB_RUN_ATTEMPT':'1','EMAIL_PASSWORD':'test mailbox secret'}),patch.object(private_archive,'api_json',side_effect=[None,{'ssh_keys':['ssh-ed25519 example-host-key']}]),patch.object(private_archive,'_git',side_effect=git):
                receipt=private_archive.publish(r,docx,bundle,root)
            self.assertTrue(receipt['private']);self.assertEqual(receipt['archive_commit'],'b'*40)
            self.assertEqual(receipt['files']['data.zip'],hashlib.sha256(bundle.read_bytes()).hexdigest())
            saved=(root/'archive_receipt.json').read_text(encoding='utf-8')
            self.assertNotIn('test private key',saved);self.assertNotIn('test mailbox secret',saved)
            self.assertTrue(any(args[:2]==['push','origin'] for args in commands))
            self.assertFalse(checkout[0].exists())

    def test_snapshot_mismatch_is_not_archived(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);docx=root/'report.docx';docx.write_bytes(b'docx')
            bundle=root/'bundle.zip'
            with zipfile.ZipFile(bundle,'w') as z:z.writestr('snapshot.json','{}')
            with patch.dict(os.environ,{'SP500_ARCHIVE_DEPLOY_KEY':'test key','GITHUB_RUN_ID':'12'}),patch.object(private_archive,'require_private_repository'),patch.object(private_archive,'_git') as git:
                with self.assertRaisesRegex(RuntimeError,'snapshot mismatch'):
                    private_archive.publish({'asof':'2026-11-02'},docx,bundle,root)
                git.assert_not_called()

    def test_archive_failure_prevents_email(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);data=root/'data';data.mkdir()
            report={'status':'failed','asof':'未知','generated_at':'2026-11-03T01:00:00Z','model_version':main.CONFIG['version']}
            with patch.object(main,'BASE',root),patch.object(main,'DATA',data),patch.object(main,'generate',return_value=report),patch.object(main,'build_deliverables',return_value=(root/'report.docx',root/'bundle.zip')),patch.object(private_archive,'publish',side_effect=RuntimeError('archive unavailable')),patch.object(main,'send') as send,patch('sys.argv',['main','--archive-private']):
                with self.assertRaisesRegex(RuntimeError,'archive unavailable'):main.main()
                send.assert_not_called()


if __name__=='__main__':unittest.main()
