#!/usr/bin/env python3
"""Rebuild the fixed upstream per-path audit; family matches stay review_pending."""
import argparse, json, subprocess, re, collections, hashlib, datetime
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--source', type=Path, default=ROOT.parent/'zhikuncode')
SOURCE=parser.parse_args().source.resolve()
BASE='3e536438ccc1d7b500416c90e92ee08783f5ee41'; TARGET='053adf9071dc1996aa1ebfa30c0a23d587ffad5c'
def git(*args): return subprocess.check_output(['git','-C',str(SOURCE),'-c','core.quotePath=false',*args])
def tree(rev):
 result={}
 for row in git('ls-tree','-r','-z',rev).split(b'\0'):
  if row:
   info,path=row.split(b'\t',1);result[path.decode()]=info.split()[2].decode()
 return result
base_tree=tree(BASE);target_tree=tree(TARGET)
frontend_audit=json.loads((ROOT/'docs/migration/frontend-path-audit.json').read_text())['paths']
test_audits={}
for audit_path in sorted((ROOT/'docs/migration').glob('*-backend-test-audit.json')):
 for source_path, review in json.loads(audit_path.read_text()).get('paths',{}).items():
  if source_path in test_audits: raise ValueError('Duplicate backend test audit: '+source_path)
  test_audits[source_path]=(audit_path.name,review)
ledger={};cap={}
for p in sorted((ROOT/'docs/migration').glob('*capabilities.json')):
 x=json.loads(p.read_text());name=p.name
 for n,c in enumerate(x.get('capabilities',[])):
  cid=c.get('id') or c.get('capability') or str(n)
  entry={'ref':f'docs/migration/{name}#{cid}','status':c.get('status','review_pending'),'paths':c.get('sourcePaths',[])+([c['sourcePath']] if 'sourcePath' in c else []),'targets':c.get('implementationPaths',c.get('implementation',c.get('rust',[]))),'verification':c.get('validation',c.get('verification',x.get('verification',[])))}
  for java_path in re.findall(r'[A-Za-z0-9_/]+\.java',c.get('source','')):
   source_path=java_path if java_path.startswith('backend/') else 'backend/src/main/java/com/aicodeassistant/'+java_path
   if source_path in target_tree or source_path in base_tree:entry['paths'].append(source_path)
  cap[cid]=entry;ledger[entry['ref']]=entry
# The mapping is conservative: family/keyword matches record a review obligation;
# only explicit ledger source-path associations inherit documented coverage.
def mapped(category, ids, targets=(), note='', exact=False, disposition='migrate'):
 refs=[cap[i]['ref'] for i in ids if i in cap]
 dest=list(targets)
 for i in ids:
  if i in cap: dest.extend(cap[i]['targets'])
 dest=list(dict.fromkeys(p for p in dest if (ROOT/re.sub(r':\d+$','',p)).exists()))
 return {'category':category,'disposition':disposition,'status':'documented_coverage' if exact and refs else 'review_pending','capabilityRefs':refs,'targetPaths':dest,'verificationStatus':'see_capability_evidence' if refs else 'pending','notes':note or 'Capability-family association; source delta must be reviewed against the referenced implementation and tests.'}
def excluded(category,note,refs=()):
 return {'category':category,'disposition':'excluded','status':'excluded_by_scope','capabilityRefs':[cap[i]['ref'] for i in refs if i in cap],'targetPaths':[],'verificationStatus':'not_applicable','notes':note}
def classify_path(p):
 q=p.lower();name=Path(p).name;stem=Path(p).stem
 if q.startswith('docs/case-studies/') or q.startswith('scripts/measure-king') or q.startswith('scripts/update-king'):
  return excluded('historical-evaluation','User excludes all historical evaluation material and copied source snapshots. These are not current production code.')
 if q.startswith('tools/office-regression/'):
  return mapped('native-office',['native-office-regression-and-nonfinite-xlsx','native-full-document-toolchain'],exact=True,note='Office fixtures/checkers retained; Docker invocation is replaced by the actual Apple Silicon native runner. Do not treat Dockerfile naming as exclusion of this capability.')
 if ('meoo' in q or 'publish-oss' in q or 'publishoss' in q or '/config/oss/' in q or '/artifact/publication/' in q or 'publishartifacttool' in q or 'ossartifact' in q or 'clipboardimagepublication' in q or 'artifactpublicationpolicy' in q or 'localfilepublisher' in q or 'pasteimagepublisher' in q or 'journey_publication' in q) and not q.endswith(('application.yml','securityconfig.java')):
  return excluded('public-publication','User excludes OSS, Meoo, flyai and public file/image upload. Local attachments, authorized previews and trust displays are retained separately.',['public-publication'])
 if q in ('dockerfile','docker-compose.yml','docker-entrypoint.sh','.dockerignore') or q.startswith('docker/'):
  return {'category':'mixed-container-environment','disposition':'adapt_partial','status':'documented_coverage','capabilityRefs':[cap['native-full-document-toolchain']['ref'],cap['native-env-summary-and-speech-forwarding']['ref'],cap['configuration-and-greenfield-schema']['ref']],'targetPaths':cap['native-full-document-toolchain']['targets'],'verificationStatus':'native_toolchain_verified_container_parts_excluded','notes':'Linux/Docker deployment, Meoo/flyai and OSS parts are excluded. Office/PDF/media/OCR/CJK dependency installation is preserved through macOS dev sync. Reviewed source net hunks: provider/summary/ASR settings use native environment plumbing; Skill/MCP state uses native durable storage. Java startup/log ownership is replaced by explicit local runtime paths. .dockerignore publication credentials exclusions are container-only. All intermediate commit blobs remain recorded; no container image/build claim.'}
 if '/powershell/' in q:
  return excluded('windows-powershell','Windows PowerShell implementation is excluded for Apple Silicon macOS. The only range delta corrects dedicated tool names; native Bash and existing dedicated tools remain.',['powershell-prompt-tool-names'])
 if not q.startswith('frontend/'):
  exact=[(cid,c) for cid,c in cap.items() if p in c['paths'] and c['status'] not in ('excluded','excluded_by_user_decision')]
  if exact:
   result=mapped('exact-capability-association',[cid for cid,c in exact],exact=True,note='Exact source-path association. Capability ledger states native adaptation, exclusions and actual validation limits. Coverage means a reviewable implementation/evidence link, not automatic approval of every source hunk or external API.')
   if p in ('README.md','CHANGELOG.md','docs/README_EN.md','.env.example','backend/src/main/resources/application.yml'):
    result['disposition']='adapt_partial'
    result['notes']+=' Mixed documentation/configuration: historical evaluation references, OSS/Meoo/flyai and Linux deployment hunks are excluded; selected product/runtime settings are adapted to native names. A capability reference does not mean every line in this source file was copied.'
   result['referencedCapabilityStatuses']={c['ref']:c['status'] for cid,c in exact}
   if any('pending' in c['status'] or 'progress' in c['status'] for cid,c in exact):result['verificationStatus']='pending_final_verification'
   return result
 if q.startswith('frontend/') and p in frontend_audit:
  a=frontend_audit[p]
  return {'category':'frontend-path-audit','disposition':a['disposition'],'status':'excluded_by_scope' if a['disposition']=='excluded_by_user' else 'documented_coverage','capabilityRefs':a['capabilityRefs'],'targetPaths':a['targetPaths'],'verificationStatus':a['reviewStatus'],'pathAuditRef':'frontend-path-audit.json#/paths/'+p.replace('~','~0').replace('/','~1'),'objectEvidence':a['objectEvidence'],'notes':a['notes']}
 if q.startswith('frontend/'):
  if any(v in q for v in ['inkmountstudio','inkretreatceremony','framed-art','retreat-ceremony']): return excluded('theme-exclusions','Framed export and retreat ceremony explicitly excluded by user.',['theme-exclusions'])
  exact=[c for c in cap.values() if c['ref'].endswith('#publishing-exclusions') is False and any(p==s or p.startswith(s.rstrip('/')+'/') for s in c['paths'])]
  ids=[]
  if any(v in q for v in ['theme','jelly','glass','ink','spaceship','palette','design-token','zksyntax','zkmonaco']): ids=['themes','ui-foundation']
  elif any(v in q for v in ['memory']):ids=['memory']
  elif any(v in q for v in ['merge','handoff']):ids=['merge-ui','ui-recovery']
  elif 'skill' in q:ids=['skills']
  elif 'mcp' in q:ids=['mcp']
  elif any(v in q for v in ['voice','speech','recorder']):ids=['voice']
  elif any(v in q for v in ['verify','evidence']):ids=['evidence']
  elif 'activitydecision' in q or 'activityapi' in q:ids=['activity']
  elif any(v in q for v in ['promptinput','promptdraft','draft','keyboard','input/','fileupload']):ids=['drafts','file-paths','stop']
  elif any(v in q for v in ['sessionmodel','sessionpermission','settingspanel']):ids=['model-config']
  elif any(v in q for v in ['message','turnview','structuredtool','contentblock','toolcall']):ids=['turns','tool-display']
  elif any(v in q for v in ['workbench','statusbar','header','usagestat']):ids=['workbenches','ui-foundation']
  elif any(v in q for v in ['sidebar','sessionlist','pagetitle','tabstatus']):ids=['sidebar']
  elif q.startswith('frontend/src/api/'):ids=['transport']
  else:ids=['ui-foundation']
  result=mapped('frontend',ids,targets=[p] if (ROOT/p).exists() else [],exact=bool(exact),note='Preserve Rust REST/WS/authz, local references, system theme, API keys and all existing workbench entrances; source Java API or public-upload imports must not be copied blindly. Family-only matches remain review_pending.')
  for e in exact:
   if e['ref'] not in result['capabilityRefs']:result['capabilityRefs'].append(e['ref'])
  if q.endswith(('.png','.webm','.mp4','.jpg','.jpeg')) and not (ROOT/p).exists():result.update(status='review_pending',notes='Source UI artifact or screenshot not copied. Relevant behavior covered by current native regression; asset disposition needs path review.')
  return result
 if q.startswith('python-service/'):
  ids=['python-cli-permission-and-errors'] if '/cli/' in q or 'cli_permission' in q else ['python-browser-lifecycle-and-disconnect']
  if any(v in q for v in ['journey','interaction_truth']):ids+=['truthful-browser-click-type-and-screenshots','verifyjourney-dsl-http-owned-preview-and-evidence']
  result=mapped('python-sidecar',ids,[p] if (ROOT/p).exists() else [],exact=any(p in cap[i]['paths'] for i in ids),note='Three-way merge keeps UDS, sandbox/navigation constraints, error storage and real lifecycle/Office tests. Container-specific process test replaced by native67-scenario gate; publication-only code excluded separately.')
  if 'token_estimation' in q:result=mapped('python-token-test',[],[p],note='Source801aa6d0 warms tokenizer before measuring cached endpoint; verify Rust sidecar test retained equivalent deterministic warmup.')
  return result
 if q.startswith('.github/'):
  return mapped('native-ci',['native-environment-and-ci-policy','native-office-regression-and-nonfinite-xlsx'],exact=True,note='macOS CI runs real native Office and Rust UDS tests. Linux container Trivy workflow is excluded; cargo-deny and candidate gitleaks retained.')
 if p in ('README.md','CHANGELOG.md') or q=='docs/readme_en.md':
  return mapped('user-documentation',[],['README.md','CHANGELOG.md','docs/migration/2026-10-migration.md'],note='Adapt capability descriptions, new defaults and local macOS setup to zkcode. Historical review links/publication/Linux deployment sections are excluded. Source README/CHANGELOG text is not a blanket import; final documentation review pending.',disposition='adapt_partial')
 if q.startswith('docs/test-results/'):
  return {'category':'historical-ui-validation-artifact','disposition':'reference_not_imported','status':'documented_coverage','capabilityRefs':[cap['ui-foundation']['ref']],'targetPaths':['frontend/e2e/production-backend.spec.ts'],'verificationStatus':'not_claimed_executed','notes':'Historical screenshot/report artifact is not a production UI asset. Preserve source blob identity as audit evidence; use current native tests and browser evidence, never label this old screenshot as a new successful run.'}
 if q.startswith('docs/'):
  if any(v in q for v in ['evaluation','评测','测评','demo','recording','benchmark']):return excluded('historical-docs','Historical evaluations, recordings and demos explicitly excluded.')
  return mapped('supporting-documentation',[],['docs/migration/2026-10-migration.md'],note='Java-specific architecture/deployment text must be adapted to Rust/macOS; useful product behavior and contracts require documentation review.',disposition='adapt_partial')
 if q=='.env.example' or q.endswith('/application.yml'):
  return mapped('runtime-configuration',['providers-models','independent-summary','native-full-document-toolchain'],['.env.example','crates/zk-server/src/config.rs'],note='Mixed configuration: retain provider/summary/ASR/skill/session settings with Rust native names; exclude OSS/Meoo/flyai/Linux. Parent owns final config docs. No active user .env modification.',disposition='adapt_partial')
 if q.startswith('.zhikun/skills/') or '/resources/skills/' in q:
  return mapped('skill-prompts',['transactional-global-skill-switches'],['crates/zk-server/resources/skills','crates/zk-server/src/skill/registry.rs'],note='Bundled/alias/project skill prompts need individual adaptation; preserve fix/stuck aliases and lint actual registered native tool names. Local project skill files are not auto-installed blindly.')
 if '/resources/prompts/' in q:
  return mapped('prompt-parameters',[],['crates/zk-engine/src/system_prompt.rs','crates/zk-tools/src/file_read.rs','crates/zk-tools/src/grep.rs'],note='Source changes correct native tool names and Read/Grep parameters. Must check Rust active prompts and its own offset semantics rather than copy Java examples.')
 if q.endswith('log4j2.xml') or q.endswith('/application.java'):
  return mapped('local-logging',[],['scripts/dev/lifecycle.sh','crates/zk-server/src/main.rs'],note='Rust uses native tracing and dev launcher logs; verify resolved log-directory visibility. Java Log4j config itself is not imported.',disposition='existing_equivalent_review')
 if q.startswith('backend/'):
  # Java tests inherit the capability family only, never an unearned test pass.
  test='/src/test/' in q
  specific={
  'fileedit':['edit-empty-match-and-visible-diff'],'grep':['grep-paging-and-protected-descendants'],'todowrite':['todo-normalization-and-anonymous-merge'],'mcptooladapter':['mcp-truthful-failure-no-success-cache-fallback'],'repl':['repl-session-parameter-and-language-compatibility'],
  'attachment':['attachment-safe-filenames-exact-uuid-atomic-save'],'contentdisposition':['attachment-safe-filenames-exact-uuid-atomic-save'],'evidence':['evidence-preview-and-content-integrity','verifyjourney-dsl-http-owned-preview-and-evidence'],'journey':['verifyjourney-dsl-http-owned-preview-and-evidence'],'browserverifier':['verifyjourney-dsl-http-owned-preview-and-evidence'],'devserverlauncher':['verifyjourney-dsl-http-owned-preview-and-evidence'],'screenshotformat':['verifyjourney-dsl-http-owned-preview-and-evidence'],'stepresult':['verifyjourney-dsl-http-owned-preview-and-evidence'],'webbrowser':['truthful-browser-click-type-and-screenshots'],'pythoncapability':['python-browser-lifecycle-and-disconnect'],'pythonprocess':['python-browser-lifecycle-and-disconnect'],
  'handoffread':['handoff-queries'],'handoffcontext':['reference-authority'],'mergepackage':['snapshot-package'],'mergesummary':['extract-all','extract-schema','summary-tree','provider-costs','budget-resume'],'mergeprogress':['durable-units'],'mergetextbudget':['extract-all'],'mergehandoffdata':['snapshot-package'],'sessionmerge':['merge-coordination','cancellation','reference-authority'],
  'worktree':['worktree-snapshot-delivery','write-child-authority'],'rawgitprocess':['git-hook-process-supervision'],'taskcoordinator':['shell-task-attached-detached','per-wait-cycle-budget'],'taskexecutionresult':['shell-task-attached-detached'],'taskget':['shell-task-attached-detached'],'tasklist':['shell-task-attached-detached'],'taskoutput':['shell-task-attached-detached'],'taskstate':['shell-task-attached-detached'],'taskstop':['shell-task-attached-detached'],'taskupdate':['shell-task-attached-detached'],'taskdetached':['shell-task-attached-detached'],'tasklifecycle':['shell-task-attached-detached'],'tasktoolgolden':['shell-task-attached-detached'],'builtinagent':['write-child-authority'],'modeltier':['providers-models'],'imageidentity':['image-source-budget-provider-routing'],'imageinjection':['image-source-budget-provider-routing'],'userimagerequest':['image-source-budget-provider-routing'],'mandatorycollision':['lossless-mandatory-context-tool-transactions'],'systemmarker':['empty-placeholder-final-single-recovery'],'contextmanagement':['lossless-mandatory-context-tool-transactions'],'officetoolchain':['native-full-document-toolchain'],'taskcreate':['shell-task-attached-detached'],'taskshell':['shell-task-attached-detached'],'backgroundagent':['shell-task-attached-detached','per-wait-cycle-budget'],'subagent':['worktree-snapshot-delivery','write-child-authority'],'agenttool':['shell-task-attached-detached','write-child-authority'],'agentresume':['shell-task-attached-detached'],'agenttimeout':['per-wait-cycle-budget'],
  'inlineimage':['image-source-budget-provider-routing'],'userimagetranscoder':['image-source-budget-provider-routing'],'imagerefinjector':['image-source-budget-provider-routing'],'visionmodel':['image-source-budget-provider-routing'],'tokenbudget':['lossless-mandatory-context-tool-transactions','image-source-budget-provider-routing'],'compact':['independent-summary','lossless-mandatory-context-tool-transactions'],'contextcollapse':['lossless-mandatory-context-tool-transactions'],'contextcascade':['lossless-mandatory-context-tool-transactions'],'collapselevel':['lossless-mandatory-context-tool-transactions'],'messagenormalizer':['lossless-mandatory-context-tool-transactions'],
  'summary':['independent-summary'],'apikey':['multi-key-priority-paid-opt-in'],'multiprovider':['multi-key-priority-paid-opt-in'],'responses':['stateless-responses'],'openrouter':['openrouter-opaque-reasoning','providers-models'],'retry':['retryability-propagation'],'providererror':['retryability-propagation'],'llmapiexception':['retryability-propagation'],'toolcalltracker':['once-hint-final-after-errors'],'terminationstrategy':['once-hint-final-after-errors'],'queryloop':['empty-placeholder-final-single-recovery'],'taskboundary':['task-boundary-assistant-segments'],
  }
  ids=[]
  for key,val in specific.items():
   if key in q:ids.extend(val)
  if '/llm/' in q and not ids:ids=['providers-models']
  if stem=='QueryEngine':ids=['empty-placeholder-final-single-recovery','steering','task-boundary-assistant-segments','once-hint-final-after-errors','lossless-mandatory-context-tool-transactions']
  if ids:
   ids=list(dict.fromkeys(ids)); exact=any(p in cap[i]['paths'] for i in ids if i in cap)
   return mapped('backend-test' if test else 'backend-capability',ids,exact=exact and not test,note=('Source test requires native semantic-equivalent case, not copying JUnit or claiming external provider calls. ' if test else '')+'Source path mapped to capability ledger; referenced local evidence scope and pending refinements remain authoritative.')
  roots=[('ssestream','native-sse-null-safety',['crates/zk-server/src/api/query.rs','crates/zk-server/src/error.rs']),('enterplanmode','retained-plan-mode',['crates/zk-tools/src/plan_mode.rs','crates/zk-engine/src/engine.rs']),('exitplanmode','retained-plan-mode',['crates/zk-tools/src/plan_mode.rs','crates/zk-engine/src/engine.rs']),('shellstate','private-shell-files',['crates/zk-tools/src/bash/shell_state.rs']),('bashtool','shell-cleanup-truth',['crates/zk-tools/src/bash.rs','crates/zk-tools/src/process.rs']),('workspac','native-workspace-boundary',['crates/zk-authz/src/subject.rs','crates/zk-server/src/file_access.rs']),('interactiontool','native-interactions',['crates/zk-tools/src/ask_user_question.rs','crates/zk-tools/src/brief.rs']),('concurrency','native-task-concurrency',['crates/zk-engine/src/task/runtime.rs']),('asr','speech',['crates/zk-server/src/speech.rs','crates/zk-server/src/api/speech.rs']),('skill','skill-management',['crates/zk-server/src/skill/registry.rs','crates/zk-db/src/skill_state.rs']),('memory','memory-cas',['crates/zk-db/src/memory.rs','crates/zk-server/src/api/memory.rs']),('memdir','memory-cas',['crates/zk-db/src/memory.rs']),('permission','session-permission',['crates/zk-authz/src/mode.rs','crates/zk-server/src/authz.rs','crates/zk-server/src/api/session.rs']),('elicitation','interaction-multiselect',['crates/zk-server/src/interaction/elicitation.rs','crates/zk-tools/src/ask_user_question.rs']),('askuserquestion','interaction-multiselect',['crates/zk-tools/src/ask_user_question.rs']),('durableinteraction','interaction-durability',['crates/zk-server/src/interaction/service.rs']),('session','session-persistence-gates',['crates/zk-db/src/session.rs','crates/zk-db/src/message.rs','crates/zk-server/src/api/session.rs']),('run','run-accounting-projection',['crates/zk-db/src/runtime_ledger.rs','crates/zk-server/src/api/workbench.rs','crates/zk-engine/src/engine.rs']),('workbench','workbench-scope',['crates/zk-server/src/api/workbench.rs']),('activity','activity-decisions',['crates/zk-server/src/api/activity.rs']),('websocket','protocol-restore',['crates/zk-server/src/ws/inbound.rs','crates/zk-server/src/ws/restore.rs']),('message','message-metadata',['crates/zk-protocol/src/model.rs','crates/zk-db/src/message.rs']),('filecontroller','safe-local-preview',['crates/zk-server/src/api/file.rs']),('filestate','read-evidence-invalidation',['crates/zk-tools/src/file_state.rs']),('brief','truthful-brief',['crates/zk-tools/src/brief.rs']),('toolsearch','enabled-tool-search',['crates/zk-tools/src/tool_search/tool.rs']),('command','slash-commands',['crates/zk-server/src/command/builtin']),('gitservice','git-output-truth',['crates/zk-tools/src/git.rs','crates/zk-server/src/api/git.rs']),('securityconfig','local-security',['crates/zk-server/src/middleware']),('authorization','native-authority',['crates/zk-authz/src']),('process','process-lifecycle',['crates/zk-tools/src/process.rs','crates/zk-tools/src/git_process.rs']),('coordinator','coordinator-gates',['crates/zk-engine/src/coordinator']),('prompt','system-prompt',['crates/zk-engine/src/system_prompt.rs']),('query','query-lifecycle',['crates/zk-server/src/api/query.rs']),('artifact','artifact-scope',['crates/zk-server/src/api/artifact.rs']),('config','native-config',['crates/zk-server/src/config.rs']),('streamingtool','tool-runtime',['crates/zk-tools/src/executor.rs'])]
  for key,category,targets in roots:
   if key in q:
    category_caps={'native-sse-null-safety':['protocol-and-message-meta','configuration-and-greenfield-schema'],'run-accounting-projection':['run-usage-authority'],'coordinator-gates':['coordinator-notification-and-tool-truth','swarm-background-lifetime-parent-model'],'query-lifecycle':['providers-models','session-search-status-permission'],'message-metadata':['protocol-and-message-meta'],'read-evidence-invalidation':['resume-file-read-evidence'],'session-persistence-gates':['merge-coordination','session-search-status-permission'],'tool-runtime':['durable-multiselect-staged-deadlines','run-termination-persistence-failure-policy'],'process-lifecycle':['shell-task-attached-detached','git-hook-process-supervision'],'private-shell-files':['shell-task-attached-detached'],'shell-cleanup-truth':['shell-task-attached-detached'],'native-interactions':['durable-multiselect-staged-deadlines','brief-truthful-context-description'],'native-task-concurrency':['shell-task-attached-detached'],'artifact-scope':['workbench-evidence-truth']}
    return mapped('backend-test' if test else category,category_caps.get(category,[]),targets,note='Parent/runtime supplemental audit required; implementation target exists but no exact capability/test claim is inferred from filename. Source hunk may mix selected capability with excluded publication/legacy behavior.')
  return mapped('backend-unclassified-review',[],[],note='No precise capability association yet; mandatory production/test source review remains pending.')
 if q in ('.gitignore','start.sh','stop.sh'):
  return mapped('native-development-entry',[],[p,'dev','scripts/dev/main.sh'],note='Preserve local dev lifecycle and scratch ignore behavior; do not import Java/Linux launcher. Verify exact delta. ',disposition='adapt_partial')
 return mapped('unclassified-review',[],[p] if (ROOT/p).exists() else [],note='Mandatory explicit source-path review; not treated as excluded merely because no direct Rust filename exists.')
def classify(p):
 result=classify_path(p)
 if p in test_audits and result['status']!='excluded_by_scope':
  filename,review=test_audits[p]
  verification_notes=review.get('verificationNotes','')
  if isinstance(verification_notes,list):verification_notes=' '.join(str(note) for note in verification_notes)
  result.update(category='backend-test-assertion-audit',status='documented_coverage',disposition=review.get('disposition','reviewed_native_adaptation'),
   verificationStatus='native_case_coverage_documented' if review.get('allSourceAssertionsClaimedCovered',False) else 'partial_native_case_coverage_explicit',
   pathAuditRef=filename+'#/paths/'+p.replace('~','~0').replace('/','~1'),
   targetPaths=[v for v in review.get('nativeImplementation',[]) if (ROOT/re.sub(r':\d+$','',v)).exists()],
   notes='Exact source test-hunk review, not a family inference or source JUnit execution. '+verification_notes,
   allSourceAssertionsClaimedCovered=review.get('allSourceAssertionsClaimedCovered',False))
 return result

commits=[]; touches=collections.defaultdict(list)
for sha in git('rev-list','--reverse',f'{BASE}..{TARGET}').decode().splitlines():
 meta=git('show','-s','--format=%aI%x00%s',sha).decode().rstrip('\n').split('\0',1)
 paths=[p.decode() for p in git('diff-tree','--no-commit-id','--name-only','--no-renames','-r','-z',sha).split(b'\0') if p]
 for p in paths:touches[p].append(sha)
 commits.append({'commit':sha,'date':meta[0],'title':meta[1],'paths':paths})
records={}
for p,shas in sorted(touches.items()):
 x=classify(p);x.update(path=p,sourceCommits=shas,baselineBlob=base_tree.get(p),targetBlob=target_tree.get(p),netChanged=base_tree.get(p)!=target_tree.get(p),sourceFinalState='present' if p in target_tree else 'removed')
 if p not in target_tree and x['status']!='excluded_by_scope':x['notes']+=' Source path is absent at fixed target; review its replacement/superseding change rather than reintroducing it.'
 records[p]=x
for c in commits:
 maps=[]
 for p in c['paths']:
  r=records[p];maps.append({'path':p,'coverageRef':'upstream-path-coverage.json#/paths/'+p.replace('~','~0').replace('/','~1'),'disposition':r['disposition'],'status':r['status'],'capabilityRefs':r['capabilityRefs']})
 raw=[v for v in git('diff-tree','--no-commit-id','--raw','--no-renames','-r','-z',c['commit']).split(b'\0') if v]
 changes={}
 for offset in range(0,len(raw),2):
  parts=raw[offset].decode().split(); path=raw[offset+1].decode();changes[path]={'changeType':parts[4],'sourceOldBlob':None if set(parts[2])=={'0'} else parts[2],'sourceCommitBlob':None if set(parts[3])=={'0'} else parts[3]}
 for item in maps:item.update(changes[item['path']])
 c.update(pathMappings=maps,status='excluded_by_scope' if all(m['status']=='excluded_by_scope' for m in maps) else 'review_pending' if any(m['status']=='review_pending' for m in maps) else 'documented_coverage',capabilities=sorted({ref for m in maps for ref in m['capabilityRefs']}),verification=[{'scope':'per-path','reference':'upstream-path-coverage.json','result':'Source paths mapped individually; documented capability evidence is not a claim that every hunk or live external API passed. Pending rows require review.'}])
summary={'commitCount':len(commits),'pathTouchCount':sum(len(c['paths']) for c in commits),'uniquePaths':len(records),'netChangedUniquePaths':sum(r['netChanged'] for r in records.values()),'statuses':dict(collections.Counter(r['status'] for r in records.values())),'categories':dict(collections.Counter(r['category'] for r in records.values()))}
(ROOT/'docs/migration/upstream-path-coverage.json').write_text(json.dumps({'schemaVersion':1,'generatedAt':datetime.datetime.now(datetime.timezone.utc).isoformat(),'sourceBaseline':BASE,'sourceTarget':TARGET,'policy':'Classification is per actual Git path with core.quotePath=false and NUL-delimited records. Exact path associations cite capability evidence; family matches remain review_pending. Review each source hunk, including paths later removed or overwritten. No pending row claims completion.','summary':summary,'paths':records},ensure_ascii=False,indent=2)+'\n')
p=ROOT/'docs/migration/2026-10-upstream.json';old=json.loads(p.read_text());old.update(schemaVersion=2,sourceBaseline=BASE,sourceTarget=TARGET,summary=summary,coverageFile='upstream-path-coverage.json',commits=commits);p.write_text(json.dumps(old,ensure_ascii=False,indent=2)+'\n')
print(json.dumps(summary,ensure_ascii=False,indent=2))
for p,r in records.items():
 if r['netChanged'] and r['status']=='review_pending' and p.startswith('backend/src/main/') and r['category'] not in ('backend-capability',): print(r['category'],p)
