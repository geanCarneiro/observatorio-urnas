#!/usr/bin/env python3
"""Serve a small local browser UI for collection and reprocessing jobs."""

from __future__ import annotations

import json
import os
import sqlite3
import sys
import threading
import time
import webbrowser
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

TOOLS_ROOT = Path(__file__).resolve().parent
if str(TOOLS_ROOT) not in sys.path:
    sys.path.insert(0, str(TOOLS_ROOT))

from local_jobs import (DATA_ROOT, STATE_ROOT, list_runs, preview_collection, reconcile_interrupted_runs,
                        request_active_run_stop,
                        resume_run, start_batch_processing, start_collection, start_reprocessing)


PAGE = r"""<!doctype html>
<html lang="pt-BR">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Observatório das Urnas — Jobs locais</title>
<style>
:root{font:15px/1.5 system-ui,sans-serif;color:#172033;background:#f3f5f8}*{box-sizing:border-box}
body{margin:0}.wrap{max-width:1060px;margin:auto;padding:24px}.card{background:white;border:1px solid #dce2ea;border-radius:12px;padding:18px;margin:0 0 16px;box-shadow:0 2px 8px #15243a0b}
h1{font-size:1.55rem;margin:0 0 4px}h2{font-size:1.05rem;margin:0 0 12px}.muted{color:#586579}.modes,.actions{display:flex;gap:12px;align-items:center;flex-wrap:wrap}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(145px,1fr));gap:10px}.field label{display:block;font-size:.86rem;color:#44516a;margin-bottom:4px}.field input,.field select{width:100%;padding:9px;border:1px solid #bfc9d6;border-radius:7px;background:white}
button{border:0;border-radius:7px;padding:9px 13px;background:#2459a6;color:white;font-weight:600;cursor:pointer}button.secondary{background:#e6ebf2;color:#172033}button.stop{background:#a83131}button:disabled{opacity:.5;cursor:not-allowed}
.check{display:flex;gap:8px;align-items:center;margin:12px 0}.status{padding:10px;background:#eef3fa;border-radius:7px;margin-top:12px}.bar{height:9px;background:#e0e6ee;border-radius:99px;margin-top:9px;overflow:hidden}.bar i{display:block;height:100%;width:0;background:#3476d1;transition:width .2s}
.preview{margin-top:12px;padding:10px;background:#f5f7fa;border-radius:7px}.progress-meta{display:flex;gap:12px;align-items:baseline;margin-top:8px}.progress-meta strong{font-size:1.05rem;font-variant-numeric:tabular-nums}.progress-meta span{color:#586579}.table-wrap{overflow:auto}table{width:100%;border-collapse:collapse;font-size:.9rem}th,td{text-align:left;padding:8px;border-bottom:1px solid #e4e8ef;white-space:nowrap}pre{max-height:230px;overflow:auto;background:#111827;color:#dce7f7;padding:12px;border-radius:8px;white-space:pre-wrap}
.job-list{display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:9px}.job{box-sizing:border-box;height:112px;min-width:0;padding:10px;border:1px solid #e2e7ee;border-radius:8px;overflow:hidden}.job.idle{background:#f8fafc;color:#718096}.job-head{display:flex;justify-content:space-between;gap:10px;min-width:0}.job-head strong{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.job .bar{margin-top:6px}.job .bar i{background:#25836a}.job .muted{height:2.6em;overflow:hidden;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical}.job.idle .bar i{background:#cbd5e1}.queue-list{margin:8px 0 0;padding-left:22px;columns:2}.queue-list li{break-inside:avoid;margin:2px 0}.job-empty{padding:8px;color:#586579}@media(max-width:1100px){.job-list{grid-template-columns:repeat(3,minmax(0,1fr))}}@media(max-width:650px){.job-list{grid-template-columns:repeat(2,minmax(0,1fr))}}
dialog{width:min(1000px,94vw);max-height:90vh;border:0;border-radius:12px;padding:20px;box-shadow:0 12px 50px #11182755}dialog::backdrop{background:#10182899}.detail-grid{display:grid;grid-template-columns:1fr 1fr;gap:12px}.detail-grid pre{max-height:260px;margin:0}.detail-table{max-height:320px;overflow:auto}#runRows td{white-space:normal;max-width:340px;overflow-wrap:anywhere}.connection{float:right;color:#586579;font-size:.85rem}
</style></head><body><main class="wrap">
<header class="card"><span id="connectionState" class="connection">Conectando atualizações…</span><h1>Observatório das Urnas</h1><div class="muted">Jobs locais · dados em <code>dados/</code> · estado e retomada em <code>.estado-local/</code></div></header>
<section class="card"><h2>Operação</h2><div class="modes"><label><input type="radio" name="mode" value="collect" checked> Coletar dados</label><label><input type="radio" name="mode" value="batch_process"> Consolidar por remessas</label><label><input type="radio" name="mode" value="reprocess"> Reprocessar dados locais</label></div>
<div id="collect"><h2 style="margin-top:18px">Abrangência da coleta</h2><div class="grid">
<div class="field"><label for="turn">Turno</label><select id="turn"><option value="2">2</option><option value="1">1</option><option value="ambos">Ambos</option></select></div>
<div class="field"><label for="uf">UF</label><input id="uf" placeholder="ex.: es"></div><div class="field"><label for="municipio">Município</label><input id="municipio" placeholder="código TSE"></div>
<div class="field"><label for="zona">Zona</label><input id="zona" placeholder="ex.: 0054"></div><div class="field"><label for="secao">Seção</label><input id="secao" placeholder="ex.: 0509"></div>
<div class="field"><label for="offset">Offset</label><input id="offset" type="number" min="0" value="0"></div><div class="field"><label for="limit">Máximo de seções</label><input id="limit" type="number" min="1" placeholder="sem limite"></div>
</div><label class="check"><input id="refresh" type="checkbox"> Atualizar os dados brutos em vez de usar o modo incremental</label>
<div class="muted">Filtros vazios ampliam a seleção. Confira a prévia antes de iniciar.</div><div class="grid" style="margin-top:12px"><div class="field"><label id="workersLabel" for="workers">Workers simultâneos</label><input id="workers" type="number" min="1" value="15"><small id="workersHint" class="muted">Padrão: 15 · sem teto fixo</small></div></div><div id="batchOptions" class="grid" style="display:none;margin-top:12px"><div class="field"><label for="batchSize">Seções por remessa</label><input id="batchSize" type="number" min="1" max="10000" value="500"></div></div><div id="batchCleanupNote" class="status" style="display:none">Ao iniciar, todo o conteúdo de <code>dados/</code> será removido. Ao final de cada remessa, os brutos serão limpos depois de salvar o CSV e preservar evidências de anomalias ou processamento incompleto.</div><div class="actions" style="margin-top:12px"><button id="preview">Pré-visualizar seleção</button><button id="start" disabled>Iniciar coleta</button></div><div id="previewResult" class="preview">Prévia ainda não calculada.</div></div>
<div class="actions" style="margin-top:16px"><button id="reprocess" class="secondary">Iniciar reprocessamento local</button><button id="stop" class="stop" disabled>Parar após a unidade atual</button></div>
<div id="status" class="status">Pronto.</div><div class="progress-meta"><strong id="progressValue">—</strong><span id="progressDetail">Nenhuma execução em andamento.</span></div><div class="bar"><i id="bar"></i></div></section>
 <section class="card"><h2>Workers de coleta e extração</h2><div id="workerSummary" class="muted">Nenhum worker em andamento.</div><div id="activeJobs" class="job-list" style="margin-top:10px"></div><details style="margin-top:12px"><summary id="queueSummary">Fila de espera</summary><ol id="queuedJobs" class="queue-list"></ol></details></section>
<section class="card"><div class="actions" style="justify-content:space-between"><h2>Execuções locais</h2><button id="refreshRuns" class="secondary">Atualizar lista</button></div><div class="table-wrap"><table><thead><tr><th>Tipo</th><th>Estado</th><th>Início UTC</th><th>ID da execução</th><th></th></tr></thead><tbody id="runs"></tbody></table></div></section>
<section class="card"><h2>Atividade recente</h2><pre id="log">Aguardando atividade.</pre></section></main>
<dialog id="runDetails"><div class="actions" style="justify-content:space-between"><h2>Detalhes da execução <span id="detailRunTitle"></span></h2><button id="closeDetails" class="secondary">Fechar</button></div><div class="actions" style="margin-bottom:12px"><button id="copyParams" class="secondary">Copiar parâmetros</button><button id="repeatRun">Nova execução com estes parâmetros</button></div><div class="detail-grid"><section><h3>Execução e parâmetros</h3><pre id="runMetadata"></pre></section><section><h3>Plano completo</h3><pre id="runPlan"></pre></section></div><h3>Contagens por etapa</h3><pre id="runCounts"></pre><h3>Seções da execução</h3><div id="sectionPageInfo" class="muted"></div><div class="detail-table"><table><thead><tr><th>Turno</th><th>Seção</th><th>Coleta bruta</th><th>Extração</th><th>Aquisição</th><th>Erro</th></tr></thead><tbody id="runRows"></tbody></table></div><div class="actions" style="margin-top:10px"><button id="prevSectionPage" class="secondary">Anterior</button><button id="nextSectionPage" class="secondary">Próxima</button></div><h3>Eventos recentes</h3><pre id="runEvents"></pre></dialog>
<script>
const $=id=>document.getElementById(id);let preview=null,cursor=0,busy=false,canStop=false,attachedRunSeen=null,source=null,jobState={worker_limit:15,active_jobs:[],waiting_count:0,waiting_jobs:[]},workerSlots={},runDetails=null,detailOffset=0,pendingRepeat=null;
const mode=()=>document.querySelector('input[name=mode]:checked').value;
const number=n=>Number(n||0).toLocaleString('pt-BR');
function progressDescription(s){const d=s.progress_detail;if(!d)return s.attached_run?(s.attached_run.cross_process_stop_supported?`Acompanhando ${s.attached_run.run_id}; parada segura disponível.`:`Acompanhando ${s.attached_run.run_id}; para parar, use a UI que iniciou o job.`):(s.busy?'Preparando execução…':'Nenhuma execução em andamento.');if(d.raw_total!=null)return `Coleta bruta ${number(d.raw_processed)} de ${number(d.raw_total)} · extrações concluídas ${number(d.extraction_processed)}.`;if(d.total!=null)return `${d.stage||'Etapa'}: ${number(d.processed)} de ${number(d.total)}.`;return d.message||d.stage||'Progresso em andamento.'}
function requestBody(){return {turn:$('turn').value,uf:$('uf').value.trim(),municipio:$('municipio').value.trim(),zona:$('zona').value.trim(),secao:$('secao').value.trim(),offset:Number($('offset').value||0),limit:$('limit').value?Number($('limit').value):null,workers:Number($('workers').value||15)}}
async function post(path,data={}){const r=await fetch(path,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(data)});const x=await r.json();if(!r.ok)throw Error(x.error||r.statusText);return x}
function updateControls(){const selectedMode=mode(),collecting=selectedMode==='collect'||selectedMode==='batch_process',batch=selectedMode==='batch_process';$('collect').style.display=collecting?'block':'none';$('batchOptions').style.display=batch?'grid':'none';$('batchCleanupNote').style.display=batch?'block':'none';$('refresh').closest('.check').style.display=batch?'none':'flex';$('workersLabel').textContent=batch?'Workers simultâneos (fila completa)':'Workers de seção (coleta + extração)';$('workersHint').textContent=batch?'Padrão: 15 · sem teto fixo. Cada worker coleta e analisa; workers podem estar em etapas diferentes.':'Padrão: 15 · sem teto fixo';$('preview').disabled=busy||!collecting;$('start').disabled=busy||!collecting||!preview;$('start').textContent=batch?'Iniciar processamento por remessas':'Iniciar coleta';$('reprocess').disabled=busy||collecting;$('stop').disabled=!canStop;$('stop').style.display=canStop?'inline-block':'none';$('stop').textContent=attachedRunSeen?'Solicitar parada segura':'Parar após a unidade atual'}
function append(message){const el=$('log');if(el.textContent==='Aguardando atividade.')el.textContent='';el.textContent+=message+'\n';el.scrollTop=el.scrollHeight}
function eventMessage(e){if(e.type==='status'||e.type==='catalog_progress')return e.message;if(e.type==='batch_started')return `Remessa ${e.batch_id}: ${number(e.section_count)} seções`;if(e.type==='batch_finished')return `Remessa ${e.batch_id} consolidada por estado: ${(e.csv_paths||[]).map(x=>`${x.uf} (${number(x.row_count)} linhas)`).join(', ')}`;if(e.type==='section_started')return `Iniciando turno ${e.turn}: ${e.section}`;if(e.type==='section_complete'||e.type==='reprocessing_complete'||e.type==='extraction_complete')return `${e.stage==='batch_pipeline'?'Coleta/processamento':e.stage||'extração'}: ${e.section||''} — ${e.status}`;if(e.type==='run_started')return `Job ${e.run_id} iniciado (${e.kind})`;if(e.type==='run_finished')return e.status==='paused'?`Execução ${e.run_id} pausada. O progresso foi salvo para retomada.`:e.status==='complete'?`Execução ${e.run_id} concluída.`:`Execução ${e.run_id} terminou: ${e.status}.`;if(e.type==='run_failed'||e.type==='worker_error'||e.type==='preview_failed')return e.error;if(e.type==='preview_complete')return 'Inventário oficial consultado.';return null}
function setGlobalProgress(progress,detail){const pct=progress===null||progress===undefined?null:Number(progress);$('bar').style.width=pct===null?'0%':`${pct}%`;$('progressValue').textContent=pct===null?'—':`${new Intl.NumberFormat('pt-BR',{minimumFractionDigits:2,maximumFractionDigits:2}).format(pct)}%`;$('progressDetail').textContent=progressDescription({progress_detail:detail,busy,attached_run:attachedRunSeen?{run_id:attachedRunSeen}:null})}
function renderJobs(j){jobState=j||jobState;const active=jobState.active_jobs||[],waiting=jobState.waiting_jobs||[],count=jobState.waiting_count||0,limit=jobState.worker_limit||15,slots=Array.from({length:limit},()=>null),activeIds=new Set(active.map(x=>x.job_id));for(const id of Object.keys(workerSlots))if(!activeIds.has(id))delete workerSlots[id];for(const x of active){let index=workerSlots[x.job_id];if(index===undefined||index>=limit||slots[index]){index=slots.findIndex(slot=>slot===null);if(index<0)continue;workerSlots[x.job_id]=index}slots[index]=x}$('workerSummary').textContent=active.length?`${active.length}/${limit} ativos · ${number(count)} aguardando`:`${limit} posições de worker · todos parados${count?` · ${number(count)} aguardando`:''}`;$('activeJobs').replaceChildren(...slots.map((x,index)=>{const box=document.createElement('div');box.className=`job${x?'':' idle'}`;const head=document.createElement('div');head.className='job-head';const label=document.createElement('strong');label.textContent=x?`${x.section||x.job_id}${x.turn?` · turno ${x.turn}`:''}`:`Worker ${String(index+1).padStart(2,'0')}`;label.title=label.textContent;const pct=document.createElement('span');pct.textContent=x?`${new Intl.NumberFormat('pt-BR',{maximumFractionDigits:0}).format(Number(x.progress||0))}%`:'—';head.append(label,pct);const detail=document.createElement('div');detail.className='muted';detail.textContent=x?(x.file?`${x.stage||'Processando'} · ${x.file}`:(x.stage||'Processando')):'Parado';detail.title=detail.textContent;const bar=document.createElement('div');bar.className='bar';const fill=document.createElement('i');fill.style.width=x?`${Math.min(100,Math.max(0,Number(x.progress||0)))}%`:'0%';bar.append(fill);box.append(head,detail,bar);return box}));$('queueSummary').textContent=`Fila de espera: ${number(count)} seção(ões)${count>waiting.length?` · exibindo próximas ${waiting.length}`:''}`;$('queuedJobs').replaceChildren(...waiting.map(x=>{const li=document.createElement('li');li.textContent=`${x.section||x.job_id}${x.turn?` · turno ${x.turn}`:''}`;return li}))}
function applySnapshot(s,showRecent=false){busy=!!s.busy;canStop=!!s.can_stop;cursor=Math.max(cursor,Number(s.cursor||0));attachedRunSeen=s.attached_run?.run_id||null;jobState=s.jobs||jobState;setGlobalProgress(s.progress,s.progress_detail);renderJobs(jobState);renderRuns(s.runs||[]);updateControls();if(showRecent){$('log').textContent='';for(const e of s.recent_events||[]){const m=eventMessage(e);if(m)append(m)}}}
function handleUpdate(e){cursor=Math.max(cursor,Number(e.cursor||0));if(e.ui_progress!==undefined)setGlobalProgress(e.ui_progress,e.ui_progress_detail);if(e.type==='catalog_progress')setGlobalProgress(e.progress,{stage:`Catálogos ${e.config_id}`,processed:e.completed,total:e.total});const m=eventMessage(e);if(m){$('status').textContent=m;append(m)}if(e.type==='job_queue'){jobState={worker_limit:e.worker_limit,active_jobs:e.active_jobs||[],waiting_count:e.waiting_count||0,waiting_jobs:e.waiting_jobs||[]};renderJobs(jobState)}else if(e.type==='job_started'){jobState.active_jobs=[...(jobState.active_jobs||[]).filter(x=>x.job_id!==e.job_id),e];renderJobs(jobState)}else if(e.type==='job_progress'){jobState.active_jobs=(jobState.active_jobs||[]).map(x=>x.job_id===e.job_id?{...x,...e}:x);renderJobs(jobState)}else if(e.type==='job_finished'){jobState.active_jobs=(jobState.active_jobs||[]).filter(x=>x.job_id!==e.job_id);renderJobs(jobState)}if(e.type==='preview_ready'){preview=e.result;$('previewResult').textContent=e.result.turns.map(x=>`Turno ${x.turn}: ${x.selected_sections} selecionadas, ${x.collectable_sections} coletáveis, ${x.aggregated_sections} agregadas`).join(' · ')+` · Total: ${e.result.total_selected_sections}`;updateControls()}if(e.type==='preview_failed'){$('previewResult').textContent=e.error;preview=null;pendingRepeat=null;updateControls()}if(e.type==='run_started'){busy=true;canStop=true;updateControls();refreshSnapshot()}if(e.type==='worker_done'){busy=false;canStop=false;jobState={...jobState,active_jobs:[],waiting_count:0,waiting_jobs:[]};renderJobs(jobState);updateControls();refreshSnapshot();if(pendingRepeat&&preview)setTimeout(beginRepeatedCollection,0)}}
function connectEvents(){source=new EventSource('/api/events');source.addEventListener('snapshot',e=>{try{const s=JSON.parse(e.data);applySnapshot(s,true);for(const item of s.recent_events||[])cursor=Math.max(cursor,Number(item.cursor||0))}catch(error){showError(error)}});source.addEventListener('state',e=>{try{applySnapshot(JSON.parse(e.data),false)}catch(error){showError(error)}});source.onmessage=e=>{try{handleUpdate(JSON.parse(e.data))}catch(error){showError(error)}};source.onopen=()=>{$('connectionState').textContent='Atualizações em tempo real conectadas'};source.onerror=()=>{$('connectionState').textContent='Reconectando atualizações…'}}
async function refreshSnapshot(){try{const r=await fetch('/api/state');const s=await r.json();if(!r.ok)throw Error(s.error||r.statusText);applySnapshot(s,false)}catch(e){showError(e)}}
function renderRuns(runs){$('runs').replaceChildren(...runs.map(run=>{const tr=document.createElement('tr');for(const value of [run.kind,run.status,run.created_at_utc,run.run_id]){const td=document.createElement('td');td.textContent=value||'';tr.append(td)}const td=document.createElement('td');const details=document.createElement('button');details.className='secondary';details.textContent='Detalhes';details.onclick=()=>loadRunDetails(run.run_id);td.append(details);if(['paused','partial','failed'].includes(run.status)){const b=document.createElement('button');b.className='secondary';b.textContent='Retomar';b.style.marginLeft='6px';b.onclick=async()=>{busy=true;canStop=true;updateControls();try{await post('/api/resume',{run_id:run.run_id});await refreshSnapshot()}catch(e){busy=false;canStop=false;updateControls();showError(e)}};td.append(b)}tr.append(td);return tr}))}
async function loadRunDetails(runId,offset=0){try{const r=await fetch(`/api/runs/${encodeURIComponent(runId)}?offset=${offset}&limit=50`);const d=await r.json();if(!r.ok)throw Error(d.error||r.statusText);runDetails=d;detailOffset=offset;$('detailRunTitle').textContent=runId;$('runMetadata').textContent=JSON.stringify(d.run,null,2);$('runPlan').textContent=JSON.stringify(d.plan,null,2);$('runCounts').textContent=JSON.stringify({section_states:d.state_counts,batches:d.batch_states||[]},null,2);$('runEvents').textContent=JSON.stringify(d.recent_events,null,2);$('sectionPageInfo').textContent=`Exibindo ${d.section_jobs.items.length?number(offset+1):'0'}–${number(offset+d.section_jobs.items.length)} de ${number(d.section_jobs.total)} registros`;const rows=$('runRows');rows.replaceChildren(...d.section_jobs.items.map(item=>{const tr=document.createElement('tr');for(const value of [item.turn,item.section_key,item.raw_status,item.extract_status,item.acquisition_path||'',item.error||'']){const td=document.createElement('td');td.textContent=value??'';tr.append(td)}return tr}));$('prevSectionPage').disabled=offset===0;$('nextSectionPage').disabled=offset+d.section_jobs.items.length>=d.section_jobs.total;if(!$('runDetails').open)$('runDetails').showModal()}catch(e){showError(e)}}
async function copyRunParameters(){if(!runDetails)return;const payload={kind:runDetails.run.kind,data_root:runDetails.run.data_root,request:runDetails.run.request};try{await navigator.clipboard.writeText(JSON.stringify(payload,null,2));$('status').textContent='Parâmetros copiados.'}catch(e){showError(new Error('Não foi possível copiar; confira se a página está em localhost e permita acesso à área de transferência.'))}}
async function repeatSelectedRun(){if(!runDetails)return;const run=runDetails.run;if(run.kind==='collect'||run.kind==='batch_process'){const req=run.request||{},batch=run.kind==='batch_process';document.querySelector(`input[name=mode][value=${batch?'batch_process':'collect'}]`).checked=true;$('turn').value=(req.turns||[]).length>1?'ambos':(req.turns||['2'])[0];for(const key of ['uf','municipio','zona','secao'])$(key).value=req[key]||'';$('offset').value=String(req.offset||0);$('limit').value=req.limit==null?'':String(req.limit);$('refresh').checked=req.incremental===false;$('workers').value=String(req.workers||15);if(batch)$('batchSize').value=String(req.sections_per_batch||500);preview=null;pendingRepeat={incremental:req.incremental!==false,run_id:run.run_id,batch,sections_per_batch:req.sections_per_batch,workers:req.workers};$('runDetails').close();updateControls();try{await post('/api/preview',requestBody());busy=true;updateControls()}catch(e){pendingRepeat=null;showError(e)}}else if(run.kind==='reprocess'){if(!confirm(`Iniciar novo reprocessamento com os mesmos dados de ${run.data_root||'dados locais'}?`))return;try{await post('/api/reprocess',{data_root:run.data_root});$('runDetails').close();busy=true;updateControls()}catch(e){showError(e)}}else{showError(new Error('Este tipo de execução não pode ser repetido pela UI.'))}}
async function beginRepeatedCollection(){const pending=pendingRepeat;pendingRepeat=null;if(!pending||!preview)return;if(pending.batch){if(!confirm(`Iniciar novo processamento por remessas com os parâmetros de ${pending.run_id}? A prévia selecionou ${preview.total_selected_sections} seções. Todo o conteúdo de dados/ será removido antes de começar.`))return;try{await post('/api/batch-process',{...requestBody(),preview_dir:preview.preview_dir,sections_per_batch:Number($('batchSize').value||pending.sections_per_batch||500),confirm_clean_data:true});busy=true;updateControls()}catch(e){showError(e)}return}if(!confirm(`Criar uma nova coleta com os mesmos parâmetros do job ${pending.run_id}? A prévia atual selecionou ${preview.total_selected_sections} seções. Nenhum arquivo de dados/ será removido.`))return;try{await post('/api/collect',{...requestBody(),preview_dir:preview.preview_dir,incremental:pending.incremental});busy=true;updateControls()}catch(e){showError(e)}}
function showError(e){$('status').textContent=e.message;append(`ERRO: ${e.message}`)}
document.querySelectorAll('input[name=mode]').forEach(x=>x.onchange=()=>{preview=null;updateControls()});
['turn','uf','municipio','zona','secao','offset','limit'].forEach(id=>$(id).oninput=()=>{preview=null;$('previewResult').textContent='A seleção mudou. Gere uma nova prévia.';updateControls()});
$('preview').onclick=async()=>{try{preview=null;updateControls();await post('/api/preview',requestBody());busy=true;updateControls()}catch(e){showError(e)}};
$('start').onclick=async()=>{if(!preview)return;if(mode()==='batch_process'){if(!confirm(`Processar ${preview.total_selected_sections} seções/turnos em remessas de ${$('batchSize').value} posições? Todo o conteúdo de dados/ será removido agora e os brutos serão limpos após a consolidação de cada remessa.`))return;try{await post('/api/batch-process',{...requestBody(),preview_dir:preview.preview_dir,sections_per_batch:Number($('batchSize').value),confirm_clean_data:true});busy=true;updateControls()}catch(e){showError(e)}return}if(!confirm(`Coletar ${preview.total_selected_sections} seções em modo ${$('refresh').checked?'atualização bruta':'incremental'}? Nenhum arquivo de dados/ será removido.`))return;try{await post('/api/collect',{...requestBody(),preview_dir:preview.preview_dir,incremental:!$('refresh').checked});busy=true;updateControls()}catch(e){showError(e)}};
$('reprocess').onclick=async()=>{if(!confirm('Reprocessar as aquisições locais sem baixar arquivos brutos?'))return;try{await post('/api/reprocess');busy=true;updateControls()}catch(e){showError(e)}};
$('stop').onclick=()=>post('/api/stop').then(x=>{$('status').textContent=`Parada segura solicitada para ${x.run_id}; as unidades ativas vão terminar primeiro.`}).catch(showError);$('refreshRuns').onclick=refreshSnapshot;$('closeDetails').onclick=()=>$('runDetails').close();$('copyParams').onclick=copyRunParameters;$('repeatRun').onclick=repeatSelectedRun;$('prevSectionPage').onclick=()=>loadRunDetails(runDetails.run.run_id,Math.max(0,detailOffset-50));$('nextSectionPage').onclick=()=>loadRunDetails(runDetails.run.run_id,detailOffset+50);connectEvents();updateControls();
</script></body></html>"""


class JobServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address):
        super().__init__(address, JobHandler)
        self.lock = threading.Lock()
        self.events_condition = threading.Condition(self.lock)
        self.events = deque(maxlen=1000)
        self.cursor = 0
        self.busy = False
        self.progress = None
        self.progress_detail = None
        self.current_run_id = None
        self.attached_run_id = None
        self.stop_event = None
        self.preview_result = None
        self.external_progress_cache = (0.0, None)
        self.active_jobs = {}
        self.waiting_jobs = []
        self.waiting_count = 0
        self.worker_limit = 15

    def emit(self, event: dict) -> None:
        with self.events_condition:
            self.cursor += 1
            self.events.append({**event, "cursor": self.cursor})
            if event.get("type") == "run_started":
                self.active_jobs.clear()
                self.waiting_jobs = []
                self.waiting_count = 0
            elif event.get("type") == "job_queue":
                self.worker_limit = int(event.get("worker_limit") or self.worker_limit)
                self.waiting_count = int(event.get("waiting_count") or 0)
                self.waiting_jobs = list(event.get("waiting_jobs") or [])
                self.active_jobs = {item["job_id"]: item for item in event.get("active_jobs", []) if item.get("job_id")}
            elif event.get("type") == "job_started" and event.get("job_id"):
                self.active_jobs[event["job_id"]] = dict(event)
            elif event.get("type") == "job_progress" and event.get("job_id"):
                self.active_jobs[event["job_id"]] = {**self.active_jobs.get(event["job_id"], {}), **event}
            elif event.get("type") == "job_finished":
                self.active_jobs.pop(event.get("job_id"), None)
            elif event.get("type") == "worker_done":
                self.active_jobs.clear()
                self.waiting_jobs = []
                self.waiting_count = 0
            if event.get("type") == "run_started":
                self.current_run_id = event.get("run_id")
                self.progress = 0.0
                self.progress_detail = {"stage": "Preparando execução"}
            is_worker_event = event.get("type") in {"job_queue", "job_started", "job_progress", "job_finished"}
            processed = None if is_worker_event else event.get("progress_processed", event.get("processed"))
            total = None if is_worker_event else event.get("progress_total", event.get("total"))
            if processed is not None and total:
                self.progress = min(100.0, round(processed * 100 / total, 2))
                self.progress_detail = {
                    "stage": event.get("stage", "execução"),
                    "processed": processed,
                    "total": total,
                    "raw_processed": event.get("raw_processed"),
                    "raw_total": event.get("raw_total"),
                    "extraction_processed": event.get("extraction_processed"),
                    "extraction_total": event.get("extraction_total"),
                    "section": event.get("section"),
                    "turn": event.get("turn"),
                }
            if event.get("type") == "run_finished":
                self.current_run_id = event.get("run_id", self.current_run_id)
                if event.get("status") == "complete":
                    self.progress = 100.0
                    self.progress_detail = {**(self.progress_detail or {}), "stage": "Concluído"}
            self.events[-1]["ui_progress"] = self.progress
            self.events[-1]["ui_progress_detail"] = self.progress_detail
            self.events_condition.notify_all()

    def launch(self, operation: str, payload: dict) -> None:
        with self.lock:
            if self.busy:
                raise RuntimeError("já existe uma operação em andamento")
            reconcile_interrupted_runs()
            active_run = next((run for run in list_runs() if run.get("status") in {"preparing", "running"}), None)
            if active_run:
                raise RuntimeError(f"a execução {active_run.get('run_id')} já está ativa; esta UI está em modo de acompanhamento")
            self.busy = True
            self.progress = None
            self.progress_detail = None
            self.current_run_id = None
            self.attached_run_id = None
            self.external_progress_cache = (0.0, None)
            self.stop_event = threading.Event()
            stop_event = self.stop_event

        def work() -> None:
            try:
                if operation == "preview":
                    turns = ["1", "2"] if payload["turn"] == "ambos" else [payload["turn"]]
                    result = preview_collection(turns=turns, uf=payload["uf"], municipio=payload["municipio"],
                        zona=payload["zona"], secao=payload["secao"], offset=payload["offset"], limit=payload["limit"],
                        progress_callback=self.emit)
                    self.preview_result = result
                    self.emit({"type": "preview_ready", "result": result})
                elif operation == "collect":
                    result = start_collection(turns=payload["turns"], uf=payload["uf"], municipio=payload["municipio"],
                        zona=payload["zona"], secao=payload["secao"], offset=payload["offset"], limit=payload["limit"],
                        workers=payload["workers"],
                        incremental=payload["incremental"], preview_dir=payload["preview_dir"],
                        progress_callback=self.emit, stop_event=stop_event)
                elif operation == "batch-process":
                    result = start_batch_processing(turns=payload["turns"], uf=payload["uf"], municipio=payload["municipio"],
                        zona=payload["zona"], secao=payload["secao"], offset=payload["offset"], limit=payload["limit"],
                        sections_per_batch=payload["sections_per_batch"], workers=payload["workers"],
                        preview_dir=payload["preview_dir"], confirm_clean_data=payload["confirm_clean_data"],
                        progress_callback=self.emit, stop_event=stop_event)
                    self.emit({"type": "worker_result", "result": result})
                elif operation == "reprocess":
                    result = start_reprocessing(data_root=payload.get("data_root", DATA_ROOT),
                                               progress_callback=self.emit, stop_event=stop_event)
                    self.emit({"type": "worker_result", "result": result})
                else:
                    result = resume_run(payload["run_id"], progress_callback=self.emit, stop_event=stop_event)
                    self.emit({"type": "worker_result", "result": result})
            except Exception as error:
                event_type = "preview_failed" if operation == "preview" else "worker_error"
                self.emit({"type": event_type, "error": f"{type(error).__name__}: {error}"})
            finally:
                with self.lock:
                    self.busy = False
                    self.stop_event = None
                self.emit({"type": "worker_done"})

        threading.Thread(target=work, name=f"observatorio-{operation}", daemon=True).start()


def _persisted_run_progress(server: JobServer, run: dict) -> tuple[float, dict]:
    run_id = run.get("run_id")
    now = time.monotonic()
    with server.lock:
        cached_at, cached = server.external_progress_cache
        if cached and cached[0] == run_id and now - cached_at < 1.0:
            return cached[1], cached[2]

    total = int(run.get("total_sections") or 0)
    raw_done = extraction_done = extraction_total = 0
    db_path = STATE_ROOT / "runs" / str(run_id) / "state.sqlite3"
    try:
        if db_path.is_file():
            connection = sqlite3.connect(f"file:{db_path.as_posix()}?mode=ro", uri=True, timeout=2)
            try:
                counts = connection.execute("""SELECT raw_status,extract_status,COUNT(*)
                    FROM section_jobs GROUP BY raw_status,extract_status""").fetchall()
            finally:
                connection.close()
            if run.get("kind") == "reprocess":
                extraction_done = sum(n for _raw, extraction, n in counts if extraction == "complete")
                extraction_total = total
                percent = round(extraction_done * 100 / total, 2) if total else 0.0
                detail = {"stage": "reprocessamento", "processed": extraction_done, "total": extraction_total}
            else:
                raw_complete = {"collected", "skipped_existing", "aggregated", "partial", "failed"}
                no_extraction = {"skipped_existing", "aggregated", "failed"}
                raw_done = sum(n for raw, _extraction, n in counts if raw in raw_complete)
                no_extraction_done = sum(n for raw, _extraction, n in counts if raw in no_extraction)
                extraction_done = sum(n for raw, extraction, n in counts
                                      if raw in {"collected", "partial"} and extraction in {"complete", "failed", "not_needed"})
                completed_units = raw_done + no_extraction_done + extraction_done
                work_total = total * 2
                percent = round(completed_units * 100 / work_total, 2) if work_total else 0.0
                stage = "collection" if raw_done < total else "extraction"
                detail = {"stage": stage, "processed": completed_units, "total": work_total,
                          "raw_processed": raw_done, "raw_total": total,
                          "extraction_processed": extraction_done, "extraction_total": total}
    except (OSError, sqlite3.Error, ValueError):
        percent = 0.0
        detail = {"stage": "Aguardando atualização do estado", "processed": 0, "total": total}

    with server.lock:
        server.external_progress_cache = (now, (run_id, percent, detail))
    return percent, detail


def _tail_jsonl(path: Path, limit: int = 100) -> list[dict]:
    if not path.is_file() or limit < 1:
        return []
    with path.open("rb") as source:
        source.seek(0, os.SEEK_END)
        position = source.tell()
        buffer = b""
        while position > 0 and buffer.count(b"\n") <= limit:
            size = min(8192, position)
            position -= size
            source.seek(position)
            buffer = source.read(size) + buffer
    result = []
    for line in buffer.splitlines()[-limit:]:
        try:
            result.append(json.loads(line.decode("utf-8")))
        except (UnicodeDecodeError, json.JSONDecodeError):
            continue
    return result


def _run_details(run_id: str, offset: int = 0, limit: int = 50) -> dict:
    if not run_id or Path(run_id).name != run_id or "/" in run_id or "\\" in run_id:
        raise ValueError("run_id inválido")
    runs_root = (STATE_ROOT / "runs").resolve()
    run_dir = (runs_root / run_id).resolve()
    if run_dir.parent != runs_root:
        raise ValueError("run_id inválido")
    run_path = run_dir / "run.json"
    if not run_path.is_file():
        raise FileNotFoundError("execução local não encontrada")
    run = json.loads(run_path.read_text(encoding="utf-8"))
    plan_path = run_dir / "remessas-plan.json" if run.get("kind") == "batch_process" else run_dir / "coleta-plan.json"
    plan = json.loads(plan_path.read_text(encoding="utf-8")) if plan_path.is_file() else None
    db_path = run_dir / "state.sqlite3"
    state_counts = []
    section_jobs = {"offset": offset, "limit": limit, "total": 0, "items": []}
    if db_path.is_file():
        connection = sqlite3.connect(f"file:{db_path.as_posix()}?mode=ro", uri=True, timeout=2)
        connection.row_factory = sqlite3.Row
        try:
            state_counts = [dict(row) for row in connection.execute("""SELECT raw_status,extract_status,COUNT(*) AS count
                FROM section_jobs GROUP BY raw_status,extract_status ORDER BY raw_status,extract_status""")]
            total = connection.execute("SELECT COUNT(*) FROM section_jobs").fetchone()[0]
            rows = connection.execute("""SELECT turn,section_key,raw_status,extract_status,acquisition_path,error
                FROM section_jobs ORDER BY turn,section_key LIMIT ? OFFSET ?""", (limit, offset)).fetchall()
            section_jobs = {"offset": offset, "limit": limit, "total": total,
                            "items": [dict(row) for row in rows]}
            batch_states = ([dict(row) for row in connection.execute("SELECT * FROM processing_batches ORDER BY batch_index")]
                            if run.get("kind") == "batch_process" else [])
        finally:
            connection.close()
    else:
        batch_states = []
    return {"run": run, "plan": plan, "batch_states": batch_states, "state_counts": state_counts,
            "section_jobs": section_jobs, "recent_events": _tail_jsonl(run_dir / "events.jsonl")}


class JobHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server: JobServer

    def log_message(self, _format, *_args):
        return

    def _json(self, status: int, payload: dict) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _state_snapshot(self, since: int = 0) -> dict:
        reconcile_interrupted_runs()
        runs = list_runs()
        active_run = next((run for run in runs if run.get("status") in {"preparing", "running"}), None)
        with self.server.lock:
            events = [event for event in self.server.events if event["cursor"] > since]
            recent_events = list(self.server.events)[-50:]
            state = {"busy": self.server.busy,
                     "can_stop": self.server.busy and self.server.stop_event is not None,
                     "events": events, "recent_events": recent_events, "cursor": self.server.cursor,
                     "progress": self.server.progress, "progress_detail": self.server.progress_detail,
                     "jobs": {"worker_limit": self.server.worker_limit,
                              "active_jobs": list(self.server.active_jobs.values()),
                              "waiting_count": self.server.waiting_count,
                              "waiting_jobs": list(self.server.waiting_jobs)}}
            current_run_id = self.server.current_run_id
            attached_run_id = self.server.attached_run_id
        if not state["busy"] and active_run and active_run.get("run_id") != current_run_id:
            progress, detail = _persisted_run_progress(self.server, active_run)
            state.update({"busy": True, "can_stop": bool(active_run.get("cross_process_stop_supported")),
                          "attached_run": {"run_id": active_run.get("run_id"),
                                           "kind": active_run.get("kind"), "status": active_run.get("status"),
                                           "cross_process_stop_supported": bool(active_run.get("cross_process_stop_supported"))},
                          "progress": progress, "progress_detail": detail})
            with self.server.lock:
                self.server.attached_run_id = active_run.get("run_id")
                self.server.progress = progress
                self.server.progress_detail = detail
        elif not state["busy"] and attached_run_id and not active_run:
            completed = next((run for run in runs if run.get("run_id") == attached_run_id), None)
            if completed and completed.get("status") != "running":
                state["progress"] = 100.0 if completed.get("status") == "complete" else state["progress"]
                state["progress_detail"] = {**(state["progress_detail"] or {}), "stage": completed.get("status")}
        state["runs"] = runs[:20]
        return state

    def _sse(self, event: str, data: dict, event_id: int | None = None) -> None:
        fields = []
        if event_id is not None:
            fields.append(f"id: {event_id}")
        if event:
            fields.append(f"event: {event}")
        fields.append("data: " + json.dumps(data, ensure_ascii=False, separators=(",", ":")))
        body = ("\n".join(fields) + "\n\n").encode("utf-8")
        self.wfile.write(f"{len(body):X}\r\n".encode("ascii") + body + b"\r\n")
        self.wfile.flush()

    def _sse_heartbeat(self) -> None:
        body = b": keep-alive\n\n"
        self.wfile.write(f"{len(body):X}\r\n".encode("ascii") + body + b"\r\n")
        self.wfile.flush()

    def _serve_events(self) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-cache, no-transform")
        self.send_header("X-Accel-Buffering", "no")
        self.send_header("Transfer-Encoding", "chunked")
        self.end_headers()
        try:
            header_cursor = self.headers.get("Last-Event-ID", "")
            try:
                cursor = max(0, int(header_cursor)) if header_cursor else 0
            except ValueError:
                cursor = 0
            if cursor == 0:
                snapshot = self._state_snapshot()
                snapshot["events"] = snapshot["recent_events"]
                cursor = int(snapshot["cursor"])
                self._sse("snapshot", snapshot, cursor)
            else:
                with self.server.events_condition:
                    oldest = self.server.events[0]["cursor"] if self.server.events else self.server.cursor + 1
                    needs_snapshot = cursor < oldest - 1 or cursor > self.server.cursor
                if needs_snapshot:
                    snapshot = self._state_snapshot()
                    snapshot["events"] = snapshot["recent_events"]
                    cursor = int(snapshot["cursor"])
                    self._sse("snapshot", snapshot, cursor)

            while True:
                with self.server.events_condition:
                    events = [item for item in self.server.events if item["cursor"] > cursor]
                    if not events:
                        self.server.events_condition.wait(timeout=15)
                        events = [item for item in self.server.events if item["cursor"] > cursor]
                if events:
                    for item in events:
                        self._sse("message", item, item["cursor"])
                        cursor = item["cursor"]
                else:
                    snapshot = self._state_snapshot()
                    cursor = int(snapshot["cursor"])
                    self._sse("state", snapshot, cursor)
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, OSError):
            return
        finally:
            try:
                self.wfile.write(b"0\r\n\r\n")
                self.wfile.flush()
            except OSError:
                pass

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/":
            body = PAGE.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if parsed.path == "/api/events":
            self._serve_events()
            return
        if parsed.path == "/api/state":
            query = parse_qs(parsed.query)
            try:
                since = int(query.get("since", ["0"])[0])
            except ValueError:
                since = 0
            self._json(200, self._state_snapshot(since))
            return
        if parsed.path.startswith("/api/runs/"):
            run_id = unquote(parsed.path.removeprefix("/api/runs/"))
            query = parse_qs(parsed.query)
            try:
                offset = max(0, int(query.get("offset", ["0"])[0]))
                limit = min(100, max(1, int(query.get("limit", ["50"])[0])))
                self._json(200, _run_details(run_id, offset, limit))
            except FileNotFoundError as error:
                self._json(404, {"error": str(error)})
            except (ValueError, OSError, sqlite3.Error, json.JSONDecodeError) as error:
                self._json(400, {"error": str(error)})
            return
        self._json(404, {"error": "not found"})

    def do_POST(self):
        path = urlparse(self.path).path
        try:
            length = int(self.headers.get("Content-Length", "0"))
            payload = json.loads(self.rfile.read(length) or b"{}")
            if path == "/api/preview":
                self._validate_selection(payload)
                self.server.launch("preview", payload)
            elif path == "/api/collect":
                self._validate_selection(payload)
                if not payload.get("preview_dir"):
                    raise ValueError("gere a prévia antes da coleta")
                workers = int(payload.get("workers", 15))
                if workers < 1:
                    raise ValueError("workers deve ser pelo menos 1")
                turns = ["1", "2"] if payload["turn"] == "ambos" else [payload["turn"]]
                self.server.launch("collect", {**payload, "turns": turns, "workers": workers})
            elif path == "/api/batch-process":
                self._validate_selection(payload)
                if not payload.get("preview_dir"):
                    raise ValueError("gere a prévia antes do processamento por remessas")
                sections_per_batch = int(payload.get("sections_per_batch", 500))
                workers = int(payload.get("workers", 15))
                if not 1 <= sections_per_batch <= 10000:
                    raise ValueError("seções por remessa deve estar entre 1 e 10000")
                if workers < 1:
                    raise ValueError("workers deve ser pelo menos 1")
                if payload.get("confirm_clean_data") is not True:
                    raise ValueError("confirme a limpeza inicial de dados/")
                turns = ["1", "2"] if payload["turn"] == "ambos" else [payload["turn"]]
                self.server.launch("batch-process", {**payload, "turns": turns,
                    "sections_per_batch": sections_per_batch, "workers": workers})
            elif path == "/api/reprocess":
                data_root = payload.get("data_root")
                self.server.launch("reprocess", {"data_root": data_root} if data_root else {})
            elif path == "/api/resume":
                if not payload.get("run_id"):
                    raise ValueError("run_id é obrigatório")
                self.server.launch("resume", payload)
            elif path == "/api/stop":
                reconcile_interrupted_runs()
                with self.server.lock:
                    stop_event = self.server.stop_event
                if stop_event:
                    stop_event.set()
                    run_id = self.server.current_run_id
                else:
                    run_id = request_active_run_stop()
                self._json(202, {"ok": True, "run_id": run_id,
                                 "message": "A parada segura foi solicitada; workers ativos concluirão a unidade atual."})
                return
            else:
                self._json(404, {"error": "not found"})
                return
            self._json(202, {"ok": True})
        except (ValueError, KeyError, TypeError, RuntimeError, json.JSONDecodeError) as error:
            self._json(400, {"error": str(error)})

    @staticmethod
    def _validate_selection(payload: dict) -> None:
        if payload.get("turn") not in {"1", "2", "ambos"}:
            raise ValueError("selecione turno 1, 2 ou ambos")
        payload.setdefault("uf", "")
        payload.setdefault("municipio", "")
        payload.setdefault("zona", "")
        payload.setdefault("secao", "")
        payload.setdefault("offset", 0)
        payload.setdefault("limit", None)
        payload["offset"] = int(payload["offset"])
        payload["limit"] = int(payload["limit"]) if payload["limit"] is not None else None
        if payload["offset"] < 0 or (payload["limit"] is not None and payload["limit"] < 1):
            raise ValueError("offset deve ser >= 0 e máximo de seções deve ser > 0")


def main() -> int:
    server = JobServer(("127.0.0.1", 0))
    url = f"http://{server.server_address[0]}:{server.server_address[1]}"
    visible_url = f"\x1b]8;;{url}\x1b\\{url}\x1b]8;;\x1b\\" if os.environ.get("WT_SESSION") else url
    print(f"UI local disponível em {visible_url} — pressione Ctrl+C para encerrar.", flush=True)
    if "--no-open" not in sys.argv[1:]:
        try:
            webbrowser.open(url)
        except webbrowser.Error:
            pass
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nEncerrando UI local.")
    finally:
        server.shutdown()
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
