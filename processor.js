// processor.js – replica do processar_nap.py no browser
// Processa o CSV exportado do sistema e retorna os mesmos agregados

const STATUS_FECHADO = new Set(['fechado','resolvido','cancelado','encerrado','closed','resolved']);

const COL_MAP = {
  cidade:     ['cidades','cidade'],
  ticket:     ['nº do ticket','numero do ticket','ticket','n do ticket'],
  status:     ['status'],
  grupo:      ['grupo'],
  abertura:   ['abertura'],
  previsao:   ['previsão','previsao'],
  fechamento: ['data resolução','data resolucao','data resolucao'],
};

function normText(s){ return (s||'').normalize('NFD').replace(/[\u0300-\u036f]/g,'').toLowerCase().trim(); }

function findCol(headers, canonical){
  const variants = COL_MAP[canonical] || [canonical];
  return headers.find(h => variants.includes(normText(h))) || null;
}

function normalizeCity(s){
  if(!s||!s.trim()) return 'INDEFINIDO';
  return s.trim().toUpperCase().replace(/_/g,' ').replace(/\s+/g,' ');
}

function parseDate(s){
  if(!s||!s.trim()) return null;
  const d = new Date(s.trim().replace(' ','T'));
  return isNaN(d) ? null : d;
}

function pct(closed, opened){ return opened ? +(closed/opened*100).toFixed(2) : 0; }

function isoWeek(d){
  const jan4 = new Date(d.getFullYear(),0,4);
  const startW1 = new Date(jan4);
  startW1.setDate(jan4.getDate() - ((jan4.getDay()+6)%7));
  const diff = d - startW1;
  return Math.floor(diff/(7*864e5)) + 1;
}

function dateKey(d){ return d ? `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}` : null; }
function monthKey(d){ return d ? `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}` : null; }
function weekKey(d){ return d ? `${d.getFullYear()}-W${String(isoWeek(d)).padStart(2,'0')}` : null; }

const MES_NOME = {1:'Janeiro',2:'Fevereiro',3:'Março',4:'Abril',5:'Maio',6:'Junho',7:'Julho',8:'Agosto',9:'Setembro',10:'Outubro',11:'Novembro',12:'Dezembro'};

export function processRows(rawRows){
  // Detect column mapping from first row keys
  const headers = Object.keys(rawRows[0]||{});
  const C = {};
  for(const k of Object.keys(COL_MAP)) C[k] = findCol(headers, k);

  // Normalize rows
  const rows = rawRows.map(r => {
    const ab = parseDate(C.abertura ? r[C.abertura] : null);
    const fe = parseDate(C.fechamento ? r[C.fechamento] : null);
    const st = C.status ? (r[C.status]||'').trim() : '';
    const fechado = fe != null || STATUS_FECHADO.has(normText(st));
    return {
      cidade:    normalizeCity(C.cidade ? r[C.cidade] : ''),
      ticket:    C.ticket ? r[C.ticket] : '',
      status:    st,
      grupo:     C.grupo ? (r[C.grupo]||'').trim() : '',
      abertura:  ab,
      fechamento:fe,
      fechado,
    };
  }).filter(r => r.abertura);

  // ── Helpers ──────────────────────────────────────────────
  function agg(groups){
    return Object.entries(groups).map(([k,g]) => {
      const ab = g.length, fe = g.filter(r=>r.fechado).length;
      return {key:k, abertos:ab, fechados:fe, diferenca:ab-fe, pct_fechamento:pct(fe,ab)};
    });
  }

  function group(keyFn){
    return rows.reduce((acc,r)=>{ const k=keyFn(r); if(k){(acc[k]=acc[k]||[]).push(r);} return acc; }, {});
  }

  // ── Mensal ────────────────────────────────────────────────
  const mensal = agg(group(r=>monthKey(r.abertura)))
    .sort((a,b)=>a.key.localeCompare(b.key))
    .map(r => {
      const [yr,mo] = r.key.split('-').map(Number);
      return {...r, periodo:r.key, ano:yr, mes:mo, mes_nome:MES_NOME[mo]};
    });

  // ── Semanal ───────────────────────────────────────────────
  const semanal = agg(group(r=>weekKey(r.abertura)))
    .sort((a,b)=>a.key.localeCompare(b.key))
    .map(r => {
      const [yr,w] = r.key.split('-W').map(Number);
      return {...r, semana:r.key, ano:yr, semana_iso:w};
    });

  // ── Diário ────────────────────────────────────────────────
  // abertura por dia de abertura
  const abPorDia = group(r=>dateKey(r.abertura));
  // fechamento por dia de resolução
  const fePorDia = rows.filter(r=>r.fechamento).reduce((acc,r)=>{ const k=dateKey(r.fechamento); (acc[k]=acc[k]||[]).push(r); return acc; },{});
  const diasSet = new Set([...Object.keys(abPorDia),...Object.keys(fePorDia)]);
  const diario = [...diasSet].sort().map(d => {
    const ab=(abPorDia[d]||[]).length, fe=(fePorDia[d]||[]).length;
    return {data:d, abertos:ab, fechados:fe, diferenca:ab-fe, pct_fechamento:pct(fe,ab)};
  });

  // ── Por cidade ────────────────────────────────────────────
  const cidGrp = group(r=>r.cidade);
  let cidades = agg(cidGrp).map(r=>({...r, cidade:r.key, backlog:r.diferenca}));
  cidades.sort((a,b)=>b.abertos-a.abertos);
  cidades.forEach((r,i)=>r.rank_volume=i+1);
  const byBacklog = [...cidades].sort((a,b)=>b.backlog-a.backlog);
  byBacklog.forEach((r,i)=>r.rank_pendencia=i+1);

  // ── Por status ────────────────────────────────────────────
  const stGrp = rows.reduce((acc,r)=>{ (acc[r.status]=acc[r.status]||0); acc[r.status]++; return acc; },{});
  const status = Object.entries(stGrp)
    .map(([s,n])=>({status:s||'(vazio)', quantidade:n, pct:pct(n,rows.length)}))
    .sort((a,b)=>b.quantidade-a.quantidade);

  // ── Por grupo ─────────────────────────────────────────────
  const grpGrp = group(r=>r.grupo||'(sem grupo)');
  const grupo = agg(grpGrp).map(r=>({...r, grupo:r.key, backlog:r.diferenca}))
    .sort((a,b)=>b.abertos-a.abertos);

  return {rows, mensal, semanal, diario, cidades, status, grupo};
}
