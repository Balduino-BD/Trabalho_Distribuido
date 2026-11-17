


"""
Painel web para simular e visualizar a REPLICAÇÃO SEMI-ATIVA.

O painel é um CLIENTE das réplicas: faz as operações bancárias (como o
cliente.py, falando só com o líder) e, a cada 0,4 s, consulta o estado
(STATUS) e os eventos (EVENTOS) de cada réplica para os mostrar.

Uso:
    python painel.py                 (abre http://localhost:8080)
    python painel.py --porta 9000 --sem-navegador
"""

import argparse
import json
import os
import threading
import time
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from cliente import ClienteBanco
from replica import enviar_mensagem


class Coletor:
    """Consulta periodicamente as réplicas e junta os eventos numa linha do tempo."""

    def __init__(self, config):
        self.replicas = {int(r): (d["host"], d["porta"]) for r, d in config["replicas"].items()}
        self.estado = {r: None for r in self.replicas}
        self.ultimo_seq = {r: 0 for r in self.replicas}
        self.eventos, self.gid = [], 0
        self.trava = threading.Lock()

    def consultar(self, rid):
        host, porta = self.replicas[rid]
        status = enviar_mensagem(host, porta, {"tipo": "STATUS", "origem": "painel"}, 0.5)
        novos = []
        if status is not None:
            r = enviar_mensagem(host, porta, {"tipo": "EVENTOS", "origem": "painel",
                                              "desde": self.ultimo_seq[rid]}, 0.5)
            if r is not None:
                if r["ultimo"] < self.ultimo_seq[rid]:          # a réplica reiniciou
                    self.ultimo_seq[rid] = 0
                    r = enviar_mensagem(host, porta, {"tipo": "EVENTOS", "origem": "painel",
                                                      "desde": 0}, 0.5) or {"eventos": []}
                novos = r["eventos"]
                if novos:
                    self.ultimo_seq[rid] = novos[-1]["seq"]
        with self.trava:
            self.estado[rid] = status
            for e in novos:
                self.gid += 1
                e["gid"] = self.gid
                self.eventos.append(e)
            self.eventos = self.eventos[-600:]

    def laco(self):
        while True:
            threads = [threading.Thread(target=self.consultar, args=(r,)) for r in self.replicas]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            time.sleep(0.4)

    def instantaneo(self, desde):
        with self.trava:
            evs = sorted((e for e in self.eventos if e["gid"] > desde), key=lambda e: e["t"])
            return {"replicas": {str(r): self.estado[r] for r in sorted(self.replicas)},
                    "eventos": evs, "gid": self.gid}


def criar_tratador(coletor, cliente):
    trava_cliente = threading.Lock()

    class Tratador(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def responder(self, codigo, corpo, tipo="application/json"):
            dados = corpo.encode("utf-8")
            self.send_response(codigo)
            self.send_header("Content-Type", f"{tipo}; charset=utf-8")
            self.send_header("Content-Length", str(len(dados)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(dados)

        def do_GET(self):
            url = urlparse(self.path)
            if url.path == "/":
                self.responder(200, PAGINA, "text/html")
            elif url.path == "/api/estado":
                desde = int(parse_qs(url.query).get("desde", ["0"])[0])
                self.responder(200, json.dumps(coletor.instantaneo(desde)))
            elif url.path == "/favicon.ico":
                self.send_response(204)
                self.end_headers()
            else:
                self.responder(404, json.dumps({"erro": "não encontrado"}))

        def do_POST(self):
            try:
                corpo = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))))
            except ValueError:
                self.responder(400, json.dumps({"erro": "pedido inválido"}))
                return
            if self.path == "/api/operacao":
                op = corpo.get("op")
                if op not in ("depositar", "sacar", "saldo", "extrato"):
                    self.responder(400, json.dumps({"erro": "operação inválida"}))
                    return
                valor = corpo.get("valor") if op in ("depositar", "sacar") else None
                with trava_cliente:
                    r, tentativas = cliente.operacao(op, str(corpo.get("conta", "A")), valor)
                r["tentativas"] = tentativas
                self.responder(200, json.dumps(r))
            elif self.path == "/api/admin":
                rid, cmd = int(corpo.get("replica", 0)), corpo.get("comando")
                if rid not in coletor.replicas or cmd not in ("falhar", "recuperar"):
                    self.responder(400, json.dumps({"erro": "pedido inválido"}))
                    return
                r = cliente.admin(rid, cmd.upper())
                self.responder(200, json.dumps(r or {"tipo": "ERRO",
                                                     "erro": f"R{rid} não respondeu"}))
            else:
                self.responder(404, json.dumps({"erro": "não encontrado"}))

    return Tratador


PAGINA = r"""<!DOCTYPE html>
<html lang="pt">
<head>
<meta charset="utf-8">
<title>Replicação Semi-ativa — Painel</title>
<style>
  :root { --bg:#0f1720; --card:#17212c; --line:#2a3846; --txt:#e6edf3; --dim:#93a4b5;
          --lider:#f2b134; --seg:#2f9e8f; --caida:#5b6572; --off:#30363d; }
  * { box-sizing:border-box; }
  body { margin:0; font-family:system-ui,-apple-system,"Segoe UI",sans-serif; background:var(--bg); color:var(--txt); }
  header { padding:16px 26px; border-bottom:1px solid var(--line); display:flex; gap:18px; align-items:baseline; flex-wrap:wrap; }
  header h1 { margin:0; font-size:22px; }
  header .sub { color:var(--dim); font-size:14px; }
  main { display:grid; grid-template-columns:minmax(560px,1.15fr) minmax(380px,1fr); gap:18px; padding:18px 26px; }
  .card { background:var(--card); border:1px solid var(--line); border-radius:12px; padding:14px 16px; }
  h2 { margin:0 0 10px 0; font-size:14px; color:var(--dim); font-weight:600; text-transform:uppercase; letter-spacing:.06em; }
  svg { width:100%; height:auto; display:block; max-height:44vh; }
  .legenda { display:flex; gap:12px; flex-wrap:wrap; font-size:12px; color:var(--dim); margin:4px 0 10px; }
  .legenda span::before { content:""; display:inline-block; width:11px; height:11px; border-radius:3px; margin-right:5px; vertical-align:-1px; background:var(--c); }
  .reps { display:grid; grid-template-columns:repeat(auto-fit,minmax(150px,1fr)); gap:8px; }
  .rep { border:1px solid var(--line); border-radius:10px; padding:8px 10px; font-size:13px; }
  .rep .topo { display:flex; justify-content:space-between; align-items:center; margin-bottom:4px; }
  .rep b { font-size:16px; }
  .badge { font-size:11px; padding:2px 8px; border-radius:20px; }
  .rep .info { color:var(--dim); margin-bottom:6px; min-height:34px; }
  .botoes { display:grid; grid-template-columns:1fr 1fr; gap:5px; }
  button { font:inherit; font-size:12px; padding:6px 6px; border-radius:7px; border:1px solid var(--line); background:#1f2b38; color:var(--txt); cursor:pointer; }
  button:hover { background:#273646; }
  button.perigo { border-color:#6b2f33; } button.bom { border-color:#25584c; }
  input { font:inherit; font-size:14px; padding:6px 8px; border-radius:7px; border:1px solid var(--line); background:#0f1720; color:var(--txt); width:90px; }
  .linha-ctrl { display:flex; gap:8px; align-items:center; flex-wrap:wrap; }
  .res { margin-top:10px; font-size:14px; min-height:20px; }
  .tent { color:var(--dim); font-size:12px; margin-top:4px; font-family:ui-monospace,Consolas,monospace; }
  table { border-collapse:collapse; width:100%; font-size:13px; margin-top:8px; }
  th, td { text-align:left; padding:5px 8px; border-bottom:1px solid var(--line); }
  th { color:var(--dim); font-weight:600; }
  td.num { text-align:right; font-variant-numeric:tabular-nums; }
  tr.dif td { color:#ff9b9b; }
  #linha { height:calc(100vh - 150px); min-height:420px; overflow:auto; font-family:ui-monospace,Consolas,monospace; font-size:12.5px; }
  .ev { padding:4px 8px; border-left:4px solid var(--line); margin-bottom:3px; background:#131c25; border-radius:0 6px 6px 0; }
  .ev .h { color:var(--dim); margin-right:6px; } .ev .n { font-weight:700; margin-right:6px; }
  .msg-linha { fill:none; stroke-width:4; stroke-linecap:round; animation: some 2.2s ease-in forwards; }
  @keyframes some { 0%,70% { opacity:1; } 100% { opacity:0; } }
  .sub-no { paint-order:stroke; stroke:#17212c; stroke-width:6px; stroke-linejoin:round; }
  .grid2 { display:grid; grid-template-columns:1fr 1fr; gap:12px; margin-top:12px; }
</style>
</head>
<body>
<header>
  <h1>Replicação Semi-ativa · Banco replicado</h1>
  <span class="sub">Grupo 12 · Sistemas Distribuídos · só o líder recebe e responde aos clientes; todas as réplicas executam</span>
</header>
<main>
  <section>
    <div class="card">
      <h2>Grupo de réplicas</h2>
      <svg id="cena" viewBox="0 0 640 345" aria-label="Cliente, líder e seguidores"></svg>
      <div class="legenda" id="legenda"></div>
      <div class="reps" id="reps"></div>
    </div>
    <div class="grid2">
      <div class="card">
        <h2>Cliente</h2>
        <div class="linha-ctrl">
          Conta <input id="conta" value="A">
          Valor <input id="valor" type="number" value="100" min="0" step="0.01">
        </div>
        <div class="linha-ctrl" style="margin-top:8px">
          <button class="bom" onclick="operacao('depositar')">Depositar</button>
          <button onclick="operacao('sacar')">Sacar</button>
          <button onclick="operacao('saldo')">Saldo</button>
          <button onclick="operacao('extrato')">Extrato</button>
        </div>
        <div class="res" id="res"></div>
        <div class="tent" id="tent"></div>
        <div id="extrato"></div>
      </div>
      <div class="card">
        <h2>Estado em cada réplica</h2>
        <div id="comparacao"></div>
        <div class="tent">Todas as réplicas ativas devem ter o mesmo seq e os mesmos saldos.</div>
      </div>
    </div>
  </section>
  <section class="card">
    <h2>Linha do tempo das mensagens</h2>
    <div id="linha"></div>
  </section>
</main>
<script>
const COR = { PEDIDO:"#c58af9", ORDEM:"#f08c3a", ACK:"#4cc38a", RESPOSTA:"#c58af9",
              VISTA:"#5aa9ff", ESTADO:"#3fd0d0", JUNTAR:"#e8d44d", FALHA:"#ff6b6b" };
const NOMES = { PEDIDO:"PEDIDO", ORDEM:"ORDEM (difusão FIFO)", ACK:"ACK", RESPOSTA:"RESPOSTA",
                VISTA:"VISTA (membership)", ESTADO:"get/set_state", JUNTAR:"JUNTAR", FALHA:"Falha detectada" };
const NS = "http://www.w3.org/2000/svg";
const svg = document.getElementById("cena");
let pos = {}, ids = [], desde = 0;

function el(tag, a, pai) { const e = document.createElementNS(NS, tag); for (const k in a) e.setAttribute(k, a[k]); if (pai) pai.appendChild(e); return e; }

function montar(reps) {
  ids = Object.keys(reps).map(Number).sort((a,b)=>a-b);
  pos["C"] = { x: 80, y: 165 };
  const cx = 420, cy = 165, r = 120;
  ids.forEach((id, i) => { const a = Math.PI + 2*Math.PI*i/ids.length + (ids.length===3 ? 0 : 0);
    pos[id] = { x: cx + r*Math.cos(a) * 1.05 + 20, y: cy + r*Math.sin(a) }; });
  if (ids.length === 3) { pos[ids[0]] = {x:330,y:165}; pos[ids[1]] = {x:540,y:62}; pos[ids[2]] = {x:540,y:262}; }
  const defs = el("defs", {}, svg);
  for (const t in COR) { const m = el("marker", { id:"s-"+t, viewBox:"0 0 10 10", refX:"9", refY:"5", markerWidth:"5", markerHeight:"5", orient:"auto-start-reverse" }, defs);
    el("path", { d:"M0,0 L10,5 L0,10 z", fill:COR[t] }, m); }
  el("g", { id:"msgs" }, svg);
  const g = el("g", {}, svg);
  // cliente
  el("rect", { x:pos.C.x-46, y:pos.C.y-34, width:92, height:68, rx:12, fill:"#c58af9" }, g);
  const tc = el("text", { x:pos.C.x, y:pos.C.y+7, "text-anchor":"middle", "font-size":20, "font-weight":700, fill:"#0f1720" }, g); tc.textContent = "Cliente";
  ids.forEach(id => {
    const gr = el("g", { id:"r-"+id }, g);
    el("circle", { cx:pos[id].x, cy:pos[id].y, r:44, class:"corpo", stroke:"#0f1720", "stroke-width":3 }, gr);
    const t = el("text", { x:pos[id].x, y:pos[id].y+9, "text-anchor":"middle", "font-size":26, "font-weight":700, fill:"#0f1720" }, gr); t.textContent = "R"+id;
    el("text", { x:pos[id].x, y:pos[id].y+66, "text-anchor":"middle", "font-size":14, fill:"#c9d4de", class:"sub sub-no" }, gr);
    el("text", { x:pos[id].x, y:pos[id].y-54, "text-anchor":"middle", "font-size":20, class:"coroa" }, gr);
  });
  const leg = document.getElementById("legenda");
  leg.innerHTML = `<span style="--c:var(--lider)">Líder</span><span style="--c:var(--seg)">Seguidor</span><span style="--c:var(--caida)">Caída</span>` +
    Object.keys(NOMES).filter(k=>k!=="PEDIDO").map(k => `<span style="--c:${COR[k]}">${NOMES[k]}</span>`).join("");
  const cont = document.getElementById("reps");
  ids.forEach(id => cont.insertAdjacentHTML("beforeend", `
    <div class="rep"><div class="topo"><b>R${id}</b><span class="badge" id="b-${id}">—</span></div>
      <div class="info" id="i-${id}"></div>
      <div class="botoes"><button class="perigo" onclick="admin(${id},'falhar')">Falhar</button>
        <button class="bom" onclick="admin(${id},'recuperar')">Recuperar</button></div></div>`));
}

function papel(s, id) {
  if (!s) return ["Desligada", "#30363d", "#30363d"];
  if (!s.ativo) return ["Caída", "#5b6572", "#5b6572"];
  if (s.lider === id) return ["Líder", "#f2b134", "#8a6414"];
  if (s.lider == null) return ["Entrando", "#3b4b5c", "#3b4b5c"];
  return ["Seguidor", "#2f9e8f", "#1d5f56"];
}

function desenhar(reps) {
  ids.forEach(id => {
    const s = reps[id], g = document.getElementById("r-"+id);
    const [rot, cor, fundo] = papel(s, id);
    g.querySelector(".corpo").setAttribute("fill", cor);
    g.querySelector(".coroa").textContent = rot === "Líder" ? "👑" : "";
    g.querySelector(".sub").textContent = s && s.ativo ? `seq ${s.seq}` : rot.toLowerCase();
    const b = document.getElementById("b-"+id); b.textContent = rot; b.style.background = fundo;
    document.getElementById("i-"+id).textContent = s && s.ativo
      ? `vista #${s.num_vista} [${s.vista.map(v=>"R"+v).join(", ")}]` : "";
  });
  // tabela de comparação de estado
  const contas = [...new Set(ids.flatMap(id => reps[id] && reps[id].ativo ? Object.keys(reps[id].contas) : []))].sort();
  const lider = ids.find(id => reps[id] && reps[id].ativo && reps[id].lider === id);
  const ref = lider ? reps[lider] : null;
  let h = `<table><tr><th>Réplica</th><th>seq</th>${contas.map(c=>`<th>${c}</th>`).join("")}</tr>`;
  ids.forEach(id => { const s = reps[id];
    if (!s || !s.ativo) { h += `<tr><td>R${id}</td><td colspan="${contas.length+1}" style="color:var(--dim)">${papel(s,id)[0].toLowerCase()}</td></tr>`; return; }
    const dif = ref && s.seq !== ref.seq;
    h += `<tr class="${dif ? "dif" : ""}"><td>R${id}${s.lider===id?" 👑":""}</td><td class="num">${s.seq}</td>` +
         contas.map(c => `<td class="num">${s.contas[c] != null ? s.contas[c].toFixed(2) : "—"}</td>`).join("") + "</tr>"; });
  document.getElementById("comparacao").innerHTML = h + "</table>";
}

function seta(de, para, tipo) {
  if (!pos[de] || !pos[para] || de === para) return;
  const a = pos[de], b = pos[para];
  const ra = de === "C" ? 50 : 48, rb = para === "C" ? 52 : 50;
  const dx = b.x-a.x, dy = b.y-a.y, d = Math.hypot(dx,dy), ux = dx/d, uy = dy/d, nx = -uy*7, ny = ux*7;
  const x1 = a.x+ux*ra+nx, y1 = a.y+uy*ra+ny, x2 = b.x-ux*rb+nx, y2 = b.y-uy*rb+ny;
  const l = el("line", { x1, y1, x2, y2, stroke:COR[tipo], class:"msg-linha", "marker-end":"url(#s-"+tipo+")" }, document.getElementById("msgs"));
  if (tipo === "FALHA") l.setAttribute("stroke-dasharray", "8 8");
  const comp = Math.hypot(x2-x1, y2-y1);
  l.animate([{ strokeDasharray:`0 ${comp}` }, { strokeDasharray:`${comp} 0` }], { duration:450, easing:"ease-out" });
  setTimeout(() => l.remove(), 2300);
}

function eventos(evs) {
  const linha = document.getElementById("linha");
  const fim = linha.scrollTop + linha.clientHeight >= linha.scrollHeight - 30;
  evs.forEach(e => {
    const div = document.createElement("div"); div.className = "ev";
    if (e.msg) div.style.borderLeftColor = COR[e.msg];
    div.innerHTML = `<span class="h">${e.hora}</span><span class="n">R${e.no}</span>`;
    div.appendChild(document.createTextNode(e.texto)); linha.appendChild(div);
    if (e.msg) (e.para || []).forEach(p => seta(e.no, p, e.msg));
  });
  while (linha.children.length > 500) linha.removeChild(linha.firstChild);
  if (fim) linha.scrollTop = linha.scrollHeight;
}

async function atualizar() {
  try { const d = await (await fetch("/api/estado?desde=" + desde)).json();
    if (!ids.length) montar(d.replicas); desenhar(d.replicas); eventos(d.eventos); desde = d.gid;
  } catch (e) {}
  setTimeout(atualizar, 400);
}

async function post(url, corpo) {
  return (await fetch(url, { method:"POST", headers:{ "Content-Type":"application/json" }, body:JSON.stringify(corpo) })).json();
}

async function operacao(op) {
  const conta = document.getElementById("conta").value.trim() || "A";
  const valor = Number(document.getElementById("valor").value);
  const res = document.getElementById("res"), tent = document.getElementById("tent"), ext = document.getElementById("extrato");
  res.style.color = "var(--dim)"; res.textContent = "a enviar ao líder…"; tent.textContent = ""; ext.innerHTML = "";
  const lider = ids.find(id => { const g = document.getElementById("b-"+id); return g && g.textContent === "Líder"; });
  if (lider) seta("C", lider, "PEDIDO");
  const r = await post("/api/operacao", { op, conta, valor });
  tent.textContent = (r.tentativas || []).join(" · ");
  if (r.tipo !== "RESPOSTA") { res.style.color = "#ff6b6b"; res.textContent = r.erro; return; }
  const rep = r.repetido ? " (pedido repetido: não executado de novo)" : "";
  if (op === "depositar" || op === "sacar") {
    res.style.color = r.ok ? "#4cc38a" : "#ff6b6b";
    res.textContent = `${op} ${valor.toFixed(2)} na conta ${conta}: ${r.ok ? "OK" : "RECUSADO — " + r.erro} · saldo ${r.saldo.toFixed(2)} · ` +
                      `${r.transacao} · respondido pelo líder R${r.origem}${rep}`;
  } else {
    res.style.color = "#4cc38a";
    res.textContent = `Saldo da conta ${conta}: ${r.saldo.toFixed(2)} · respondido pelo líder R${r.origem}`;
    if (op === "extrato") ext.innerHTML = `<table><tr><th>Transação</th><th>Data/hora (decidida pelo líder)</th><th>Op.</th><th>Valor</th><th>Saldo</th></tr>` +
      r.extrato.map(e => `<tr><td>${e.transacao}</td><td>${e.data_hora}</td><td>${e.op}</td><td class="num">${e.valor.toFixed(2)}</td><td class="num">${e.saldo.toFixed(2)}</td></tr>`).join("") + "</table>";
  }
}

async function admin(replica, comando) {
  const r = await post("/api/admin", { replica, comando });
  const res = document.getElementById("res");
  res.style.color = r.tipo === "ERRO" ? "#ff6b6b" : "var(--dim)";
  res.textContent = r.tipo === "ERRO" ? r.erro : `R${replica}: comando "${comando}" enviado`;
}

atualizar();
</script>
</body>
</html>
"""


def main():
    parser = argparse.ArgumentParser(description="Painel da replicação semi-ativa")
    parser.add_argument("--config", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json"))
    parser.add_argument("--porta", type=int, default=8080)
    parser.add_argument("--sem-navegador", action="store_true")
    args = parser.parse_args()
    with open(args.config, encoding="utf-8") as f:
        config = json.load(f)

    coletor = Coletor(config)
    threading.Thread(target=coletor.laco, daemon=True).start()
    servidor = ThreadingHTTPServer(("127.0.0.1", args.porta),
                                   criar_tratador(coletor, ClienteBanco(config)))
    url = f"http://localhost:{args.porta}"
    print(f"Painel em {url}  (Ctrl+C para sair)")
    if not args.sem_navegador:
        threading.Timer(0.8, webbrowser.open, args=(url,)).start()
    try:
        servidor.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()