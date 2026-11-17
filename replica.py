#!/usr/bin/env python3
"""
Réplica de um serviço bancário com REPLICAÇÃO SEMI-ATIVA.

Modelo (Aula 10 - Tolerância a Faltas):
  - Todas as réplicas são ativas (todas executam cada operação), mas só o
    LÍDER recebe e responde às invocações dos clientes.
  - Os SEGUIDORES executam as operações na ordem imposta pelo líder e têm a
    função de substituir o líder caso ele falhe.
  - O determinismo de réplica é garantido pelo líder: é ele que decide a ordem
    (número de sequência) e os valores não determinísticos (data/hora e
    identificador da transação) e envia essas decisões aos seguidores.

Mecanismos implementados:
  - Difusão FIFO das ordens do líder para os seguidores (ORDEM + ACK);
  - Detecção de falhas por timeouts e keepalives (HEARTBEAT);
  - Gerenciamento de membros: a VISTA é a lista ordenada de réplicas ativas;
    o líder é sempre o primeiro da vista;
  - Transferência de estado: get_state / set_state (JUNTAR, PEDIR_ESTADO,
    SET_STATE) para réplicas que entram, voltam ou ficaram atrasadas.

Mensagens (JSON, uma por linha, uma conexão TCP por mensagem):
  Cliente -> líder:   PEDIDO (op: depositar | sacar | saldo | extrato)
  Líder -> seguidor:  ORDEM, HEARTBEAT, VISTA, SET_STATE
  Seguidor -> líder:  ACK, FALTA (falta uma ordem), JUNTAR, PEDIR_ESTADO
  Testes / painel:    STATUS, EVENTOS, FALHAR, RECUPERAR

Uso:
    python replica.py --id 1
"""

import argparse
import json
import os
import socket
import socketserver
import threading
import time
from collections import deque
from datetime import datetime


# --------------------------------------------------------------------------
# Comunicação: uma conexão TCP por mensagem (linha JSON) e uma resposta.
# --------------------------------------------------------------------------
def enviar_mensagem(host, porta, mensagem, timeout):
    """Envia `mensagem` e devolve a resposta (dict) ou None se o destino não
    responder dentro do `timeout` (a réplica é considerada falha)."""
    try:
        with socket.create_connection((host, porta), timeout=timeout) as s:
            s.settimeout(timeout)
            s.sendall((json.dumps(mensagem) + "\n").encode("utf-8"))
            linha = s.makefile("r", encoding="utf-8").readline()
            return json.loads(linha) if linha else None
    except (OSError, ValueError):
        return None


class TratadorConexao(socketserver.StreamRequestHandler):
    """Recebe UMA mensagem por conexão e devolve a resposta da réplica."""

    def handle(self):
        try:
            linha = self.rfile.readline().decode("utf-8")
            if not linha:
                return
            mensagem = json.loads(linha)
        except (OSError, ValueError):
            return
        resposta = self.server.replica.tratar_mensagem(mensagem)
        if resposta is not None:          # None = réplica "caída": não responde
            self.wfile.write((json.dumps(resposta) + "\n").encode("utf-8"))


class ServidorTCP(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


def estado_vazio():
    return {"seq": 0,           # nº da última ordem aplicada
            "contas": {},       # conta -> saldo
            "extrato": [],      # transações aplicadas, em ordem
            "executados": {}}   # req_id -> resultado (evita executar 2 vezes)


# --------------------------------------------------------------------------
# A réplica
# --------------------------------------------------------------------------
class Replica:
    def __init__(self, meu_id, config):
        self.id = meu_id
        # Números das réplicas: chaves do config.json
        self.replicas = {int(r): (d["host"], d["porta"])
                         for r, d in config["replicas"].items()}
        t = config["tempos"]
        self.t_conexao = t["timeout_conexao"]
        self.t_heartbeat = t["intervalo_heartbeat"]
        self.t_suspeita = t["suspeita_lider"]

        self.ativo = True
        self.estado = estado_vazio()     # ESTADO REPLICADO
        self.vista = []                  # membros, por ordem; vista[0] = líder
        self.num_vista = 0
        self.seq_confirmado = 0          # última ordem confirmada por todos (líder)
        self.ultimo_heartbeat = time.time()
        self.juntando = False

        self.trava = threading.RLock()       # protege estado e vista (curto)
        self.trava_ordem = threading.Lock()  # serializa as ordens do líder (FIFO)
        self.eventos = deque(maxlen=300)
        self.seq_evento = 0
        self.trava_eventos = threading.Lock()

    # ------------------------------------------------------------ utilidades
    @property
    def lider(self):
        return self.vista[0] if self.vista else None

    def sou_lider(self):
        return self.ativo and self.lider == self.id

    def log(self, texto, msg=None, para=None):
        hora = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        papel = "LÍDER" if self.sou_lider() else ("seguidor" if self.vista else "-")
        print(f"[{hora}] [R{self.id} {papel}] {texto}", flush=True)
        with self.trava_eventos:
            self.seq_evento += 1
            self.eventos.append({"seq": self.seq_evento, "t": time.time(), "hora": hora,
                                 "no": self.id, "texto": texto, "msg": msg,
                                 "para": para or []})

    def enviar(self, destino, tipo, **campos):
        host, porta = self.replicas[destino]
        return enviar_mensagem(host, porta, {"tipo": tipo, "origem": self.id, **campos},
                               self.t_conexao)

    def em_paralelo(self, destinos, funcao):
        """Executa funcao(destino) para cada destino em paralelo e devolve
        {destino: resultado}."""
        resultados = {}

        def chamar(d):
            resultados[d] = funcao(d)

        threads = [threading.Thread(target=chamar, args=(d,)) for d in destinos]
        for th in threads:
            th.start()
        for th in threads:
            th.join()
        return resultados

    # --------------------------------------------- serviço (determinístico)
    def aplicar(self, ordem):
        """Aplica uma ordem ao estado. Determinístico: com as mesmas ordens,
        na mesma sequência, todas as réplicas chegam ao mesmo estado."""
        with self.trava:
            contas = self.estado["contas"]
            conta, op, valor = ordem["conta"], ordem["op"], ordem["valor"]
            saldo = contas.get(conta, 0.0)
            if op == "depositar":
                saldo = round(saldo + valor, 2)
                res = {"ok": True, "saldo": saldo}
            elif op == "sacar" and valor > saldo:
                res = {"ok": False, "erro": "saldo insuficiente", "saldo": saldo}
            else:                                   # sacar
                saldo = round(saldo - valor, 2)
                res = {"ok": True, "saldo": saldo}
            contas[conta] = saldo
            res.update({"transacao": ordem["transacao"], "data_hora": ordem["data_hora"],
                        "seq": ordem["seq"]})
            if res["ok"]:
                self.estado["extrato"].append({
                    "seq": ordem["seq"], "transacao": ordem["transacao"],
                    "data_hora": ordem["data_hora"], "conta": conta,
                    "op": op, "valor": valor, "saldo": saldo})
                self.estado["extrato"] = self.estado["extrato"][-500:]
            self.estado["seq"] = ordem["seq"]
            executados = self.estado["executados"]
            executados[ordem["req_id"]] = res
            while len(executados) > 300:                  # guarda só os recentes
                executados.pop(next(iter(executados)))
            return res

    # ------------------------------------------------ transferência de estado
    def get_state(self):
        with self.trava:
            return json.loads(json.dumps(self.estado))          # cópia profunda

    def set_state(self, estado, origem):
        with self.trava:
            if estado["seq"] < self.estado["seq"]:
                return False                                     # nunca regredir
            self.estado = estado
        self.log(f"set_state: estado recebido de R{origem} (seq={estado['seq']})")
        return True

    # ------------------------------------------------ LÍDER: pedidos do cliente
    def processar_pedido(self, msg):
        op, conta = msg.get("op"), str(msg.get("conta", "")).strip()
        if not conta:
            return {"tipo": "ERRO", "origem": self.id, "erro": "conta não informada"}

        # Leituras: o líder responde com o seu estado (não alteram o estado)
        if op in ("saldo", "extrato"):
            with self.trava:
                saldo = self.estado["contas"].get(conta, 0.0)
                linhas = [e for e in self.estado["extrato"] if e["conta"] == conta]
            self.log(f"PEDIDO {op} conta {conta} -> responde ao cliente", msg="RESPOSTA", para=["C"])
            return {"tipo": "RESPOSTA", "origem": self.id, "ok": True, "op": op,
                    "conta": conta, "saldo": saldo, "extrato": linhas if op == "extrato" else None}

        if op not in ("depositar", "sacar"):
            return {"tipo": "ERRO", "origem": self.id, "erro": f"operação desconhecida: {op}"}
        try:
            valor = round(float(msg["valor"]), 2)
            if valor <= 0:
                raise ValueError
        except (KeyError, TypeError, ValueError):
            return {"tipo": "ERRO", "origem": self.id, "erro": "valor inválido"}

        with self.trava_ordem:                   # uma ordem de cada vez (FIFO)
            req_id = msg.get("req_id")
            with self.trava:
                anterior = self.estado["executados"].get(req_id)
            if anterior is not None:
                # Pedido repetido (o cliente reenviou após uma falha): não executa de novo
                self.log(f"PEDIDO {req_id} já executado -> devolve o resultado guardado",
                         msg="RESPOSTA", para=["C"])
                return {"tipo": "RESPOSTA", "origem": self.id, "op": op, "conta": conta,
                        "repetido": True, **anterior}

            # O LÍDER decide a ordem e os valores não determinísticos
            ordem = {"seq": self.estado["seq"] + 1, "req_id": req_id, "op": op,
                     "conta": conta, "valor": valor,
                     "data_hora": datetime.now().strftime("%d/%m/%Y %H:%M:%S.%f")[:-3],
                     "transacao": f"T{self.estado['seq'] + 1:05d}-R{self.id}"}
            res = self.aplicar(ordem)
            seguidores = [r for r in self.vista if r != self.id]
            self.log(f"PEDIDO {op} {valor} conta {conta} -> executa (seq={ordem['seq']}) "
                     f"e difunde ORDEM para {seguidores}", msg="ORDEM", para=seguidores)
            self.difundir_ordem(ordem, seguidores)
            self.seq_confirmado = ordem["seq"]

        self.log(f"responde ao cliente: {'ok' if res['ok'] else res['erro']}, "
                 f"saldo {res['saldo']}", msg="RESPOSTA", para=["C"])
        return {"tipo": "RESPOSTA", "origem": self.id, "op": op, "conta": conta, **res}

    def difundir_ordem(self, ordem, seguidores):
        """Envia a ORDEM a cada seguidor e espera o ACK. Um seguidor atrasado
        (FALTA) recebe o estado completo; um que não responde é excluído da vista."""
        def para_um(r):
            resposta = self.enviar(r, "ORDEM", ordem=ordem, vista=self.vista,
                                   num_vista=self.num_vista)
            if resposta and resposta.get("tipo") == "FALTA":
                self.log(f"R{r} está atrasado (seq={resposta['seq']}) -> envia SET_STATE",
                         msg="ESTADO", para=[r])
                resposta = self.enviar(r, "SET_STATE", estado=self.get_state(),
                                       vista=self.vista, num_vista=self.num_vista)
            return resposta is not None and resposta.get("tipo") == "ACK"

        resultados = self.em_paralelo(seguidores, para_um)
        falhos = [r for r, ok in resultados.items() if not ok]
        if falhos:
            self.excluir_da_vista(falhos)

    # ------------------------------------------------ gerenciamento de membros
    def excluir_da_vista(self, falhos):
        with self.trava:
            antes = list(self.vista)
            self.vista = [r for r in self.vista if r not in falhos]
            if self.vista == antes:
                return
            self.num_vista += 1
        self.log(f"réplica(s) {', '.join('R%d' % r for r in falhos)} não responderam -> excluídas da vista",
                 msg="FALHA", para=falhos)
        self.difundir_vista()

    def difundir_vista(self):
        outros = [r for r in self.vista if r != self.id]
        self.log(f"nova VISTA #{self.num_vista}: {self.vista} (líder R{self.lider})",
                 msg="VISTA", para=outros)
        for r in outros:
            threading.Thread(target=self.enviar, args=(r, "VISTA"),
                             kwargs={"vista": self.vista, "num_vista": self.num_vista},
                             daemon=True).start()

    def adotar_vista(self, vista, num_vista, origem):
        """Seguidor: adota a vista enviada pelo líder se for mais recente."""
        with self.trava:
            if num_vista < self.num_vista and origem != self.lider:
                return False
            mudou = vista != self.vista
            self.vista, self.num_vista = list(vista), num_vista
        if mudou:
            self.log(f"adota VISTA #{num_vista}: {vista} (líder R{vista[0]})")
        if self.id not in vista and self.ativo and not self.juntando:
            self.log("não estou na vista -> volto a juntar-me ao grupo")
            threading.Thread(target=self.juntar, daemon=True).start()
        return True

    # ------------------------------------------------ troca de líder
    def assumir_lideranca(self):
        """Seguidor que passa a ser o primeiro da vista: sincroniza o estado com
        os outros (pode haver alguém com uma ordem a mais) e anuncia a vista."""
        self.log("o líder falhou e sou o próximo da vista -> ASSUMO A LIDERANÇA")
        outros = [r for r in self.vista if r != self.id]
        status = self.em_paralelo(outros, lambda r: self.enviar(r, "STATUS"))
        vivos = [r for r, s in status.items() if s and s.get("ativo")]
        mortos = [r for r in outros if r not in vivos]
        with self.trava:
            self.vista = [r for r in self.vista if r not in mortos]
            self.num_vista += 1
        mais_adiantado = max(vivos, key=lambda r: status[r]["seq"], default=None)
        if mais_adiantado and status[mais_adiantado]["seq"] > self.estado["seq"]:
            r = self.enviar(mais_adiantado, "PEDIR_ESTADO")
            if r and r.get("tipo") == "ESTADO":
                self.set_state(r["estado"], mais_adiantado)
        self.seq_confirmado = self.estado["seq"]
        self.difundir_vista()

    # ------------------------------------------------ entrar / voltar ao grupo
    def juntar(self):
        """Ao arrancar ou voltar de uma falha: encontra o líder e pede o estado
        (get_state); se não houver grupo, a réplica de menor número cria-o."""
        with self.trava:
            if self.juntando:
                return
            self.juntando = True
        try:
            while self.ativo:
                outros = [r for r in self.replicas if r != self.id]
                status = self.em_paralelo(outros, lambda r: self.enviar(r, "STATUS"))
                vivos = [r for r, s in status.items() if s and s.get("ativo")]
                lideres = [s for s in status.values()
                           if s and s.get("ativo") and s.get("lider") is not None]
                if any(s["lider"] == self.id for s in lideres):
                    # Os outros ainda me têm como líder (voltei antes de detectarem
                    # a minha falha): espero que elejam o novo líder.
                    time.sleep(1.0)
                    continue
                if lideres:
                    lider = max(lideres, key=lambda s: s["num_vista"])["lider"]
                    self.log(f"pede para entrar no grupo ao líder R{lider} (JUNTAR / get_state)",
                             msg="JUNTAR", para=[lider])
                    r = self.enviar(lider, "JUNTAR")
                    if r and r.get("tipo") == "ESTADO":
                        with self.trava:
                            self.estado = estado_vazio()
                        self.set_state(r["estado"], lider)
                        self.adotar_vista(r["vista"], r["num_vista"], lider)
                        self.ultimo_heartbeat = time.time()
                        return
                elif self.id == min(vivos + [self.id]):
                    with self.trava:
                        self.vista, self.num_vista = [self.id], self.num_vista + 1
                    self.seq_confirmado = self.estado["seq"]
                    self.log("nenhum grupo ativo -> crio o grupo e sou o LÍDER")
                    return
                time.sleep(1.0)
        finally:
            self.juntando = False

    # ------------------------------------------------ recepção de mensagens
    def tratar_mensagem(self, msg):
        tipo, origem = msg.get("tipo"), msg.get("origem")

        if not self.ativo and tipo not in ("STATUS", "EVENTOS", "RECUPERAR"):
            return None                                   # réplica "caída"

        if tipo == "PEDIDO":
            if self.sou_lider():
                return self.processar_pedido(msg)
            # Só o líder recebe invocações de clientes
            return {"tipo": "REDIRECIONA", "origem": self.id, "lider": self.lider}

        if tipo == "ORDEM":                               # seguidor
            if not self.adotar_vista(msg["vista"], msg["num_vista"], origem):
                return {"tipo": "ERRO", "origem": self.id, "erro": "vista desatualizada"}
            self.ultimo_heartbeat = time.time()
            ordem = msg["ordem"]
            with self.trava:
                atual = self.estado["seq"]
                if ordem["seq"] <= atual:                 # repetida: já aplicada
                    return {"tipo": "ACK", "origem": self.id, "seq": atual}
                if ordem["seq"] > atual + 1:              # falta alguma: pede estado
                    self.log(f"ORDEM seq={ordem['seq']} fora de ordem (tenho {atual}) -> FALTA")
                    return {"tipo": "FALTA", "origem": self.id, "seq": atual}
                res = self.aplicar(ordem)
            self.log(f"ORDEM seq={ordem['seq']} de R{origem}: {ordem['op']} {ordem['valor']} "
                     f"conta {ordem['conta']} -> executa (saldo {res['saldo']}), ACK",
                     msg="ACK", para=[origem])
            return {"tipo": "ACK", "origem": self.id, "seq": ordem["seq"]}

        if tipo == "SET_STATE":
            self.set_state(msg["estado"], origem)
            self.adotar_vista(msg["vista"], msg["num_vista"], origem)
            return {"tipo": "ACK", "origem": self.id, "seq": self.estado["seq"]}

        if tipo == "HEARTBEAT":
            self.ultimo_heartbeat = time.time()
            self.adotar_vista(msg["vista"], msg["num_vista"], origem)
            if msg["seq"] > self.estado["seq"] and self.id in msg["vista"]:
                threading.Thread(target=self.atualizar_estado, args=(origem,),
                                 daemon=True).start()
            return {"tipo": "ACK", "origem": self.id, "seq": self.estado["seq"]}

        if tipo == "VISTA":
            self.ultimo_heartbeat = time.time()
            self.adotar_vista(msg["vista"], msg["num_vista"], origem)
            return {"tipo": "ACK", "origem": self.id}

        if tipo == "JUNTAR":                              # líder
            if not self.sou_lider():
                return {"tipo": "REDIRECIONA", "origem": self.id, "lider": self.lider}
            with self.trava_ordem:                        # estado consistente
                with self.trava:
                    if origem not in self.vista:
                        self.vista.append(origem)
                        self.num_vista += 1
                    estado = self.get_state()
                    vista, num = list(self.vista), self.num_vista
            self.log(f"R{origem} entrou no grupo -> envia o estado (get_state, seq={estado['seq']})",
                     msg="ESTADO", para=[origem])
            threading.Thread(target=self.difundir_vista, daemon=True).start()
            return {"tipo": "ESTADO", "origem": self.id, "estado": estado,
                    "vista": vista, "num_vista": num}

        if tipo == "PEDIR_ESTADO":
            self.log(f"R{origem} pediu o estado -> get_state (seq={self.estado['seq']})",
                     msg="ESTADO", para=[origem])
            return {"tipo": "ESTADO", "origem": self.id, "estado": self.get_state(),
                    "vista": self.vista, "num_vista": self.num_vista}

        if tipo == "STATUS":
            with self.trava:
                return {"tipo": "STATUS", "origem": self.id, "ativo": self.ativo,
                        "lider": self.lider if self.ativo else None,
                        "vista": self.vista, "num_vista": self.num_vista,
                        "seq": self.estado["seq"], "contas": dict(self.estado["contas"]),
                        "n_transacoes": len(self.estado["extrato"])}

        if tipo == "EVENTOS":
            desde = msg.get("desde", 0)
            with self.trava_eventos:
                novos = [e for e in self.eventos if e["seq"] > desde]
            return {"tipo": "EVENTOS", "origem": self.id, "eventos": novos,
                    "ultimo": self.seq_evento}

        if tipo == "FALHAR":
            self.log("*** FALHA SIMULADA: a réplica pára e perde o estado em memória ***")
            self.ativo = False
            with self.trava:
                self.vista, self.estado = [], estado_vazio()
            return {"tipo": "ACK", "origem": self.id}

        if tipo == "RECUPERAR":
            if not self.ativo:
                self.ativo = True
                self.log("*** RECUPERADA: volta ao grupo com transferência de estado ***")
                threading.Thread(target=self.juntar, daemon=True).start()
            return {"tipo": "ACK", "origem": self.id}

        return {"tipo": "ERRO", "origem": self.id, "erro": f"tipo desconhecido: {tipo}"}

    def atualizar_estado(self, lider):
        r = self.enviar(lider, "PEDIR_ESTADO")
        if r and r.get("tipo") == "ESTADO":
            self.set_state(r["estado"], lider)

    # ------------------------------------------------ keepalives / timeouts
    def laco_heartbeat(self):
        while True:
            time.sleep(self.t_heartbeat)
            if not self.ativo or self.juntando:
                continue

            if self.sou_lider():
                # Líder: keepalive para os seguidores; quem não responde sai da vista
                seguidores = [r for r in self.vista if r != self.id]
                res = self.em_paralelo(seguidores, lambda r: self.enviar(
                    r, "HEARTBEAT", vista=self.vista, num_vista=self.num_vista,
                    seq=self.seq_confirmado))
                falhos = [r for r, x in res.items() if x is None]
                if falhos:
                    self.excluir_da_vista(falhos)

            elif self.vista:
                # Seguidor: sem notícias do líder há muito tempo -> líder suspeito
                if time.time() - self.ultimo_heartbeat > self.t_suspeita:
                    antigo = self.lider
                    self.log(f"sem HEARTBEAT do líder R{antigo} há {self.t_suspeita}s "
                             "-> líder considerado falho", msg="FALHA", para=[antigo])
                    with self.trava:
                        self.vista = [r for r in self.vista if r != antigo]
                    self.ultimo_heartbeat = time.time()
                    if self.lider == self.id:
                        self.assumir_lideranca()
                    elif self.lider is None:
                        threading.Thread(target=self.juntar, daemon=True).start()

            else:
                threading.Thread(target=self.juntar, daemon=True).start()

    # ------------------------------------------------ arranque
    def executar(self):
        host, porta = self.replicas[self.id]
        servidor = ServidorTCP((host, porta), TratadorConexao)
        servidor.replica = self
        threading.Thread(target=servidor.serve_forever, daemon=True).start()
        self.log(f"iniciada em {host}:{porta}; réplicas: {sorted(self.replicas)}")
        threading.Thread(target=self.juntar, daemon=True).start()
        try:
            self.laco_heartbeat()
        except KeyboardInterrupt:
            self.log("encerrada")
            servidor.shutdown()


def main():
    parser = argparse.ArgumentParser(description="Réplica com replicação semi-ativa")
    parser.add_argument("--id", type=int, required=True, help="número da réplica")
    parser.add_argument("--config", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json"))
    args = parser.parse_args()
    with open(args.config, encoding="utf-8") as f:
        config = json.load(f)
    if str(args.id) not in config["replicas"]:
        parser.error(f"a réplica {args.id} não está em {args.config}")
    Replica(args.id, config).executar()


if __name__ == "__main__":
    main()