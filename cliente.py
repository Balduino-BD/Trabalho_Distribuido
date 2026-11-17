#!/usr/bin/env python3
"""
Cliente do serviço bancário replicado (replicação semi-ativa).

O cliente só fala com o LÍDER. Se contactar um seguidor, recebe REDIRECIONA
com o número do líder; se o líder não responder (falhou), tenta as outras
réplicas até um novo líder assumir. Cada pedido tem um identificador único
(req_id): se for reenviado depois de uma falha, não é executado duas vezes.

Uso:
    python cliente.py depositar A 100
    python cliente.py sacar A 30
    python cliente.py saldo A
    python cliente.py extrato A
    python cliente.py todos                 (estado de todas as réplicas)
    python cliente.py falhar 1              (simula a falha da réplica 1)
    python cliente.py recuperar 1
    python cliente.py                       (modo interativo)
"""

import argparse
import json
import os
import time
import uuid

from replica import enviar_mensagem


class ClienteBanco:
    def __init__(self, config):
        self.replicas = {int(r): (d["host"], d["porta"]) for r, d in config["replicas"].items()}
        self.lider = min(self.replicas)          # palpite inicial
        self.id_cliente = uuid.uuid4().hex[:6]
        self.contador = 0

    def enviar(self, rid, mensagem, timeout=3.0):
        host, porta = self.replicas[rid]
        return enviar_mensagem(host, porta, mensagem, timeout)

    def operacao(self, op, conta, valor=None, limite=12.0):
        """Envia o pedido ao líder, seguindo redirecionamentos e tentando outras
        réplicas enquanto o líder não responder. Devolve (resposta, tentativas)."""
        self.contador += 1
        pedido = {"tipo": "PEDIDO", "origem": f"cliente-{self.id_cliente}",
                  "req_id": f"{self.id_cliente}-{self.contador}",
                  "op": op, "conta": conta, "valor": valor}
        tentativas = []
        inicio = time.time()
        fim = inicio + limite
        while time.time() < fim:
            candidatos = [self.lider] + [r for r in sorted(self.replicas) if r != self.lider]
            for rid in candidatos:
                r = self.enviar(rid, pedido)
                if r is None:
                    tentativas.append(f"R{rid}: sem resposta")
                    continue
                if r["tipo"] == "REDIRECIONA":
                    tentativas.append(f"R{rid}: não sou o líder -> R{r['lider']}")
                    if r["lider"] is not None and r["lider"] != rid:
                        r2 = self.enviar(r["lider"], pedido)
                        if r2 is not None and r2["tipo"] in ("RESPOSTA", "ERRO"):
                            self.lider = r["lider"]
                            return r2, self.resumo(tentativas, inicio)
                        tentativas.append(f"R{r['lider']}: sem resposta")
                    continue
                self.lider = rid
                return r, self.resumo(tentativas, inicio)
            time.sleep(0.7)             # provavelmente há uma troca de líder em curso
        return {"tipo": "ERRO", "erro": "nenhum líder respondeu"}, self.resumo(tentativas, inicio)

    @staticmethod
    def resumo(tentativas, inicio):
        """Tentativas sem repetições, mais o tempo total (para mostrar ao usuário)."""
        unicas = list(dict.fromkeys(tentativas))
        if unicas:
            unicas.append(f"resposta obtida após {time.time() - inicio:.1f} s")
        return unicas

    def admin(self, rid, tipo):
        return self.enviar(rid, {"tipo": tipo, "origem": "cliente"}, timeout=1.0)


def mostrar_resposta(r, tentativas):
    for t in tentativas:
        print(f"  ({t})")
    if r["tipo"] == "ERRO":
        print(f"ERRO: {r['erro']}")
        return
    extra = " [pedido repetido: não foi executado de novo]" if r.get("repetido") else ""
    if r["op"] in ("depositar", "sacar"):
        estado = "OK" if r["ok"] else f"RECUSADO ({r['erro']})"
        print(f"{r['op']} conta {r['conta']}: {estado} | saldo = {r['saldo']:.2f} | "
              f"transação {r['transacao']} em {r['data_hora']} | respondido pelo líder "
              f"R{r['origem']}{extra}")
    elif r["op"] == "saldo":
        print(f"Saldo da conta {r['conta']} = {r['saldo']:.2f} (líder R{r['origem']})")
    else:
        print(f"Extrato da conta {r['conta']} (líder R{r['origem']}):")
        for e in r["extrato"]:
            print(f"  {e['transacao']}  {e['data_hora']}  {e['op']:<9} "
                  f"{e['valor']:>10.2f}  saldo {e['saldo']:>10.2f}")
        print(f"  Saldo atual: {r['saldo']:.2f}")


def mostrar_todos(cli):
    for rid in sorted(cli.replicas):
        s = cli.admin(rid, "STATUS")
        if s is None:
            print(f"R{rid}: desligada")
        elif not s["ativo"]:
            print(f"R{rid}: CAÍDA (simulada)")
        else:
            papel = "LÍDER" if s["lider"] == rid else "seguidor"
            contas = ", ".join(f"{c}={v:.2f}" for c, v in sorted(s["contas"].items())) or "-"
            print(f"R{rid}: {papel:<8} | vista #{s['num_vista']} {s['vista']} | "
                  f"seq={s['seq']} | contas: {contas}")


def executar(cli, partes):
    cmd = partes[0]
    if cmd in ("depositar", "sacar") and len(partes) == 3:
        mostrar_resposta(*cli.operacao(cmd, partes[1], float(partes[2])))
    elif cmd in ("saldo", "extrato") and len(partes) == 2:
        mostrar_resposta(*cli.operacao(cmd, partes[1]))
    elif cmd == "todos":
        mostrar_todos(cli)
    elif cmd in ("falhar", "recuperar") and len(partes) == 2:
        r = cli.admin(int(partes[1]), cmd.upper())
        print(f"R{partes[1]}: {'comando enviado' if r else 'sem resposta'}")
    else:
        print("comandos: depositar <conta> <valor> | sacar <conta> <valor> | saldo <conta> | "
              "extrato <conta> | todos | falhar <id> | recuperar <id> | sair")


def main():
    parser = argparse.ArgumentParser(description="Cliente do banco replicado")
    parser.add_argument("--config", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json"))
    parser.add_argument("comando", nargs="*")
    args = parser.parse_args()
    with open(args.config, encoding="utf-8") as f:
        cli = ClienteBanco(json.load(f))

    if args.comando:
        executar(cli, args.comando)
        return
    print("Cliente do banco replicado. Digite 'ajuda' ou 'sair'.")
    while True:
        try:
            partes = input("> ").strip().split()
        except (EOFError, KeyboardInterrupt):
            break
        if not partes:
            continue
        if partes[0] == "sair":
            break
        try:
            executar(cli, partes)
        except ValueError:
            print("valor inválido")


if __name__ == "__main__":
    main()