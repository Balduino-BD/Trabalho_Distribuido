


# Sobe as 3 réplicas em segundo plano; logs em logs/R<id>.log
# Uso: ./iniciar_replicas.sh        (para parar: ./parar_replicas.sh)
cd "$(dirname "$0")"
mkdir -p logs
for id in 1 2 3; do
  python3 -u replica.py --id "$id" > "logs/R$id.log" 2>&1 &
  echo $! > "logs/R$id.pid"
  echo "R$id iniciada (PID $!) -> logs/R$id.log"
  sleep 0.5   # R1 arranca primeiro e cria o grupo (fica líder)
done
