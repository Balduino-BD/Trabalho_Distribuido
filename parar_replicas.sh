


# Encerra as réplicas iniciadas por iniciar_replicas.sh
cd "$(dirname "$0")"
for f in logs/R*.pid; do
  [ -f "$f" ] && kill "$(cat "$f")" 2>/dev/null && rm "$f"
done
echo "réplicas encerradas"
