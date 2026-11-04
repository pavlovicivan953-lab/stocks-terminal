#!/usr/bin/env bash
# Pokrece backend + Cloudflare quick tunnel u pozadini, svaki u petlji koja ga ponovno pokrene ako padne.
# Upotreba: bash start.sh   (TUNNEL=0 bash start.sh = bez tunela)
#                 (pokreni / restartaj sve)
#           bash start.sh stop   (zaustavi sve)
APP="$(cd "$(dirname "$0")" && pwd)"; PORT="${PORT:-8787}"; LOGS="$APP/logs"; mkdir -p "$LOGS"
stop() {
  for f in "$LOGS"/*.pid; do [ -f "$f" ] && kill -- -"$(cat "$f")" 2>/dev/null; rm -f "$f"; done
  pkill -f "$APP/server.py" 2>/dev/null; pkill -f "cloudflared tunnel --no-autoupdate --url http://localhost:$PORT" 2>/dev/null; sleep 1
}
CF="${CLOUDFLARED:-/home/box/.local/bin/cloudflared}"
if [ ! -x "$CF" ]; then mkdir -p "$(dirname "$CF")"; curl -sL -o "$CF" https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64 && chmod +x "$CF"; fi
stop
[ "$1" = "stop" ] && { echo "zaustavljeno"; exit 0; }

setsid bash -c "while true; do PORT=$PORT python3 '$APP/server.py' >> '$LOGS/server.log' 2>&1; echo \"\$(date) server pao, restart za 3s\" >> '$LOGS/server.log'; sleep 3; done" >/dev/null 2>&1 < /dev/null &
echo $! > "$LOGS/server.pid"

if [ "${TUNNEL:-1}" = "1" ]; then
setsid bash -c "while true; do '$CF' tunnel --no-autoupdate --url http://localhost:$PORT > '$LOGS/tunnel.log' 2>&1; echo \"\$(date) tunel pao, restart za 5s\" >> '$LOGS/tunnel-restarts.log'; sleep 5; done" >/dev/null 2>&1 < /dev/null &
echo $! > "$LOGS/tunnel.pid"
fi

# pricekaj URL tunela
for i in $(seq 1 40); do
  URL=$(grep -o 'https://[a-z0-9-]*\.trycloudflare\.com' "$LOGS/tunnel.log" 2>/dev/null | head -1)
  [ -n "$URL" ] && break; sleep 1
done
echo "$URL" > "$APP/tunnel_url.txt"
echo "Lokalno: http://localhost:$PORT"
echo "Javni URL: ${URL:-nije jos dostupan, pogledaj $LOGS/tunnel.log}"
