# Burzovni terminal

Live burzovni terminal (tamna tema u stilu trading terminala): watchlista od 12 dionica s cijenama uživo, sparklineovima i
bljeskanjem promjena, panel „Detalji“ za svaku dionicu (graf 1D/5D/1M/6M/1G, fundamenti, zadnji kvartalni izvještaj,
vijesti, bull/bear), simulirani portfelj (paper trading, 10.000 $ virtualno) s P&L-om i udaljenošću do SL/TP te tjedna
statistika (win rate, zatvoreni trejdovi).

**Simulacija – nije financijski savjet.** Podaci: Yahoo Finance (mogu kasniti).

## Kako radi
- `server.py` – mali Python server (samo standardna biblioteka). Dohvaća podatke s Yahoo Financea (cookie + crumb),
  kešira ih 12 s – 15 min i poslužuje `static/index.html`.
- API: `/api/state`, `/api/chart?s=MSFT&r=1d|5d|1mo|6mo|1y`, `/api/detail?s=MSFT`, `/health`.
- Stranica osvježava podatke svakih 12 s dok je burza otvorena, inače svake minute.
- `portfolio.json` – watchlista, trejdovi, gotovina. Na Renderu se čita s `PORTFOLIO_URL` (raw GitHub link na ovaj
  fajl) i osvježava svakih 10 min; ako URL nije postavljen ili ne radi, koristi se kopija iz repozitorija.
- Kad cijena dotakne SL ili TP, server poziciju označi zatvorenom. Na Renderu (`PERSIST=0`) je to samo prikaz u
  memoriji („privremeno“); službeno zatvaranje radi tjedna rutina koja commita `portfolio.json` u repo.
- `info.json` – hrvatski opisi, sažeci izvještaja i bull/bear po dionici.

## Pokretanje lokalno
```
PORT=9000 python3 server.py          # zatim otvori http://localhost:9000
```
`start.sh` je skripta za vlastito računalo/box: pokreće server i Cloudflare quick tunnel u petljama s restartom
(`bash start.sh`, `bash start.sh stop`, `TUNNEL=0 bash start.sh` bez tunela).

## Render (besplatno)
Blueprint je u `render.yaml`. Ručno: New → Web Service → ovaj repo, Runtime Python,
Build `pip install -r requirements.txt`, Start `python3 server.py`, Health check `/health`, plan Free.
Varijable: `PERSIST=0`, `PORTFOLIO_URL=https://raw.githubusercontent.com/<korisnik>/<repo>/main/portfolio.json`.
