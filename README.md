# Deploying the Deriv bot to Railway

## 1. Supabase (state persistence — Railway has no persistent disk)

1. Create a free project at supabase.com.
2. Open **SQL Editor** → paste and run the SQL block from the top of `bot.py`
   (the docstring around line 158) — it creates `bot_trade_log`,
   `bot_symbol_state`, `bot_global_state`, `bot_gate_config`.
3. Go to **Project Settings → API** and copy:
   - `Project URL` → this is `SUPABASE_URL`
   - `service_role` key (not the anon key) → this is `SUPABASE_KEY`

## 2. Deriv app + token

1. Go to developers.deriv.com, log in, register a **new** app to get an
   `app_id`. Old/legacy app IDs (e.g. `1089`) will not work — the bot uses
   the newer REST OTP bootstrap flow.
2. Go to your Deriv account → API Token → create a token with trading
   scopes. This is `DERIV_API_TOKEN`.

## 3. Push this folder to a GitHub repo

Railway deploys from a repo (or you can use the Railway CLI to deploy a
local folder directly — see step 5b).

```
git init
git add .
git commit -m "deriv bot"
git branch -M main
git remote add origin <your-repo-url>
git push -u origin main
```

## 4. Create the Railway project

1. railway.app → New Project → Deploy from GitHub repo → pick this repo.
2. Railway will detect Python via `runtime.txt` and install
   `requirements.txt` automatically (Nixpacks).
3. Under the service's **Settings → Deploy**, confirm the start command
   picked up the `Procfile` (`python bot.py`). If not, set it manually.
4. Since this is a background worker (no web server, no port binding),
   do **not** generate a public domain for it.

## 5a. Set environment variables

In the Railway service → **Variables**, add:

| Key | Value |
|---|---|
| `DERIV_APP_ID` | your new app id |
| `DERIV_API_TOKEN` | your API token |
| `DERIV_ACCOUNT_TYPE` | `demo` (leave as demo until you've watched it run) |
| `SUPABASE_URL` | from Supabase |
| `SUPABASE_KEY` | service_role key from Supabase |
| `VERBOSE_LOGS` | `1` while you're checking it over, `0` once stable |

`DERIV_ACCOUNT_ID` is optional — only add it if you want to skip the
account lookup and pin a specific account.

## 5b. (Alternative) Deploy without GitHub, via CLI

```
npm i -g @railway/cli
railway login
railway init
railway up
```
Then set the same variables with `railway variables set KEY=value` or in
the dashboard.

## 6. Watch the first run

Railway → your service → **Logs**. On a clean deploy you should see the
startup calibration run across R_75, R_100, and RDBEAR before any trade
signal is printed — that first calibration pass can take a few minutes.

## Before you flip `DERIV_ACCOUNT_TYPE` to `real`

- This script places live trades with martingale recovery staking.
  Confirm the demo run behaves the way you expect over a real stretch of
  time — win rate, stake sizing, and how it handles losing sequences —
  before pointing it at real money.
- Re-check `MAX_SEQUENCE_LOSS_PCT`, `MARTINGALE_MAX_STEPS`, and
  `MARTINGALE_FACTOR` in the config section against how much drawdown
  you're actually willing to accept.
