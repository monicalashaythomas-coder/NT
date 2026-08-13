"""
DERIV RISE/FALL BOT — 1HZ100V
================================
Symbol  : 1HZ100V  (Volatility 100 Index — 1-second feed)
Contract: CALL (Rise) / PUT (Fall)
Duration: 1–5 ticks, EV-optimised per session

Signal model — 8-layer combined score system:

  MODEL 1 — Bernoulli Bias
    · Sliding window p̂ = up_ticks / N over last 100 ticks
    · p̂ > 0.52 → Rise bias | p̂ < 0.48 → Fall bias

  MODEL 2 — Markov Chain
    · Tracks P(U→U) and P(D→D) from transition matrix
    · Either > 0.55 → trade continuation

  MODEL 3 — Multi-Step Conditional (3-tick pattern)
    · P(Rise | last 3 ticks) or P(Fall | last 3 ticks)
    · Only fires when pattern seen ≥ 30 times and p > 0.60

  MODEL 4 — Momentum
    · M = sum of last 5 ticks (+1 up, −1 down)
    · M ≥ +3 → Rise  |  M ≤ −3 → Fall

  MODEL 5 — Volatility Filter (gate)
    · Blocks trade if σ too low (dead) or too high (chaotic)
    · Sigma computed on RETURN-SCALE (scale-invariant across symbols)

  MODEL 6 — EV-based Expiry Selection (historical baseline)
    · Tracks win rate per expiry (1–5 ticks) in-session
    · Beta-shrunk toward true breakeven; used as prior for MODEL 6b

  MODEL 6b — Monte Carlo Duration Selection
    · Estimates local drift (µ) and sigma from recent 50-tick returns
    · Simulates N forward paths per candidate duration using GBM
    · Picks duration maximising EV on THIS tick's conditions
    · Blended with MODEL 6 historical win rate via Beta shrinkage

  MODEL 7 — Combined Weighted Score
    · S = 0.30·bias + 0.40·markov + 0.30·momentum
    · |S| > 0.60 required to place trade
    · Direction resolved first, then MC picks optimal duration

Martingale:
    1.5× multiplier — resets after max_losses or on any win

Circuit Breaker:
    3 consecutive losses → 10-minute pause
"""

import asyncio
import json
import math
import os
import sys
import time
import urllib.error
import urllib.request
from collections import deque, defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional

try:
    import websockets
    from websockets.exceptions import (
        ConnectionClosed, ConnectionClosedError, ConnectionClosedOK,
    )
except ImportError:
    sys.exit("websockets not installed — run: pip install websockets")

try:
    import requests
except ImportError:
    requests = None   # persistence is optional — bot runs fine without it


# ============================================================================
# CONFIGURATION
# ============================================================================

def _env(key: str, default):
    val = os.environ.get(key)
    if val is None:
        return default
    if isinstance(default, bool):
        return val.lower() in ("1", "true", "yes")
    if isinstance(default, float):
        return float(val)
    if isinstance(default, int):
        return int(val)
    return val


CONFIG = {
    # ── Deriv credentials ──────────────────────────────────────
    # No hardcoded default — a real token must never live in source. Set
    # DERIV_API_TOKEN in the environment. (If a token was ever hardcoded
    # here and this file has been shared/committed anywhere, treat that
    # token as compromised and revoke/rotate it from your Deriv account
    # settings immediately, regardless of this fix.)
    "api_token":        _env("DERIV_API_TOKEN", ""),
    # No default app_id. 1089 is an old shared/legacy app ID that does not
    # reliably work with current API behaviour — register a fresh one at
    # https://developers.deriv.com and set DERIV_APP_ID explicitly. Missing
    # this is treated the same as a missing token: bot refuses to start
    # rather than silently running against a legacy ID.
    # (Previously this line read _env("33yLH5BDgaA4vcRK3qwY6", ...) — the
    # first arg to _env is supposed to be the ENV VAR NAME, not the app id
    # value, so DERIV_APP_ID was never actually being read from the
    # environment. Fixed below.)
    "app_id":           _env("DERIV_APP_ID", ""),
    # New Options API auth flow: WebSocket connections are authenticated
    # via a short-lived OTP obtained over REST, using an account_id
    # (e.g. "CR1234567" or "VRTC1234567"), not a raw app_id query param.
    # Leave blank to auto-resolve the account via GET /accounts (picks a
    # demo account unless DERIV_USE_REAL is set); set explicitly to pin it.
    "account_id":       _env("DERIV_ACCOUNT_ID", ""),
    "use_real_account": _env("DERIV_USE_REAL", False),

    # ── Contract parameters ────────────────────────────────────
    "symbol":           _env("SYMBOL",   "R_75"),
    "currency":         "USD",

    # ── Model thresholds ──────────────────────────────────────
    "window_size":      _env("WINDOW_SIZE",     100),   # ticks for bias/markov
    "min_window":       _env("MIN_WINDOW",       50),   # warmup
    "bias_rise":        _env("BIAS_RISE",      0.52),
    "bias_fall":        _env("BIAS_FALL",      0.48),
    "markov_thresh":    _env("MARKOV_THRESH",  0.55),
    "momentum_window":  _env("MOM_WINDOW",        5),
    "momentum_thresh":  _env("MOM_THRESH",        3),
    "cond_prob_thresh": _env("COND_THRESH",    0.60),
    "cond_min_samples": _env("COND_SAMPLES",     30),
    "combined_thresh":  _env("COMBINED_THRESH",0.60),

    # ── Combined score weights (must sum to 1.0) ───────────────
    "w_bias":           _env("W_BIAS",     0.30),
    "w_markov":         _env("W_MARKOV",   0.40),
    "w_momentum":       _env("W_MOM",      0.30),

    # ── Volatility filter ─────────────────────────────────────
    # Bounds are now in RETURN-SCALE (relative tick moves), not
    # absolute price-unit scale — see _vol_ok() fix above.
    # Sized for typical synthetic index tick behaviour (R_75, 1HZ100V):
    # return-sigma typically runs 0.00001–0.00005 on these instruments.
    "vol_min":          _env("VOL_MIN", 0.000002),
    "vol_max":          _env("VOL_MAX", 0.000200),

    # ── Expiry optimiser ──────────────────────────────────────
    "expiry_options":   [1, 2, 3, 4, 5],   # ticks
    # min_expiry_ev was 0.0 by default — any EV above exactly breakeven
    # passed, and before min_expiry_hist trades existed for an expiry there
    # was NO gate at all. Now expressed as a required margin ABOVE
    # breakeven (real breakeven computed from the live-tracked
    # payout_ratio, not assumed), and a Beta-shrinkage prior fills in for
    # thin/no history instead of leaving the gate wide open. See
    # SignalEngine.best_expiry() and ev_prior_strength below.
    "min_ev_margin":    _env("MIN_EV_MARGIN",  0.02),  # required edge over breakeven
    "min_expiry_hist":  _env("MIN_EV_HIST",      10),  # trades before history dominates the prior
    "ev_prior_strength": _env("EV_PRIOR_STRENGTH", 10),  # Beta-shrinkage weight (pseudo-trades)

    # ── Monte Carlo duration selection ────────────────────────────
    # Number of forward-path simulations per candidate expiry. Higher =
    # more accurate p(win) estimate but more CPU per evaluation cycle.
    # 500 is a good balance — fast enough to not noticeably delay ticks,
    # accurate enough that sampling noise is ~2% on a 50% base rate.
    "mc_sims":          _env("MC_SIMS", 500),

    # ── Risk / Martingale ──────────────────────────────────────
    "initial_stake":    _env("INITIAL_STAKE",  1.00),
    "martingale_mul":   _env("MARTINGALE_MUL", 1.50),
    "max_losses":       _env("MAX_LOSSES",        5),
    # target_profit / stop_loss: if the _pct variants are > 0, they take
    # precedence and are evaluated against CURRENT balance (so they scale
    # with account size); the flat $ versions remain as a fallback for
    # backward compatibility if left at their defaults with _pct at 0.
    "target_profit":     _env("TARGET_PROFIT",  10.0),
    "stop_loss":          _env("STOP_LOSS",      20.0),
    "target_profit_pct": _env("TARGET_PROFIT_PCT", 0.0),
    "stop_loss_pct":      _env("STOP_LOSS_PCT",     0.0),
    # Sequence-level martingale guard — caps total stake committed in ONE
    # losing sequence to this fraction of the balance AT SEQUENCE START
    # (a fixed snapshot, captured once when the sequence begins — NOT
    # recomputed against the live, shrinking balance on every step; that
    # version mechanically tightens every step and can make a fixed
    # percentage unable to unlock more than one recovery step regardless
    # of its value). 0 disables the guard.
    "max_sequence_loss_pct": _env("MAX_SEQUENCE_LOSS_PCT", 0.25),

    # ── Circuit breaker ───────────────────────────────────────
    "consec_loss_limit": _env("CONSEC_LOSS_LIMIT",    3),
    "consec_pause_secs": _env("CONSEC_PAUSE_SECS",  600),

    # ── Trade pacing (skip ticks between evals) ───────────────
    "eval_every_ticks":  _env("EVAL_EVERY",     3),

    # ── Resilience ────────────────────────────────────────────
    "lock_timeout":         _env("LOCK_TIMEOUT",      30),   # ticks expiry + buffer
    "buy_recv_retries":     _env("BUY_RETRIES",        8),
    "reconnect_delay_min":  _env("RECONNECT_MIN",       2),
    "reconnect_delay_max":  _env("RECONNECT_MAX",      60),
    "ws_ping_interval":     _env("WS_PING",            30),
    "orphan_poll_attempts": _env("ORPHAN_ATTEMPTS",     4),
    "orphan_poll_interval": _env("ORPHAN_INTERVAL",     3),

    # ── Pre-live calibration ───────────────────────────────────
    # Fetches historical ticks and walk-forward-replays them through the
    # signal models before live trading starts, to check the configured
    # thresholds show a plausible edge against REAL price history rather
    # than trusting hand-picked numbers on faith. See run_calibration().
    "calibration_enabled":  _env("CALIBRATION_ENABLED", True),
    "calibration_ticks":    _env("CALIBRATION_TICKS",   5000),
    "calibration_folds":    _env("CALIBRATION_FOLDS",      3),
    # If calibration finds no model/expiry clearing min_ev_margin, the bot
    # refuses to start live trading rather than running on faith. This is
    # a deliberate, informed override — not a silent bypass.
    "force_live_without_edge": _env("FORCE_LIVE_WITHOUT_EDGE", False),

    # ── Persistence (optional — bot runs fine without it, just loses
    # learned state — markov table, pattern table, expiry stats, martingale
    # state — on every restart, same as before) ────────────────────────
    "supabase_url": _env("SUPABASE_URL", ""),
    "supabase_key": _env("SUPABASE_KEY", ""),
    "persist_every_secs": _env("PERSIST_EVERY_SECS", 60),
}



# ============================================================================
# HELPERS
# ============================================================================

def _ts() -> str:
    return datetime.now().strftime("%H:%M:%S")

def _log(tag: str, msg: str):
    print(f"[{_ts()}] [{tag}] {msg}", flush=True)


# ============================================================================
# SIGNAL ENGINE — 7 models
# ============================================================================

class Direction(Enum):
    UP   =  1
    DOWN = -1


@dataclass
class TradeSignal:
    direction: Optional[Direction]
    score:     float
    expiry:    int
    ev:        float
    reasons:   list = field(default_factory=list)


class SignalEngine:
    def __init__(self, cfg: dict):
        self.cfg   = cfg
        self.ws    = cfg["window_size"]
        self.ticks: deque = deque(maxlen=self.ws)
        self.dirs:  deque = deque(maxlen=self.ws)

        # Markov: {(prev_dir, curr_dir): count}
        self.markov:       defaultdict = defaultdict(int)
        self.markov_total: defaultdict = defaultdict(int)

        # Multi-step patterns: {(d1,d2,d3): {dir: count}}
        self.patterns:       defaultdict = defaultdict(lambda: defaultdict(int))
        self.pattern_total:  defaultdict = defaultdict(int)

        # Per-expiry win tracking: {n: [wins, total]}
        self.expiry_stats: dict = {n: [0, 0] for n in cfg["expiry_options"]}

        # Pending EV checks: (resolve_at_tick_idx, ref_price, direction, expiry)
        self._pending_ev:  list = []
        self._tick_idx:    int  = 0

        self.tick_count:   int  = 0
        self.payout_ratio: float = 0.95   # updated from live proposals

    # ── Ingestion ─────────────────────────────────────────────────────────────

    def add_tick(self, price: float):
        # FIX: _resolve_ev used to run BEFORE self._tick_idx incremented,
        # so the "has target tick arrived" check compared against a
        # tick_idx that was always one behind the tick actually being
        # processed. A registered N-tick-expiry trade was silently being
        # resolved on the (N+1)-th subsequent tick instead of the N-th —
        # the in-session EV/expiry-selection tracker (used to pick which
        # expiry to trade) was learning from a horizon one tick longer
        # than the real contracts it's meant to represent. Real Deriv
        # contract settlement is unaffected (handled separately via
        # proposal_open_contract) — this only corrupted the bot's own
        # self-assessment of which expiry performs best. Caught by a
        # direct pipeline test, not visible from reading the code alone.
        if self.ticks:
            d = Direction.UP if price > self.ticks[-1] else Direction.DOWN
            self._update_markov(d)
            self._update_patterns(d)
            self.dirs.append(d)
        self.ticks.append(price)
        self._tick_idx   += 1
        self.tick_count  += 1
        if len(self.ticks) > 1:
            self._resolve_ev(price)

    def _update_markov(self, curr: Direction):
        if self.dirs:
            prev = self.dirs[-1]
            self.markov[(prev, curr)]   += 1
            self.markov_total[prev]     += 1

    def _update_patterns(self, curr: Direction):
        if len(self.dirs) >= 3:
            key = (self.dirs[-3], self.dirs[-2], self.dirs[-1])
            self.patterns[key][curr]  += 1
            self.pattern_total[key]   += 1

    def _resolve_ev(self, price: float):
        still = []
        for (target, ref, direction, expiry) in self._pending_ev:
            if self._tick_idx >= target:
                won = ((direction == Direction.UP   and price > ref) or
                       (direction == Direction.DOWN and price < ref))
                self.expiry_stats[expiry][0] += int(won)
                self.expiry_stats[expiry][1] += 1
            else:
                still.append((target, ref, direction, expiry))
        self._pending_ev = still

    def register_trade(self, direction: Direction, expiry: int):
        if self.ticks:
            self._pending_ev.append(
                (self._tick_idx + expiry, self.ticks[-1], direction, expiry)
            )

    def is_ready(self) -> bool:
        return self.tick_count >= self.cfg["min_window"]

    # ── Model 1: Bernoulli Bias ───────────────────────────────────────────────

    def _bias(self) -> tuple[Optional[Direction], float]:
        if len(self.dirs) < self.cfg["min_window"]:
            return None, 0.5
        n_up  = sum(1 for d in self.dirs if d == Direction.UP)
        p_hat = n_up / len(self.dirs)
        if p_hat > self.cfg["bias_rise"]:
            return Direction.UP, p_hat
        if p_hat < self.cfg["bias_fall"]:
            return Direction.DOWN, 1 - p_hat
        return None, p_hat

    # ── Model 2: Markov Chain ─────────────────────────────────────────────────

    def _markov(self) -> tuple[Optional[Direction], float]:
        if not self.dirs:
            return None, 0.5
        last  = self.dirs[-1]
        total = self.markov_total.get(last, 0)
        if total == 0:
            return None, 0.5
        thresh = self.cfg["markov_thresh"]
        p_cont = self.markov.get((last, last), 0) / total
        if p_cont > thresh:
            return last, p_cont
        other = Direction.DOWN if last == Direction.UP else Direction.UP
        p_rev = self.markov.get((last, other), 0) / total
        if p_rev > thresh:
            return other, p_rev
        return None, max(p_cont, p_rev)

    # ── Model 3: Multi-Step Conditional ──────────────────────────────────────

    def _multistep(self) -> tuple[Optional[Direction], float]:
        if len(self.dirs) < 3:
            return None, 0.5
        key   = (self.dirs[-3], self.dirs[-2], self.dirs[-1])
        total = self.pattern_total.get(key, 0)
        if total < self.cfg["cond_min_samples"]:
            return None, 0.5
        p_up  = self.patterns[key].get(Direction.UP,   0) / total
        p_dn  = 1 - p_up
        thresh = self.cfg["cond_prob_thresh"]
        if p_up > thresh:
            return Direction.UP,   p_up
        if p_dn > thresh:
            return Direction.DOWN, p_dn
        return None, max(p_up, p_dn)

    # ── Model 4: Momentum ─────────────────────────────────────────────────────

    def _momentum(self) -> tuple[Optional[Direction], int]:
        window = self.cfg["momentum_window"]
        recent = list(self.dirs)[-window:]
        if len(recent) < window:
            return None, 0
        M = sum(1 if d == Direction.UP else -1 for d in recent)
        thresh = self.cfg["momentum_thresh"]
        if M >= thresh:
            return Direction.UP,   M
        if M <= -thresh:
            return Direction.DOWN, M
        return None, M

    # ── Model 5: Volatility Filter ───────────────────────────────────────────

    def _vol_ok(self) -> tuple[bool, float]:
        prices = list(self.ticks)
        if len(prices) < 10:
            return False, 0.0
        # FIX: was computing std-dev of ABSOLUTE tick moves (e.g. 0.04 USD),
        # which is price-scale dependent. The configured vol_min/vol_max
        # (0.0001-0.0020) are return-scale numbers sized for near-1.0 prices.
        # For R_75 / 1HZ100V trading at ~300-800, absolute moves are orders
        # of magnitude above vol_max, blocking every signal indefinitely.
        # Returns are scale-invariant: same bounds work regardless of price.
        returns = [abs(prices[i] - prices[i-1]) / prices[i-1]
                   for i in range(1, len(prices))
                   if prices[i-1] != 0]
        if not returns:
            return False, 0.0
        mu    = sum(returns) / len(returns)
        var   = sum((x - mu)**2 for x in returns) / len(returns)
        sigma = math.sqrt(var)
        return self.cfg["vol_min"] <= sigma <= self.cfg["vol_max"], sigma

    # ── Model 6: EV-optimised expiry ─────────────────────────────────────────

    def best_expiry(self) -> tuple[int, float]:
        """
        Picks the expiry with the best BETA-SHRUNK expected value.

        Previously: raw win rate once total >= min_expiry_hist, otherwise
        NO gate at all (any expiry could fire freely before 10 trades of
        history existed for it) and the EV bar itself was 0.0 — exactly
        breakeven, not a real edge requirement.

        Now: every expiry's win rate is shrunk toward the TRUE breakeven
        rate implied by the live-tracked payout_ratio, weighted by
        ev_prior_strength (pseudo-trades). At zero history this correctly
        evaluates to exactly breakeven (EV=0) rather than an unconstrained
        free pass — so the min_ev_margin requirement applies from trade
        one, not just after 10 trades happen to accumulate.
        """
        breakeven_p = 1.0 / (1.0 + self.payout_ratio) if self.payout_ratio > 0 else 0.5
        prior_n = self.cfg.get("ev_prior_strength", 10)

        best_ev, best_n = -999.0, self.cfg["expiry_options"][2]
        for n in self.cfg["expiry_options"]:
            wins, total = self.expiry_stats[n]
            shrunk_p = (wins + prior_n * breakeven_p) / (total + prior_n)
            ev = shrunk_p * self.payout_ratio - (1 - shrunk_p)
            if ev > best_ev:
                best_ev, best_n = ev, n
        return best_n, best_ev

    # ── Model 6b: Monte Carlo duration selector ───────────────────────────────

    def mc_best_expiry(self, direction: "Direction",
                       n_sims: int = 500) -> tuple[int, float]:
        """
        Monte Carlo duration selection — replaces the flat per-expiry
        win-rate lookup for choosing how many ticks to hold.

        Instead of asking "which expiry had the best historical win rate?",
        asks "which expiry maximises expected value RIGHT NOW, given what
        this price series is currently doing?":

          1. Estimate local drift (µ) and volatility (σ) from the most
             recent window of returns. Drift is the market's current
             directional tendency over this window — positive σ·√dt noise
             on a rising drift means Rise contracts have a natural edge,
             and the simulation captures that automatically rather than
             relying on hand-picked threshold calibration to approximate it.

          2. Simulate N forward paths of each candidate duration using a
             simple discrete GBM: p_i+1 = p_i · exp((µ − σ²/2)·dt + σ·ε)
             where ε ~ N(0,1) and dt=1 tick. This is a closed-form
             estimate rather than full path-by-path simulation — each
             terminal price is drawn directly from the known distribution
             of GBM(T steps), which is both accurate and fast.

          3. Estimate p(win) for each duration — the fraction of simulated
             paths where price moves in the signal's direction.

          4. Blend with historical win rate using Beta shrinkage (same
             ev_prior_strength knob as best_expiry()) so a thin historical
             record doesn't get overridden by MC alone, and a rich record
             stays anchored to real outcomes even when MC disagrees.

          5. Compute EV = blended_p × payout_ratio − (1 − blended_p) and
             return whichever expiry clears the EV margin with the best EV.

        Falls back gracefully to best_expiry() if there's insufficient
        price history to estimate drift/sigma reliably.
        """
        prices = list(self.ticks)
        if len(prices) < 20:
            return self.best_expiry()

        # Local drift and sigma from recent returns (GBM parameters)
        rets   = [(prices[i] - prices[i-1]) / prices[i-1]
                  for i in range(max(1, len(prices)-50), len(prices))
                  if prices[i-1] != 0]
        if len(rets) < 5:
            return self.best_expiry()

        mu_ret = sum(rets) / len(rets)
        sigma2 = sum((r - mu_ret)**2 for r in rets) / len(rets)
        sigma  = math.sqrt(max(sigma2, 1e-12))

        # Terminal-price MC: for each duration T, draw n_sims terminal prices
        # from the GBM terminal distribution directly (closed form) rather
        # than stepping through each tick — accurate and ~50x faster.
        import random
        sign = 1 if direction == Direction.UP else -1
        breakeven_p = 1.0 / (1.0 + self.payout_ratio) if self.payout_ratio > 0 else 0.5
        prior_n     = self.cfg.get("ev_prior_strength", 10)

        best_ev, best_n = -999.0, self.cfg["expiry_options"][2]
        for T in self.cfg["expiry_options"]:
            # GBM drift and vol over T ticks
            mu_T    = (mu_ret - 0.5 * sigma2) * T
            sigma_T = sigma * math.sqrt(T)
            # Simulate terminal log-returns
            wins_mc = 0
            for _ in range(n_sims):
                # Box-Muller for a single N(0,1) draw
                u1 = random.random() or 1e-15
                u2 = random.random() or 1e-15
                z  = math.sqrt(-2 * math.log(u1)) * math.cos(2 * math.pi * u2)
                log_ret = mu_T + sigma_T * z
                if sign * log_ret > 0:
                    wins_mc += 1

            mc_p = wins_mc / n_sims

            # Blend MC estimate with historical win rate using Beta shrinkage.
            # At zero historical trades, the prior is pure MC. As historical
            # trades accumulate they increasingly anchor the estimate.
            hist_wins, hist_total = self.expiry_stats.get(T, [0, 0])
            blended_p = (hist_wins + prior_n * mc_p) / (hist_total + prior_n)

            ev = blended_p * self.payout_ratio - (1 - blended_p)
            if ev > best_ev:
                best_ev, best_n = ev, T

        return best_n, best_ev

    # ── Model 7: Combined signal ──────────────────────────────────────────────

    def compute(self) -> TradeSignal:
        bias_dir,   bias_s   = self._bias()
        markov_dir, markov_s = self._markov()
        mom_dir,    mom_raw  = self._momentum()
        ms_dir,     ms_s     = self._multistep()
        vol_ok,     sigma    = self._vol_ok()

        # Normalise momentum to [0,1]
        mom_s = min(abs(mom_raw) / self.cfg["momentum_window"], 1.0)

        def signed(direction, strength, weight):
            if direction is None:
                return 0.0
            sign = 1 if direction == Direction.UP else -1
            return sign * strength * weight

        S = (signed(bias_dir,   bias_s,   self.cfg["w_bias"])   +
             signed(markov_dir, markov_s, self.cfg["w_markov"]) +
             signed(mom_dir,    mom_s,    self.cfg["w_momentum"]))

        reasons = []

        if not vol_ok:
            # Still compute a preliminary best_expiry for logging/display
            expiry, ev = self.best_expiry()
            reasons.append(f"vol_blocked σ={sigma:.6f}")
            return TradeSignal(None, S, expiry, ev, reasons)

        if bias_dir:
            reasons.append(f"bias={'↑' if bias_dir==Direction.UP else '↓'} p={bias_s:.3f}")
        if markov_dir:
            reasons.append(f"markov={'↑' if markov_dir==Direction.UP else '↓'} p={markov_s:.3f}")
        if mom_dir:
            reasons.append(f"mom={'↑' if mom_dir==Direction.UP else '↓'} M={mom_raw:+d}")
        if ms_dir:
            reasons.append(f"cond={'↑' if ms_dir==Direction.UP else '↓'} p={ms_s:.3f}")

        thresh = self.cfg["combined_thresh"]
        if S > thresh:
            direction = Direction.UP
        elif S < -thresh:
            direction = Direction.DOWN
        else:
            # Score below threshold — use best_expiry for logging only
            expiry, ev = self.best_expiry()
            reasons.append(f"score {S:+.3f} below ±{thresh}")
            return TradeSignal(None, S, expiry, ev, reasons)

        # Direction resolved — use Monte Carlo to pick the best duration
        # for THIS tick's conditions rather than a session-level average.
        n_sims = self.cfg.get("mc_sims", 500)
        expiry, ev = self.mc_best_expiry(direction, n_sims=n_sims)
        reasons.append(f"mc_expiry={expiry}t EV={ev:+.3f}")
        return TradeSignal(direction, S, expiry, ev, reasons)


# ============================================================================
# PERSISTENCE (optional — bot runs fine without it)
# ============================================================================
# Previously: fully in-memory. Every Markov transition, pattern count, and
# per-expiry win rate reset to nothing on every restart/redeploy — a bot
# that's supposed to be learning within a session was throwing that
# learning away constantly. This persists it to Supabase if configured;
# if SUPABASE_URL/SUPABASE_KEY aren't set, or `requests` isn't installed,
# it degrades to a silent no-op — nothing about the bot's core behaviour
# depends on this being available.

class PersistenceStore:
    def __init__(self, cfg: dict):
        self.url = cfg.get("supabase_url", "")
        self.key = cfg.get("supabase_key", "")
        self.ok  = bool(self.url and self.key and requests is not None)
        if not self.ok and (self.url or self.key):
            _log("STORE", "Supabase URL/key set but 'requests' not "
                          "installed — persistence disabled.")
        self._headers = {
            "apikey":        self.key,
            "Authorization": f"Bearer {self.key}",
            "Content-Type":  "application/json",
            "Prefer":        "resolution=merge-duplicates",
        }

    def _upsert(self, table: str, row: dict):
        if not self.ok:
            return
        try:
            requests.post(
                f"{self.url}/rest/v1/{table}",
                headers=self._headers, json=row, timeout=10,
            )
        except Exception as exc:
            _log("STORE", f"Upsert to {table} failed: {exc}")

    def _select(self, table: str, key: str) -> Optional[dict]:
        if not self.ok:
            return None
        try:
            resp = requests.get(
                f"{self.url}/rest/v1/{table}?key=eq.{key}&select=*",
                headers=self._headers, timeout=10,
            )
            rows = resp.json()
            return rows[0] if rows else None
        except Exception as exc:
            _log("STORE", f"Select from {table} failed: {exc}")
            return None

    # ── Serialization helpers ───────────────────────────────────────────────
    # Direction-keyed dicts (markov, patterns) don't survive JSON directly —
    # Enum keys and tuple keys both need string encoding on the way out and
    # decoding on the way back in.

    @staticmethod
    def _dir_str(d: "Direction") -> str:
        return "UP" if d == Direction.UP else "DOWN"

    @staticmethod
    def _str_dir(s: str) -> "Direction":
        return Direction.UP if s == "UP" else Direction.DOWN

    def save_signal_state(self, symbol: str, eng: "SignalEngine"):
        markov = {f"{self._dir_str(p)}->{self._dir_str(c)}": v
                  for (p, c), v in eng.markov.items()}
        markov_total = {self._dir_str(k): v for k, v in eng.markov_total.items()}
        patterns = {
            "|".join(self._dir_str(d) for d in key): {
                self._dir_str(d): v for d, v in inner.items()
            }
            for key, inner in eng.patterns.items()
        }
        pattern_total = {
            "|".join(self._dir_str(d) for d in key): v
            for key, v in eng.pattern_total.items()
        }
        expiry_stats = {str(k): v for k, v in eng.expiry_stats.items()}

        self._upsert("bot_signal_state", {
            "key":           symbol,
            "markov":        json.dumps(markov),
            "markov_total":  json.dumps(markov_total),
            "patterns":      json.dumps(patterns),
            "pattern_total": json.dumps(pattern_total),
            "expiry_stats":  json.dumps(expiry_stats),
            "payout_ratio":  eng.payout_ratio,
            "updated_at":    datetime.utcnow().isoformat(),
        })

    def load_signal_state(self, symbol: str, eng: "SignalEngine"):
        row = self._select("bot_signal_state", symbol)
        if not row:
            _log("STORE", "No prior signal state found — cold start.")
            return
        try:
            markov = json.loads(row.get("markov") or "{}")
            for k, v in markov.items():
                p_s, c_s = k.split("->")
                eng.markov[(self._str_dir(p_s), self._str_dir(c_s))] = v

            markov_total = json.loads(row.get("markov_total") or "{}")
            for k, v in markov_total.items():
                eng.markov_total[self._str_dir(k)] = v

            patterns = json.loads(row.get("patterns") or "{}")
            for k, inner in patterns.items():
                key = tuple(self._str_dir(s) for s in k.split("|"))
                for d_s, v in inner.items():
                    eng.patterns[key][self._str_dir(d_s)] = v

            pattern_total = json.loads(row.get("pattern_total") or "{}")
            for k, v in pattern_total.items():
                key = tuple(self._str_dir(s) for s in k.split("|"))
                eng.pattern_total[key] = v

            expiry_stats = json.loads(row.get("expiry_stats") or "{}")
            for k, v in expiry_stats.items():
                if int(k) in eng.expiry_stats:
                    eng.expiry_stats[int(k)] = v

            if row.get("payout_ratio"):
                eng.payout_ratio = float(row["payout_ratio"])

            total_markov = sum(markov_total.values())
            _log("STORE", f"Warm-started signal state — "
                          f"{total_markov} markov samples, "
                          f"{len(patterns)} patterns, "
                          f"payout_ratio={eng.payout_ratio:.3f}")
        except Exception as exc:
            _log("STORE", f"Failed to parse signal state — cold start: {exc}")

    def save_risk_state(self, symbol: str, risk: "MartingaleManager"):
        self._upsert("bot_risk_state", {
            "key":            symbol,
            "current_stake":  risk.current_stake,
            "loss_streak":    risk.loss_streak,
            "total_profit":   risk.total_profit,
            "wins":           risk.wins,
            "losses":         risk.losses,
            "updated_at":     datetime.utcnow().isoformat(),
        })

    def load_risk_state(self, symbol: str, risk: "MartingaleManager"):
        row = self._select("bot_risk_state", symbol)
        if not row:
            return
        try:
            risk.current_stake = float(row.get("current_stake", risk.initial_stake))
            risk.loss_streak   = int(row.get("loss_streak", 0))
            risk.total_profit  = float(row.get("total_profit", 0.0))
            risk.wins          = int(row.get("wins", 0))
            risk.losses        = int(row.get("losses", 0))
            _log("STORE", f"Warm-started risk state — "
                          f"{risk.wins}W/{risk.losses}L, "
                          f"P&L=${risk.total_profit:+.2f}, "
                          f"stake=${risk.current_stake:.2f}")
        except Exception as exc:
            _log("STORE", f"Failed to parse risk state: {exc}")


# ============================================================================
# MARTINGALE MANAGER
# ============================================================================

class MartingaleManager:
    def __init__(self, cfg: dict):
        self.initial_stake = cfg["initial_stake"]
        self.current_stake = cfg["initial_stake"]
        self.mul           = cfg["martingale_mul"]
        self.max_losses    = cfg["max_losses"]
        self.target_profit = cfg["target_profit"]
        self.stop_loss     = cfg["stop_loss"]
        self.target_profit_pct = cfg.get("target_profit_pct", 0.0)
        self.stop_loss_pct     = cfg.get("stop_loss_pct", 0.0)
        # Set once via set_session_start_balance() after the first real
        # balance fetch. Flat $ target_profit/stop_loss don't scale with
        # account size — a $20 stop is nothing on a $5000 account and
        # nearly everything on a $50 one. When the _pct variants are set
        # (>0), they're evaluated against this fixed session-start balance
        # instead, and take precedence over the flat $ figures.
        self.session_start_balance: Optional[float] = None
        self.loss_streak   = 0
        self.total_profit  = 0.0
        self.wins          = 0
        self.losses        = 0

    def set_session_start_balance(self, balance: float):
        self.session_start_balance = balance
        if self.target_profit_pct > 0 or self.stop_loss_pct > 0:
            _log("RISK", f"Session start balance ${balance:.2f} — "
                 f"target={self.target_profit_pct:.0%} "
                 f"(${balance * self.target_profit_pct:.2f})  "
                 f"stop={self.stop_loss_pct:.0%} "
                 f"(${balance * self.stop_loss_pct:.2f})")

    def get_stake(self) -> float:
        return round(self.current_stake, 2)

    def record_win(self, profit: float):
        self.wins         += 1
        self.total_profit += profit
        self.loss_streak   = 0
        self.current_stake = self.initial_stake
        _log("WIN",   f"+${profit:.2f} | stake reset → ${self.initial_stake:.2f}")
        self._print_stats()

    def record_loss(self, loss: float):
        self.losses       += 1
        self.total_profit += loss   # loss is already negative
        self.loss_streak  += 1
        _log("LOSS",  f"-${abs(loss):.2f} | streak={self.loss_streak}")
        if self.loss_streak >= self.max_losses:
            _log("MARTI", f"{self.max_losses} losses → reset to ${self.initial_stake:.2f}")
            self.current_stake = self.initial_stake
            self.loss_streak   = 0
        else:
            self.current_stake = round(self.current_stake * self.mul, 2)
            _log("MARTI", f"L{self.loss_streak} next stake ${self.current_stake:.2f}")
        self._print_stats()

    def can_trade(self) -> bool:
        target = self.target_profit
        stop   = self.stop_loss
        if self.session_start_balance:
            if self.target_profit_pct > 0:
                target = self.session_start_balance * self.target_profit_pct
            if self.stop_loss_pct > 0:
                stop = self.session_start_balance * self.stop_loss_pct
        if self.total_profit >= target:
            _log("RISK", f"Target profit reached (${self.total_profit:.2f})")
            return False
        if self.total_profit <= -stop:
            _log("RISK", f"Stop-loss hit (${self.total_profit:.2f})")
            return False
        return True

    def _print_stats(self):
        total = self.wins + self.losses
        wr    = (self.wins / total * 100) if total > 0 else 0.0
        print(f"\n{'='*58}")
        print(f"  {total} trades | W:{self.wins} L:{self.losses} | WR:{wr:.1f}%")
        print(f"  P&L ${self.total_profit:+.2f} | next stake ${self.current_stake:.2f}")
        print(f"{'='*58}\n")


# ============================================================================
# DERIV CLIENT  (send queue · receive inbox · orphan recovery)
# ============================================================================

REST_BASE = "https://api.derivws.com"


class DerivClient:
    def __init__(self, cfg: dict):
        self.api_token  = cfg["api_token"]
        self.app_id     = cfg["app_id"]
        self.account_id = cfg.get("account_id") or None
        self.use_real   = bool(cfg.get("use_real_account", False))
        self.symbol     = cfg["symbol"]
        self.cfg        = cfg
        # Resolved lazily in connect() — the new Options API authenticates
        # the WebSocket via a one-time-password embedded in the connection
        # URL itself (obtained over REST), rather than an app_id query
        # param + an in-band {"authorize": token} message.
        self.ws_url                = None
        self.ws                    = None
        self._send_queue: Optional[asyncio.Queue] = None
        self._inbox:      Optional[asyncio.Queue] = None
        self._send_task:  Optional[asyncio.Task]  = None
        self._recv_task:  Optional[asyncio.Task]  = None
        self.last_profit_ratio: Optional[float]   = None
        self.initial_balance:   float             = 0.0

    # ── REST helpers (new Options API) ──────────────────────────────────
    # These run in a thread executor since they're plain blocking HTTP
    # calls (stdlib urllib, so no extra dependency is required just to
    # authenticate) and only happen once per connect/reconnect.

    def _rest_request(self, path: str, method: str = "GET") -> dict:
        req = urllib.request.Request(
            f"{REST_BASE}{path}",
            method=method,
            headers={
                "Deriv-App-ID":  self.app_id,
                "Authorization": f"Bearer {self.api_token}",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"HTTP {exc.code} from {path}: {body}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"Network error calling {path}: {exc.reason}") from exc

    def _resolve_account_id(self) -> str:
        """Pick an account_id if one wasn't pinned via DERIV_ACCOUNT_ID.
        NOTE: field names below (data / account_id / type) follow Deriv's
        documented Options API shape as published; if Deriv changes this
        response shape, set DERIV_ACCOUNT_ID explicitly to skip this call
        entirely — that's the more robust option for production use."""
        payload = self._rest_request("/trading/v1/options/accounts")
        accounts = payload.get("data") or payload.get("accounts") or []
        if not accounts:
            raise RuntimeError("GET /trading/v1/options/accounts returned no accounts")
        wanted_type = "real" if self.use_real else "demo"
        for acc in accounts:
            acc_type = str(acc.get("type") or acc.get("account_type") or "").lower()
            if acc_type == wanted_type:
                return acc.get("account_id") or acc.get("id")
        # Fall back to the first account if type filtering didn't match.
        first = accounts[0]
        return first.get("account_id") or first.get("id")

    def _fetch_ws_url(self) -> str:
        if not self.account_id:
            self.account_id = self._resolve_account_id()
            self.cfg["account_id"] = self.account_id  # cache for reconnects
        payload = self._rest_request(
            f"/trading/v1/options/accounts/{self.account_id}/otp", method="POST"
        )
        url = (payload.get("data") or {}).get("url")
        if not url:
            raise RuntimeError(f"OTP response missing url: {payload}")
        return url

    async def connect(self) -> bool:
        loop = asyncio.get_event_loop()
        try:
            self.ws_url = await loop.run_in_executor(None, self._fetch_ws_url)
        except Exception as exc:
            _log("AUTH", f"Failed to obtain OTP WebSocket URL: {exc}")
            return False

        safe_url = self.ws_url.split("?")[0]  # never log the otp itself
        _log("WS", f"Connecting → {safe_url} (account {self.account_id})")
        self.ws = await websockets.connect(
            self.ws_url,
            ping_interval=self.cfg["ws_ping_interval"],
            ping_timeout=20,
            close_timeout=10,
        )
        self._send_queue = asyncio.Queue()
        self._inbox      = asyncio.Queue()
        self._start_io()

        # The OTP embedded in the connection URL already authenticates
        # this session — no separate {"authorize": token} message is sent
        # or expected. Confirm the session + pull the starting balance
        # with an ordinary balance request instead.
        await self.send({"balance": 1})
        resp = await self.receive_type("balance", timeout=15)
        if resp is None or "error" in resp:
            err = (resp or {}).get("error", {}).get("message", "timeout")
            _log("AUTH", f"Failed: {err}")
            return False
        bal = resp.get("balance", {})
        self.initial_balance = float(bal.get("balance", 0) or 0)
        _log("AUTH",
             f"OK | account {self.account_id} | "
             f"Balance: ${self.initial_balance:.2f} {bal.get('currency', '')}")
        return True

    def _start_io(self):
        for t in (self._send_task, self._recv_task):
            if t and not t.done():
                t.cancel()
        self._send_task = asyncio.create_task(self._send_pump(), name="send_pump")
        self._recv_task = asyncio.create_task(self._recv_pump(), name="recv_pump")

    async def _send_pump(self):
        while True:
            data, fut = await self._send_queue.get()
            try:
                await self.ws.send(json.dumps(data))
                if fut and not fut.done():
                    fut.set_result(True)
            except Exception as exc:
                if fut and not fut.done():
                    fut.set_exception(exc)
            finally:
                self._send_queue.task_done()

    async def _recv_pump(self):
        try:
            async for raw in self.ws:
                try:
                    await self._inbox.put(json.loads(raw))
                except json.JSONDecodeError:
                    pass
        except (ConnectionClosed, ConnectionClosedError, ConnectionClosedOK):
            await self._inbox.put({"__disconnect__": True})
        except Exception as exc:
            _log("RECV", f"Error: {exc}")
            await self._inbox.put({"__disconnect__": True})

    async def close(self):
        for t in (self._send_task, self._recv_task):
            if t and not t.done():
                t.cancel()
        if self.ws:
            try:
                await self.ws.close()
            except Exception:
                pass

    async def send(self, data: dict):
        loop = asyncio.get_event_loop()
        fut  = loop.create_future()
        await self._send_queue.put((data, fut))
        await fut

    async def receive(self, timeout: float = 10) -> dict:
        try:
            return await asyncio.wait_for(self._inbox.get(), timeout=timeout)
        except asyncio.TimeoutError:
            return {}

    async def receive_type(self, msg_type: str, timeout: float = 10) -> Optional[dict]:
        deadline  = asyncio.get_event_loop().time() + timeout
        while True:
            remaining = deadline - asyncio.get_event_loop().time()
            if remaining <= 0:
                return None
            try:
                msg = await asyncio.wait_for(self._inbox.get(), timeout=remaining)
            except asyncio.TimeoutError:
                return None
            if "__disconnect__" in msg:
                await self._inbox.put(msg)
                return None
            if msg_type in msg or "error" in msg:
                return msg
            await self._inbox.put(msg)

    async def subscribe_ticks(self) -> bool:
        await self.send({"ticks": self.symbol, "subscribe": 1})
        resp = await self.receive_type("tick", timeout=10)
        if resp is None or "error" in resp:
            err = (resp or {}).get("error", {}).get("message", "timeout")
            _log("TICK", f"Subscribe failed: {err}")
            return False
        _log("TICK", f"Subscribed to {self.symbol}")
        return True

    async def place_trade(self, direction: "Direction", stake: float,
                          expiry: int) -> Optional[str]:
        contract_type = "CALL" if direction == Direction.UP else "PUT"
        proposal_req  = {
            "proposal":      1,
            "amount":        stake,
            "basis":         "stake",
            "contract_type": contract_type,
            "currency":      self.cfg["currency"],
            "duration":      expiry,
            "duration_unit": "t",
            "symbol":        self.symbol,
        }
        await self.send(proposal_req)
        proposal = await self.receive_type("proposal", timeout=12)
        if proposal is None or "error" in proposal:
            err = (proposal or {}).get("error", {}).get("message", "timeout")
            _log("PROPOSAL", f"Error: {err}")
            return None
        prop_data   = proposal.get("proposal", {})
        proposal_id = prop_data.get("id")
        ask_price   = float(prop_data.get("ask_price", stake))
        payout      = float(prop_data.get("payout", 0))
        if not proposal_id:
            _log("PROPOSAL", "No proposal ID")
            return None

        # Live payout ratio for EV calculations. FIX: previously computed
        # (and never even stored — see below) payout/ask_price, the GROSS
        # return multiple (e.g. 1.95x). Every EV formula in this codebase
        # expects NET profit ratio (0.95 = 95% profit on a win), matching
        # the 0.95 init default — using the gross figure here would have
        # doubled the assumed edge in every EV calculation.
        profit_ratio = None
        if ask_price > 0:
            profit_ratio = (payout - ask_price) / ask_price
            _log("PROPOSAL",
                 f"{contract_type} {expiry}t  ask=${ask_price:.2f}  "
                 f"payout=${payout:.2f}  profit_ratio={profit_ratio:.3f}")
        # FIX: this was computed and logged but never actually stored
        # anywhere the EV calculations could read it — payout_ratio sat
        # frozen at its 0.95 init value forever, regardless of what real
        # proposals showed. Every EV gate in this bot was silently running
        # against a hardcoded assumption, never real market data, since
        # the bot was first written.
        self.last_profit_ratio = profit_ratio

        buy_time    = time.time()
        contract_id = None
        await self.send({"buy": proposal_id, "price": ask_price})

        for attempt in range(self.cfg["buy_recv_retries"]):
            resp = await self.receive_type("buy", timeout=8)
            if resp is None:
                _log("BUY", f"No response (attempt {attempt + 1})")
                continue
            if "error" in resp:
                _log("BUY", f"Error: {resp['error'].get('message', '')}")
                return None
            contract_id = resp.get("buy", {}).get("contract_id")
            if contract_id:
                break

        if not contract_id:
            _log("BUY", "No contract_id — running orphan recovery")
            contract_id = await self._recover_orphan(stake, buy_time)
            if contract_id:
                _log("BUY", f"Orphan recovered → {contract_id}")
            else:
                _log("BUY", "Orphan recovery failed — unlocking")
                return None

        _log("TRADE",
             f"{contract_type}  ${stake:.2f}  {expiry}t  contract={contract_id}")

        # Subscribe to live settlement updates
        try:
            await self.send({"proposal_open_contract": 1,
                             "contract_id": contract_id, "subscribe": 1})
        except Exception as exc:
            _log("TRADE", f"Subscribe to updates failed: {exc}")

        return str(contract_id)

    async def _recover_orphan(self, stake: float, buy_time: float) -> Optional[str]:
        for attempt in range(self.cfg["orphan_poll_attempts"]):
            await asyncio.sleep(self.cfg["orphan_poll_interval"])
            try:
                await self.send({"profit_table": 1, "description": 1,
                                 "sort": "DESC", "limit": 5})
                resp = await self.receive_type("profit_table", timeout=10)
                if not resp or "error" in resp:
                    continue
                for tx in resp.get("profit_table", {}).get("transactions", []):
                    if (abs(float(tx.get("buy_price", 0)) - stake) < 0.01 and
                            float(tx.get("purchase_time", 0)) >= buy_time - 5):
                        return str(tx.get("contract_id"))
            except Exception as exc:
                _log("ORPHAN", f"Poll {attempt + 1} error: {exc}")
        return None

    async def poll_contract(self, contract_id: str) -> Optional[dict]:
        try:
            await self.send({"proposal_open_contract": 1,
                             "contract_id": contract_id})
            resp = await self.receive_type("proposal_open_contract", timeout=10)
            if resp and "proposal_open_contract" in resp:
                return resp["proposal_open_contract"]
        except Exception as exc:
            _log("POLL", f"Error: {exc}")
        return None

    async def fetch_balance(self) -> Optional[float]:
        try:
            await self.send({"balance": 1})
            resp = await self.receive_type("balance", timeout=10)
            if resp and "balance" in resp:
                return float(resp["balance"]["balance"])
        except Exception as exc:
            _log("BALANCE", f"Fetch error: {exc}")
        return None


# ============================================================================
# PRE-LIVE CALIBRATION
# ============================================================================
# Previously the bot started trading live the moment min_window (50) ticks
# of warmup passed — no backtest, no validation that the hand-picked
# thresholds (bias_rise=0.52, markov_thresh=0.55, combined_thresh=0.60...)
# actually show an edge against real price history before risking money on
# them. This fetches real historical ticks and walk-forward replays them
# through the EXACT SAME SignalEngine used live (not a separate, drifting
# reimplementation) to check that before the live loop ever starts.

async def _fetch_calibration_ticks(client: "DerivClient", cfg: dict) -> list:
    count = min(int(cfg.get("calibration_ticks", 5000)), 5000)
    await client.send({
        "ticks_history": cfg["symbol"],
        "count":         count,
        "end":           "latest",
        "style":         "ticks",
    })
    resp = await client.receive_type("history", timeout=20)
    if resp is None or "error" in resp:
        err = (resp or {}).get("error", {}).get("message", "timeout")
        _log("CALIBRATION", f"History fetch failed: {err}")
        return []
    prices = resp.get("history", {}).get("prices", [])
    return [float(p) for p in prices]


async def _fetch_reference_payout_ratio(client: "DerivClient", cfg: dict) -> float:
    """One real proposal purely to seed calibration with an actual payout
    ratio instead of the SignalEngine's cold 0.95 default — the replay's
    breakeven math is only as honest as the ratio it's measured against."""
    await client.send({
        "proposal": 1, "amount": cfg["initial_stake"], "basis": "stake",
        "contract_type": "CALL", "currency": cfg["currency"],
        "duration": 3, "duration_unit": "t", "symbol": cfg["symbol"],
    })
    resp = await client.receive_type("proposal", timeout=12)
    if resp is None or "error" in resp:
        return 0.95
    data = resp.get("proposal", {})
    ask, payout = float(data.get("ask_price", 0)), float(data.get("payout", 0))
    if ask > 0:
        return (payout - ask) / ask
    return 0.95


def _replay_fold(prices: list, cfg: dict, ref_payout_ratio: float) -> dict:
    """Replays one chunk of historical prices through a fresh SignalEngine,
    using the exact same add_tick/compute/register_trade flow the live bot
    uses — not a reimplementation. Returns that fold's expiry_stats."""
    eng = SignalEngine(cfg)
    eng.payout_ratio = ref_payout_ratio
    last_eval = 0
    for i, price in enumerate(prices):
        eng.add_tick(price)
        if not eng.is_ready():
            continue
        if (i - last_eval) < cfg["eval_every_ticks"]:
            continue
        last_eval = i
        sig = eng.compute()
        if sig.direction is not None:
            eng.register_trade(sig.direction, sig.expiry)
    return eng.expiry_stats


async def run_calibration(client: "DerivClient", cfg: dict) -> bool:
    """
    Fetches real historical ticks, splits into calibration_folds
    sequential chunks (each replayed from a cold SignalEngine so no fold
    leaks lookback state into another — genuine walk-forward, not one
    long lucky/unlucky continuous run), and reports whether the combined
    signal's EV — Beta-shrunk toward real breakeven, same formula
    best_expiry() uses live — clears min_ev_margin on any expiry.

    Returns True if an edge was found, False otherwise. Doesn't touch
    live state; RiseFallBot.run() decides what to do with the result.
    """
    print(f"\n{'='*58}")
    print(" PRE-LIVE CALIBRATION")
    print(f"{'='*58}")

    ref_ratio = await _fetch_reference_payout_ratio(client, cfg)
    breakeven_p = 1.0 / (1.0 + ref_ratio) if ref_ratio > 0 else 0.5
    _log("CALIBRATION", f"Reference payout ratio: {ref_ratio:.3f}  "
                        f"(breakeven win rate: {breakeven_p:.3f})")

    prices = await _fetch_calibration_ticks(client, cfg)
    if len(prices) < cfg["min_window"] * cfg["calibration_folds"] * 2:
        _log("CALIBRATION",
             f"Only {len(prices)} historical ticks available — too few "
             f"for {cfg['calibration_folds']} meaningful folds. Skipping "
             f"validation; thresholds are UNVALIDATED against real data.")
        return False

    fold_size = len(prices) // cfg["calibration_folds"]
    prior_n   = cfg.get("ev_prior_strength", 10)
    per_expiry_folds = defaultdict(list)   # expiry -> [ev_fold1, ev_fold2, ...]

    for f in range(cfg["calibration_folds"]):
        chunk = prices[f * fold_size:(f + 1) * fold_size]
        stats = _replay_fold(chunk, cfg, ref_ratio)
        print(f"\n  Fold {f + 1}/{cfg['calibration_folds']} "
              f"({len(chunk)} ticks):")
        for n in cfg["expiry_options"]:
            wins, total = stats[n]
            shrunk_p = (wins + prior_n * breakeven_p) / (total + prior_n)
            ev = shrunk_p * ref_ratio - (1 - shrunk_p)
            per_expiry_folds[n].append(ev)
            hist_note = f"{wins}/{total}" if total else "no signals"
            print(f"    {n}t expiry: shrunk_ev={ev:+.4f}  raw=({hist_note})")

    print(f"\n  {'='*54}")
    print(f"  Mean EV across folds (required margin: "
          f"{cfg['min_ev_margin']:+.3f}):")
    edge_found = False
    for n in cfg["expiry_options"]:
        evs = per_expiry_folds[n]
        mean_ev = sum(evs) / len(evs)
        clears  = mean_ev >= cfg["min_ev_margin"]
        edge_found = edge_found or clears
        flag = "CLEARS margin" if clears else "below margin"
        print(f"    {n}t: mean_ev={mean_ev:+.4f}  ({flag})")
    print(f"  {'='*54}")

    if edge_found:
        _log("CALIBRATION", "At least one expiry clears the required EV "
                            "margin against real historical data.")
    else:
        _log("CALIBRATION", "NO expiry cleared the required EV margin. "
                            "Configured thresholds show no validated edge "
                            "against real recent price history.")
    print(f"{'='*58}\n")

    # Aggregate cumulative expiry_stats across all folds so the caller can
    # seed the LIVE engine — without this, the Beta-shrinkage prior starts
    # at EV=0 (breakeven with zero history), the min_ev_margin gate always
    # blocks, zero trades fire, and EV is permanently stuck at zero.
    combined_stats = {}
    for n in cfg["expiry_options"]:
        total_w, total_t = 0, 0
        for f in range(cfg["calibration_folds"]):
            chunk = prices[f * fold_size:(f + 1) * fold_size]
            st = _replay_fold(chunk, cfg, ref_ratio)
            w, t = st[n]
            total_w += w
            total_t += t
        combined_stats[n] = [total_w, total_t]

    return edge_found, combined_stats, ref_ratio


# ============================================================================
# MAIN BOT
# ============================================================================

class RiseFallBot:
    def __init__(self, cfg: dict = CONFIG):
        self.cfg    = cfg
        self.client = DerivClient(cfg)
        self.signal = SignalEngine(cfg)
        self.risk   = MartingaleManager(cfg)
        self.store  = PersistenceStore(cfg)
        self._last_persist_time: float = 0.0

        self.tick_count      = 0
        self.last_eval_tick  = 0

        self.current_contract:   Optional[dict] = None
        self.waiting_for_result: bool           = False
        self.lock_since:         Optional[float] = None

        self._stop = False

        # Prevents concurrent _evaluate() calls during async I/O gaps
        self._evaluating: bool = False

        # Circuit breaker
        self._cb_paused_until: float = 0.0
        self._cb_loss_count:   int   = 0

        # Balance snapshot for precise P&L
        self._balance_before: Optional[float] = None

        # Sequence-level martingale guard state. seq_start_balance is a
        # FIXED snapshot taken once when a losing sequence begins — not
        # recomputed against the live, shrinking balance on every step.
        # (A live-recomputed version mechanically tightens every step,
        # since committed stakes grow while balance shrinks simultaneously
        # — no fixed percentage can then unlock more than one recovery
        # step regardless of its value. This bit us for real building a
        # different bot the same day this one got revisited — fixed at
        # the source here instead.)
        self.seq_start_balance:     Optional[float] = None
        self.seq_stakes_committed:  float            = 0.0

        # Latest known balance, refreshed opportunistically (pre-trade
        # fetch, settlement fetch) — used by the sequence guard so it
        # doesn't need an extra fetch of its own on every evaluation.
        self.last_known_balance: Optional[float] = None

    # ── Sequence guard helpers ─────────────────────────────────────────────────

    def _reset_sequence_guard(self):
        self.seq_start_balance    = None
        self.seq_stakes_committed = 0.0

    def _sequence_guard_allows(self, next_stake: float) -> bool:
        """
        True if committing `next_stake` on top of what's already committed
        this sequence stays within max_sequence_loss_pct of the balance AT
        SEQUENCE START. Disabled (always True) if the guard is off or no
        sequence is currently active.
        """
        pct = self.cfg.get("max_sequence_loss_pct", 0.0)
        if pct <= 0 or self.seq_start_balance is None:
            return True
        max_allowed = self.seq_start_balance * pct
        would_commit = self.seq_stakes_committed + next_stake
        if would_commit > max_allowed:
            _log("GUARD",
                 f"Sequence loss guard triggered — committed="
                 f"${self.seq_stakes_committed:.2f} next=${next_stake:.2f} "
                 f"> max=${max_allowed:.2f} "
                 f"(seq_start_balance=${self.seq_start_balance:.2f}, "
                 f"{pct:.0%} cap). Aborting sequence to protect balance.")
            return False
        return True



    def _unlock(self, reason: str = "manual"):
        if self.waiting_for_result:
            cid = (self.current_contract or {}).get("id", "?")
            _log("UNLOCK", f"Contract {cid} ({reason})")
        self.waiting_for_result = False
        self.current_contract   = None
        self.lock_since         = None
        self._evaluating        = False

    def _check_lock_timeout(self):
        if not self.waiting_for_result or self.lock_since is None:
            return
        expiry  = (self.current_contract or {}).get("expiry", 5)
        timeout = expiry + self.cfg["lock_timeout"]  # expiry ticks + buffer seconds
        elapsed = time.monotonic() - self.lock_since
        if elapsed >= timeout:
            _log("TIMEOUT", f"Locked {elapsed:.0f}s (limit {timeout}s) — auto-unlocking")
            self._unlock("timeout")

    # ── Console listener ──────────────────────────────────────────────────────

    async def _console(self):
        loop = asyncio.get_event_loop()
        _log("CMD", "Commands: [u]nlock  [s]tats  [q]uit")
        while not self._stop:
            try:
                cmd = (await loop.run_in_executor(None, input)).strip().lower()
                if cmd == "u":
                    self._unlock("user command")
                elif cmd == "s":
                    self.risk._print_stats()
                    now     = time.monotonic()
                    cb_info = ""
                    if now < self._cb_paused_until:
                        cb_info = f"  BREAKER paused {self._cb_paused_until - now:.0f}s"
                    expiry_info = "  Expiry stats:\n"
                    for n, (w, t) in self.signal.expiry_stats.items():
                        if t:
                            ev = (w/t) * self.signal.payout_ratio - (1 - w/t)
                            expiry_info += (f"    {n}t: {w}/{t} wins "
                                           f"({w/t*100:.1f}%) EV={ev:+.3f}\n")
                        else:
                            expiry_info += f"    {n}t: no data yet\n"
                    print(f"  >> Ticks: {self.tick_count}  "
                          f"Ready: {self.signal.is_ready()}{cb_info}")
                    print(expiry_info)
                elif cmd in ("q", "quit", "exit"):
                    _log("CMD", "Quit")
                    self._stop = True
                    break
            except (EOFError, KeyboardInterrupt):
                break

    # ── Tick handler ──────────────────────────────────────────────────────────

    async def on_tick(self, tick_data: dict):
        quote = tick_data.get("quote")
        if quote is None:
            return
        price = float(quote)

        self.tick_count += 1
        self.signal.add_tick(price)
        self._check_lock_timeout()

        # Periodic persistence — throttled so this isn't an HTTP call on
        # every single tick, only every persist_every_secs.
        if self.store.ok:
            now = time.monotonic()
            if now - self._last_persist_time >= self.cfg["persist_every_secs"]:
                self._last_persist_time = now
                self.store.save_signal_state(self.cfg["symbol"], self.signal)
                self.store.save_risk_state(self.cfg["symbol"], self.risk)

        if self.tick_count % 10 == 0:
            status = "WAITING" if self.waiting_for_result else "READY"
            warmup = ("" if self.signal.is_ready()
                      else f" [warmup {self.tick_count}/{self.cfg['min_window']}]")
            print(f"\r  #{self.tick_count}  p={price:.5f}  {status}{warmup}  {_ts()}",
                  end="", flush=True)

        if not self.waiting_for_result and not self._evaluating and self.signal.is_ready():
            if (self.tick_count - self.last_eval_tick) >= self.cfg["eval_every_ticks"]:
                self.last_eval_tick = self.tick_count
                print()
                await self._evaluate()

    # ── Signal evaluation and trade placement ─────────────────────────────────

    async def _evaluate(self):
        if self.waiting_for_result or self._evaluating:
            return

        self._evaluating = True
        try:
            await self._evaluate_inner()
        finally:
            self._evaluating = False

    async def _evaluate_inner(self):
        if self.waiting_for_result:
            return

        sig = self.signal.compute()

        print(f"\n{'='*58}")
        print(f"SIGNAL  #{self.tick_count}  {_ts()}")
        if sig.reasons:
            for r in sig.reasons:
                print(f"  · {r}")
        print(f"  Score={sig.score:+.3f}  Expiry={sig.expiry}t  EV={sig.ev:+.3f}")
        if sig.direction:
            label = "RISE (CALL)" if sig.direction == Direction.UP else "FALL (PUT)"
            print(f"  → {label}")
        else:
            print(f"  → No trade")
        print(f"{'='*58}")

        if sig.direction is None:
            return

        # EV gate — requires a real margin ABOVE true breakeven (computed
        # from the live payout_ratio, Beta-shrunk toward it when history is
        # thin — see best_expiry()). Previously this only applied once 10
        # trades of history existed for an expiry, and even then only
        # required EV > 0.0 (exactly breakeven, no real edge required).
        # Shrinkage means thin history now correctly evaluates near
        # breakeven itself, so the margin requirement applies from the
        # first trade, not just after history accumulates.
        if sig.ev < self.cfg["min_ev_margin"]:
            _log("EV", f"Expiry {sig.expiry}t EV={sig.ev:+.3f} "
                       f"below required margin {self.cfg['min_ev_margin']:+.3f} — skip")
            return

        # Circuit breaker
        now = time.monotonic()
        if now < self._cb_paused_until:
            remaining = self._cb_paused_until - now
            _log("BREAKER", f"Paused — {remaining:.0f}s remaining")
            return

        if not self.risk.can_trade():
            return

        stake = self.risk.get_stake()

        # Snap balance before trade
        bal_before = await self.client.fetch_balance()
        if bal_before is not None:
            self._balance_before   = bal_before
            self.last_known_balance = bal_before
            _log("BALANCE", f"Pre-trade: ${bal_before:.2f}")
        else:
            self._balance_before = None
            _log("BALANCE", "Pre-trade balance unavailable — fallback to API profit")

        # Sequence loss guard — if this stake (on top of what's already
        # committed this losing sequence) would exceed max_sequence_loss_pct
        # of the balance at sequence start, abandon the sequence and fall
        # back to the initial stake instead of placing the elevated one.
        if self.risk.loss_streak > 0 and not self._sequence_guard_allows(stake):
            self.risk.current_stake = self.risk.initial_stake
            self.risk.loss_streak   = 0
            self._reset_sequence_guard()
            stake = self.risk.get_stake()
            _log("GUARD", f"Sequence abandoned — next stake reset to ${stake:.2f}")

        contract_id = await self.client.place_trade(sig.direction, stake, sig.expiry)

        # Apply the real observed profit ratio to the signal engine, if we
        # got one from this proposal — clamped to sane bounds so a garbage
        # or missing value can't corrupt every EV calculation downstream.
        pr = self.client.last_profit_ratio
        if pr is not None and 0.1 <= pr <= 3.0:
            self.signal.payout_ratio = pr

        if contract_id:
            self.signal.register_trade(sig.direction, sig.expiry)
            self.current_contract   = {
                "id":        contract_id,
                "stake":     stake,
                "expiry":    sig.expiry,
                "direction": sig.direction,
                "time":      datetime.now(),
            }
            self.waiting_for_result = True
            self.lock_since         = time.monotonic()
            _log("LOCK", f"Waiting for result on {contract_id}")
        else:
            self._balance_before = None
            _log("TRADE", "Placement failed — READY for next signal")

    # ── Settlement ────────────────────────────────────────────────────────────

    def _is_settled(self, data: dict) -> bool:
        if data.get("is_settled"):
            return True
        for key in ("status", "contract_status"):
            if data.get(key, "").lower() in ("sold", "won", "lost"):
                return True
        return False

    async def handle_settlement(self, contract_data: dict):
        cid = str(contract_data.get("contract_id", ""))
        if not self.current_contract or cid != self.current_contract["id"]:
            return None
        if not self._is_settled(contract_data):
            return None

        bal_after  = await self.client.fetch_balance()
        api_profit = float(contract_data.get("profit", 0))
        status     = contract_data.get("status", "unknown")

        if bal_after is not None and self._balance_before is not None:
            actual_profit = round(bal_after - self._balance_before, 2)
            _log("BALANCE",
                 f"Pre: ${self._balance_before:.2f} → Post: ${bal_after:.2f} "
                 f"| Actual: ${actual_profit:+.2f} | API: ${api_profit:+.2f}")
        else:
            actual_profit = api_profit
            _log("BALANCE", f"Balance unavailable — using API profit ${api_profit:+.2f}")

        print(f"\n{'='*58}")
        print(f"RESULT  contract={cid}")
        print(f"        status={status}  profit=${actual_profit:+.2f}")
        print(f"{'='*58}")

        if actual_profit > 0:
            self.risk.record_win(actual_profit)
            self._reset_sequence_guard()
            self._cb_loss_count = 0
        else:
            stake_placed = (self.current_contract or {}).get(
                "stake", self.risk.initial_stake)
            was_fresh_sequence = (self.risk.loss_streak == 0)

            self.risk.record_loss(actual_profit)

            # Sequence guard bookkeeping. Snapshot seq_start_balance ONCE,
            # at the moment this sequence begins (was_fresh_sequence — the
            # loss that just happened was the first in a new streak), using
            # the post-loss balance as the fixed reference point. Every
            # subsequent step in this sequence checks against this same
            # fixed number, not a freshly recomputed one.
            if was_fresh_sequence:
                self._reset_sequence_guard()
                self.seq_start_balance = (
                    bal_after if bal_after is not None else self.last_known_balance
                )
            self.seq_stakes_committed += stake_placed
            if bal_after is not None:
                self.last_known_balance = bal_after
            # MartingaleManager.record_loss() itself resets loss_streak to 0
            # once max_losses is reached — that's a separate, deliberate
            # reset from the sequence guard above (guard protects the
            # balance; max_losses caps how long a sequence can run at all).
            # If that just fired, mirror it here so the next stake starts
            # clean rather than carrying a stale guard snapshot forward.
            if self.risk.loss_streak == 0:
                self._reset_sequence_guard()

            # FIX: circuit breaker used `loss_streak % limit == 0`, which
            # desyncs against MartingaleManager's own internal reset at
            # max_losses (not generally a multiple of limit) — the breaker
            # could stop re-triggering predictably once the two counters
            # drifted apart. Now a plain threshold with an explicit local
            # reset, decoupled from martingale's own bookkeeping.
            limit = self.cfg["consec_loss_limit"]
            pause = self.cfg["consec_pause_secs"]
            self._cb_loss_count += 1
            if self._cb_loss_count >= limit:
                self._cb_paused_until = time.monotonic() + pause
                self._cb_loss_count = 0
                _log("BREAKER",
                     f"{limit} consecutive losses — pausing {pause}s "
                     f"({pause//60}m {pause%60}s)")

        self._balance_before = None
        self._unlock("settlement")
        return self.risk.can_trade()

    # ── Reconnect ─────────────────────────────────────────────────────────────

    async def _reconnect(self) -> bool:
        delay   = self.cfg["reconnect_delay_min"]
        max_d   = self.cfg["reconnect_delay_max"]
        attempt = 0
        while not self._stop:
            attempt += 1
            _log("RECONNECT", f"Attempt {attempt} in {delay}s...")
            await asyncio.sleep(delay)
            delay = min(delay * 2, max_d)
            await self.client.close()
            self.client = DerivClient(self.cfg)
            try:
                if not await self.client.connect():
                    continue
                if not await self.client.subscribe_ticks():
                    continue
                # Re-attach to live contract if one is open
                if self.waiting_for_result and self.current_contract:
                    cid = self.current_contract["id"]
                    _log("RECONNECT", f"Re-attaching to {cid}")
                    data = await self.client.poll_contract(cid)
                    if data:
                        await self.handle_settlement(data)
                    if self.waiting_for_result:   # still open — re-subscribe
                        await self.client.send({"proposal_open_contract": 1,
                                                "contract_id": cid, "subscribe": 1})
                _log("RECONNECT", "OK")
                return True
            except Exception as exc:
                _log("RECONNECT", f"Error: {exc}")
        return False

    # ── Main run loop ─────────────────────────────────────────────────────────

    async def run(self):
        cfg = self.cfg
        print("\n" + "="*58)
        print("  DERIV RISE/FALL BOT — 1HZ100V")
        print("="*58)
        print(f"  Symbol   : {cfg['symbol']}")
        print(f"  Contract : CALL (Rise) / PUT (Fall)")
        print(f"  Expiry   : EV-optimised 1–5 ticks")
        print(f"  Stake    : ${cfg['initial_stake']:.2f} "
              f"(x{cfg['martingale_mul']} mart, reset @{cfg['max_losses']} losses)")
        print(f"  Target   : +${cfg['target_profit']}  "
              f"Stop: -${cfg['stop_loss']}")
        print(f"  Warmup   : {cfg['min_window']} ticks")
        print(f"  CB Limit : {cfg['consec_loss_limit']} losses → "
              f"{cfg['consec_pause_secs']}s pause "
              f"({cfg['consec_pause_secs']//60}m)")
        print("="*58)
        print("  Signal models:")
        print("    1  Bernoulli bias     (p̂ over sliding window)")
        print("    2  Markov chain       (P(X→X) transition matrix)")
        print("    3  Multi-step cond.   (3-tick pattern lookup)")
        print("    4  Momentum           (M = Σ last 5 ticks)")
        print("    5  Volatility filter  (σ gate)")
        print("    6  EV expiry select   (best 1-5t by win rate)")
        print("    7  Combined score     (bias 30% + markov 40% + mom 30%)")
        print("="*58 + "\n")

        if cfg["api_token"] in ("REPLACE_WITH_YOUR_TOKEN", ""):
            _log("ERROR", "Set DERIV_API_TOKEN env var before running")
            return
        if not cfg["app_id"]:
            _log("ERROR", "Set DERIV_APP_ID env var before running "
                           "(register one at https://developers.deriv.com)")
            return

        if not await self.client.connect():
            return
        self.initial_balance = self.client.initial_balance
        self.risk.set_session_start_balance(self.initial_balance)

        # Warm-start from persisted state, if configured — see
        # PersistenceStore. No-op if Supabase isn't set up.
        self.store.load_signal_state(cfg["symbol"], self.signal)
        self.store.load_risk_state(cfg["symbol"], self.risk)

        if self.cfg.get("calibration_enabled", True):
            result = await run_calibration(self.client, self.cfg)
            edge_found, cal_stats, cal_ratio = result
            # Seed the live engine's expiry_stats from calibration so EV
            # is non-zero from tick one — breaks the cold-start deadlock.
            for n, (w, t) in cal_stats.items():
                if t > 0 and n in self.signal.expiry_stats:
                    self.signal.expiry_stats[n] = [w, t]
            if 0.1 <= cal_ratio <= 3.0:
                self.signal.payout_ratio = cal_ratio
            _log("CALIBRATION",
                 f"Seeded live engine — "
                 f"{sum(t for w,t in cal_stats.values())} signals across "
                 f"{len(cal_stats)} expiries, payout_ratio={cal_ratio:.3f}")
            if not edge_found and not self.cfg.get("force_live_without_edge", False):
                _log("CALIBRATION",
                     "No configured model/expiry cleared the required EV "
                     "margin against real historical data. Refusing to "
                     "start live trading — set FORCE_LIVE_WITHOUT_EDGE=true "
                     "to override this and run anyway.")
                return

        if not await self.client.subscribe_ticks():
            return

        _log("BOT", f"Live — warming up ({cfg['min_window']} ticks needed)...")

        console_task = asyncio.create_task(self._console(), name="console")

        try:
            while not self._stop:
                response = await self.client.receive(timeout=60)

                if "__disconnect__" in response:
                    _log("WS", "Disconnected — reconnecting")
                    if not await self._reconnect():
                        break
                    continue

                if not response:
                    try:
                        await self.client.ws.ping()
                    except Exception:
                        _log("WS", "Ping failed — reconnecting")
                        if not await self._reconnect():
                            break
                    continue

                if "tick" in response:
                    await self.on_tick(response["tick"])

                if "proposal_open_contract" in response:
                    result = await self.handle_settlement(
                        response["proposal_open_contract"])
                    if result is False:
                        break

                # NOTE: removed a dead "buy" response branch that was here —
                # a fresh buy confirmation never has is_settled/status in
                # ("sold","won","lost"), so handle_settlement's _is_settled
                # check always returned False for it; the branch was a
                # harmless no-op every time, not a real settlement path.
                # The proposal_open_contract subscription (sent right after
                # every successful buy) is what actually delivers settlement.

                if "transaction" in response:
                    tx = response["transaction"]
                    if "contract_id" in tx:
                        result = await self.handle_settlement({
                            "contract_id": tx.get("contract_id"),
                            "profit":      tx.get("profit", 0),
                            "status":      tx.get("action", ""),
                            "is_settled":  True,
                        })
                        if result is False:
                            break

                if "profit_table" in response and self.current_contract:
                    for tx in response["profit_table"].get("transactions", []):
                        if str(tx.get("contract_id")) == self.current_contract["id"]:
                            result = await self.handle_settlement({
                                "contract_id": tx["contract_id"],
                                "profit": (float(tx.get("sell_price", 0))
                                           - float(tx.get("buy_price",  0))),
                                "status":     "sold",
                                "is_settled": True,
                            })
                            if result is False:
                                break

        except KeyboardInterrupt:
            print("\n\nInterrupted")
        except Exception as exc:
            print(f"\nUnhandled error: {exc}")
            import traceback
            traceback.print_exc()
        finally:
            console_task.cancel()
            if self.store.ok:
                self.store.save_signal_state(self.cfg["symbol"], self.signal)
                self.store.save_risk_state(self.cfg["symbol"], self.risk)
                _log("STORE", "Final state saved before shutdown.")
            await self.client.close()
            print("\nFINAL STATS")
            self.risk._print_stats()
            print(f"  Ticks processed: {self.tick_count}")
            print("Goodbye")


# ============================================================================
# ENTRY POINT
# ============================================================================

async def main():
    bot = RiseFallBot(CONFIG)
    await bot.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nExiting...")
