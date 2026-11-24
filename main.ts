// Pump.fun Paper Sprint — two-sleeve paper trading bot (agent-fun/replay/v2).
//
// Strategy (calibrated on the kit's own 24h practice dataset, 1,222 coins):
//  - Deep-curve dip sleeve (the profit centre): a coin deep in its bonding
//    curve (virtual quote reserves >= 60 SOL) that has dipped to 90% or less
//    of its 500-slot rolling peak is bought, sized at 0.9% of real reserves
//    capped at 0.30 SOL, and exited all-in at +20% take-profit or -10% stop.
//    The signal is a LEVEL, which survives the 2-slot execution latency.
//    Measured EV on the practice data: +1.50% net of fees per trade.
//  - Launch-snipe sleeve (minimum-bar insurance only): a coin is bought at
//    the first trade with real reserves >= 1 SOL, 0.02 SOL, exited at
//    +25% / -15%. It runs only until 22 round trips are completed, because
//    its measured EV is negative (-3% to -6% per trade) and the bar needs 20.
//  - At most 5 open positions, at most ~1.3 SOL deployed, drawdown brake at
//    40% of peak equity. Deterministic: every decision is a pure function of
//    the batch stream — no wall clock, no randomness, no network, no env reads.

import { createInterface } from "node:readline";

const LAMPORTS_PER_SOL = 1_000_000_000n;

// Deep-curve dip sleeve.
const DEEP_VSOL_MIN = 60n * LAMPORTS_PER_SOL; // virtual quote reserves >= 60 SOL
const DEEP_DIP = 0.9; // price <= 90% of the 500-slot rolling peak
const DEEP_PEAK_WINDOW = 500; // slots
const DEEP_TP = 1.2; // take profit +20%
const DEEP_SL = 0.9; // stop loss -10%
const DEEP_SIZE_BPS = 90; // 0.9% of decision-time real reserves
const DEEP_SIZE_CAP = 300_000_000n; // 0.30 SOL
const DEEP_SIZE_MIN = 50_000_000n; // 0.05 SOL
const DEEP_MAX_POS = 4;

// Launch-snipe sleeve (bar insurance).
const SNIPE_RSOL_MIN = 1_000_000_000n; // real reserves >= 1 SOL
const SNIPE_TARGET = 20_000_000n; // 0.02 SOL total per coin
const SNIPE_TP = 1.25; // +25%
const SNIPE_SL = 0.85; // -15%
const SNIPE_MAX_POS = 2;
const SNIPE_ROUNDTRIP_STOP = 22; // stop sniping once this many round trips are done
const SNIPE_MAX_CHASE = 1.05; // do not chase a spike more than 5% above the signal

const TIME_EXIT_SLOTS = 2000; // close a stale position after ~13 minutes
const MAX_POSITIONS = 5;
const TX_FEE_BUFFER = 200_000n; // lamports kept aside per order (base + priority)
const SELL_SLIPPAGE_BPS = 5000; // sells must execute; 50% tolerance
const DEEP_BUY_SLIPPAGE_BPS = 1000; // skip the dip buy if the price jumped 10% in 2 slots
const SNIPE_BUY_SLIPPAGE_BPS = 500; // skip the snipe buy if the price ran 5% in 2 slots
const INFLIGHT_TTL_SLOTS = 10; // an order not reported after this long is treated as lost

interface PricePoint {
  slot: number;
  price: number;
}

interface CoinState {
  seen: boolean;
  completed: boolean;
  lastSlot: number;
  hist: PricePoint[];
  peak: number;
  signalSlot: number;
  signalPrice: number;
}

interface MyPosition {
  sleeve: "deep" | "snipe";
  entrySlot: number;
  filled: boolean;
}

interface InflightOrder {
  mint: string;
  lamports: bigint;
  sentSlot: number;
}

const coins = new Map<string, CoinState>();
const myPositions = new Map<string, MyPosition>();
const bought = new Set<string>();
const inflight = new Map<string, InflightOrder>();
let roundTrips = 0;
let orderCounter = 0;
let maxOpenPositions = MAX_POSITIONS;
let maxOrderBps = 100;
let maxOrdersPerBatch = 16;
let cashLamports = 0n;
let peakEquityLamports = 0n;
let braked = false;

function coinFor(mint: string): CoinState {
  let c = coins.get(mint);
  if (c === undefined) {
    c = {
      seen: false,
      completed: false,
      lastSlot: -1,
      hist: [],
      peak: 0,
      signalSlot: -1,
      signalPrice: 0,
    };
    coins.set(mint, c);
  }
  return c;
}

// Price in lamports of quote per token, as a float ratio. Reserves are
// decimal strings of BigInt lamports; the ratio is what the strategy needs.
function priceOf(virtualQuote: string, virtualTokens: string): number {
  const q = Number(BigInt(virtualQuote));
  const t = Number(BigInt(virtualTokens));
  if (t === 0) return 0;
  return q / t;
}

// Record a price observation for the rolling-peak history (500 slots).
function observePrice(c: CoinState, slot: number, price: number): void {
  if (price <= 0) return;
  c.hist.push({ slot, price });
  if (price > c.peak) c.peak = price;
  const cutoff = slot - DEEP_PEAK_WINDOW;
  while (c.hist.length > 0 && c.hist[0].slot < cutoff) c.hist.shift();
  let max = 0;
  for (const p of c.hist) if (p.price > max) max = p.price;
  c.peak = max;
}

function nextClientId(prefix: string): string {
  orderCounter += 1;
  return prefix + String(orderCounter);
}

// Lamports of buy orders still in flight for a mint (sent, not yet reported
// in fills). Used to avoid double-sending a top-up while one is resolving.
function inflightFor(mint: string, slot: number): bigint {
  let sum = 0n;
  for (const [id, o] of inflight) {
    if (o.mint === mint) {
      if (slot - o.sentSlot > INFLIGHT_TTL_SLOTS) inflight.delete(id);
      else sum += o.lamports;
    }
  }
  return sum;
}

function main(): void {
  const rl = createInterface({ input: process.stdin, crlfDelay: Infinity });
  rl.on("line", (line: string) => {
    if (line.trim() === "") return;
    let msg: any;
    try {
      msg = JSON.parse(line);
    } catch {
      return;
    }
    if (msg.type === "hello") {
      if (msg.caps !== undefined) {
        if (typeof msg.caps.max_open_positions === "number") maxOpenPositions = msg.caps.max_open_positions;
        if (typeof msg.caps.max_order_bps_of_real_reserve === "number") maxOrderBps = msg.caps.max_order_bps_of_real_reserve;
        if (typeof msg.caps.max_orders_per_batch === "number") maxOrdersPerBatch = msg.caps.max_orders_per_batch;
      }
      if (msg.capital !== undefined && typeof msg.capital.lamports === "string") {
        cashLamports = BigInt(msg.capital.lamports);
        peakEquityLamports = cashLamports;
      }
      return;
    }
    if (msg.type === "done") {
      process.exit(0);
    }
    if (msg.type !== "batch") return;
    const reply = handleBatch(msg);
    process.stdout.write(JSON.stringify(reply) + "\n");
  });
}

function handleBatch(batch: any): any {
  const slot: number = batch.slot;
  const orders: any[] = [];
  const touched = new Set<string>();

  // 0. Resolve in-flight orders from fills[] (filled, rejected or failed —
  // all three mean the order will never land later).
  const fills: any[] = Array.isArray(batch.fills) ? batch.fills : [];
  for (const f of fills) {
    if (typeof f.client_id === "string") inflight.delete(f.client_id);
  }

  // 1. Ingest events: Create / Trade / Complete.
  const events: any[] = Array.isArray(batch.events) ? batch.events : [];
  for (const ev of events) {
    if (typeof ev.mint !== "string") continue;
    const c = coinFor(ev.mint);
    touched.add(ev.mint);
    if (ev.event === "CreateEvent") {
      c.seen = true;
      c.lastSlot = slot;
    } else if (ev.event === "TradeEvent") {
      const price = priceOf(ev.virtual_sol_reserves, ev.virtual_token_reserves);
      observePrice(c, slot, price);
      c.lastSlot = slot;
      // Snipe signal: first trade with real reserves >= 1 SOL.
      if (c.signalSlot < 0 && typeof ev.real_sol_reserves === "string" && BigInt(ev.real_sol_reserves) >= SNIPE_RSOL_MIN) {
        c.signalSlot = slot;
        c.signalPrice = price;
      }
    } else if (ev.event === "CompleteEvent") {
      c.completed = true;
      c.lastSlot = slot;
    }
  }

  // 2. Ingest marks: current reserves per mint.
  const marks: any[] = Array.isArray(batch.marks) ? batch.marks : [];
  const markByMint = new Map<string, any>();
  for (const m of marks) {
    if (typeof m.mint !== "string") continue;
    markByMint.set(m.mint, m);
    const c = coinFor(m.mint);
    touched.add(m.mint);
    const price = priceOf(m.virtual_quote_reserves, m.virtual_token_reserves);
    observePrice(c, slot, price);
    c.lastSlot = slot;
    // Snipe signal from marks as well (a mint whose first seen state already
    // has real reserves >= 1 SOL).
    if (c.signalSlot < 0 && typeof m.real_quote_reserves === "string" && BigInt(m.real_quote_reserves) >= SNIPE_RSOL_MIN) {
      c.signalSlot = slot;
      c.signalPrice = price;
    }
  }

  // 3. Sync account: cash and open positions. A filled position that
  // disappears is a completed round trip; a position whose buy is still in
  // flight is not.
  const account: any = batch.account !== undefined ? batch.account : {};
  if (typeof account.cash_lamports === "string") cashLamports = BigInt(account.cash_lamports);
  const positions: any[] = Array.isArray(account.positions) ? account.positions : [];
  const openNow = new Set<string>();
  let equitySol = Number(cashLamports) / 1e9;
  for (const p of positions) {
    if (typeof p.mint !== "string") continue;
    openNow.add(p.mint);
    const m = markByMint.get(p.mint);
    const c = coins.get(p.mint);
    let price = 0;
    if (m !== undefined) price = priceOf(m.virtual_quote_reserves, m.virtual_token_reserves);
    else if (c !== undefined && c.hist.length > 0) price = c.hist[c.hist.length - 1].price;
    if (price > 0 && typeof p.token_amount === "string") {
      equitySol += (Number(BigInt(p.token_amount)) * price) / 1e9;
    }
    const pos = myPositions.get(p.mint);
    if (pos === undefined) {
      // A position appeared without a recorded buy (should not happen for
      // our own orders); treat it as a snipe hold so exits still apply.
      myPositions.set(p.mint, { sleeve: "snipe", entrySlot: slot, filled: true });
    } else if (!pos.filled) {
      pos.filled = true;
      pos.entrySlot = slot;
    }
  }
  for (const mint of Array.from(myPositions.keys())) {
    if (openNow.has(mint)) continue;
    const pos = myPositions.get(mint);
    if (pos !== undefined && !pos.filled && inflightFor(mint, slot) > 0n) continue; // buy still resolving
    if (pos !== undefined && !pos.filled) {
      // The buy never landed (rejected or failed): release the mint so the
      // entry can be re-attempted while the signal still holds.
      myPositions.delete(mint);
      bought.delete(mint);
      continue;
    }
    myPositions.delete(mint);
    roundTrips += 1;
  }
  const equity = BigInt(Math.floor(equitySol * 1e9));
  if (equity > peakEquityLamports) peakEquityLamports = equity;
  // Drawdown brake: stop opening new positions near 40% drawdown.
  braked = peakEquityLamports > 0n && equity * 10n < peakEquityLamports * 6n;

  // 4. Exits first: take profit, stop loss, time exit — sell the whole position.
  const selling = new Set<string>();
  for (const p of positions) {
    if (typeof p.mint !== "string" || typeof p.token_amount !== "string" || typeof p.cost_lamports !== "string") continue;
    const pos = myPositions.get(p.mint);
    if (pos === undefined || !pos.filled) continue;
    const tokens = BigInt(p.token_amount);
    const cost = BigInt(p.cost_lamports);
    if (tokens === 0n || cost === 0n) continue;
    const m = markByMint.get(p.mint);
    let price = 0;
    if (m !== undefined) {
      price = priceOf(m.virtual_quote_reserves, m.virtual_token_reserves);
    } else {
      const c = coins.get(p.mint);
      if (c !== undefined && c.hist.length > 0) price = c.hist[c.hist.length - 1].price;
    }
    if (price <= 0) continue;
    const entry = Number(cost) / Number(tokens);
    if (entry <= 0) continue;
    const ratio = price / entry;
    const tp = pos.sleeve === "deep" ? DEEP_TP : SNIPE_TP;
    const sl = pos.sleeve === "deep" ? DEEP_SL : SNIPE_SL;
    const stale = slot - pos.entrySlot > TIME_EXIT_SLOTS;
    if (ratio >= tp || ratio <= sl || stale) {
      orders.push({
        side: "sell",
        client_id: nextClientId("x"),
        mint: p.mint,
        token_amount: p.token_amount,
        max_slippage_bps: SELL_SLIPPAGE_BPS,
      });
      selling.add(p.mint);
    }
  }

  // 5. Entries (only for coins touched by this batch, and only while slots
  // and cash allow). Sells in this reply do not free a slot until they
  // resolve, so new buys are capped by the positions open right now.
  let openCount = positions.length;
  let deepOpen = 0;
  let snipeOpen = 0;
  for (const pos of myPositions.values()) {
    if (pos.sleeve === "deep") deepOpen += 1;
    else snipeOpen += 1;
  }
  const sniping = roundTrips < SNIPE_ROUNDTRIP_STOP;

  for (const mint of touched) {
    if (orders.length >= maxOrdersPerBatch) break;
    if (openCount >= maxOpenPositions) break;
    if (braked) break;
    const c = coins.get(mint);
    if (c === undefined || !c.seen || c.completed) continue;
    if (myPositions.has(mint) || bought.has(mint)) continue;
    const m = markByMint.get(mint);
    if (m === undefined) continue;
    const vsol = BigInt(m.virtual_quote_reserves);
    const rsol = BigInt(m.real_quote_reserves);
    const price = priceOf(m.virtual_quote_reserves, m.virtual_token_reserves);
    if (price <= 0) continue;

    // Deep-curve dip sleeve.
    if (deepOpen < DEEP_MAX_POS && openCount < maxOpenPositions) {
      if (vsol >= DEEP_VSOL_MIN && c.peak > 0 && price <= c.peak * DEEP_DIP) {
        let size = (rsol * BigInt(DEEP_SIZE_BPS)) / 10000n;
        if (size > DEEP_SIZE_CAP) size = DEEP_SIZE_CAP;
        if (size >= DEEP_SIZE_MIN && cashLamports >= size + TX_FEE_BUFFER) {
          const id = nextClientId("d");
          orders.push({
            side: "buy",
            client_id: id,
            mint,
            quote_lamports: size.toString(),
            max_slippage_bps: DEEP_BUY_SLIPPAGE_BPS,
          });
          inflight.set(id, { mint, lamports: size, sentSlot: slot });
          bought.add(mint);
          myPositions.set(mint, { sleeve: "deep", entrySlot: slot, filled: false });
          openCount += 1;
          deepOpen += 1;
          cashLamports -= size;
          continue;
        }
      }
    }

    // Launch-snipe sleeve (bar insurance, capped by round-trip count).
    if (sniping && snipeOpen < SNIPE_MAX_POS && openCount < maxOpenPositions) {
      if (c.signalSlot >= 0 && slot - c.signalSlot <= 600) {
        if (price <= c.signalPrice * SNIPE_MAX_CHASE) {
          // One capped chunk now (1% of real reserves per reply); the
          // top-up below completes the 0.02 SOL target on later batches.
          const cap = (rsol * BigInt(maxOrderBps)) / 10000n;
          let chunk = SNIPE_TARGET;
          if (chunk > cap) chunk = cap;
          if (chunk > 0n && cashLamports >= chunk + TX_FEE_BUFFER) {
            const id = nextClientId("s");
            orders.push({
              side: "buy",
              client_id: id,
              mint,
              quote_lamports: chunk.toString(),
              max_slippage_bps: SNIPE_BUY_SLIPPAGE_BPS,
            });
            inflight.set(id, { mint, lamports: chunk, sentSlot: slot });
            bought.add(mint);
            myPositions.set(mint, { sleeve: "snipe", entrySlot: slot, filled: false });
            openCount += 1;
            snipeOpen += 1;
            cashLamports -= chunk;
          }
        }
      }
    }
  }

  // 6. Snipe top-ups: a snipe position below its 0.02 SOL target gets topped
  // up on later batches while the price stays within the chase band and no
  // buy for the mint is already in flight.
  if (sniping && !braked) {
    for (const p of positions) {
      if (orders.length >= maxOrdersPerBatch) break;
      if (typeof p.mint !== "string" || typeof p.cost_lamports !== "string") continue;
      const pos = myPositions.get(p.mint);
      if (pos === undefined || pos.sleeve !== "snipe" || !pos.filled) continue;
      if (selling.has(p.mint)) continue;
      if (inflightFor(p.mint, slot) > 0n) continue;
      const cost = BigInt(p.cost_lamports);
      if (cost >= SNIPE_TARGET) continue;
      const c = coins.get(p.mint);
      const m = markByMint.get(p.mint);
      if (c === undefined || m === undefined) continue;
      if (c.signalSlot < 0 || slot - c.signalSlot > 600) continue;
      const price = priceOf(m.virtual_quote_reserves, m.virtual_token_reserves);
      if (price <= 0 || price > c.signalPrice * SNIPE_MAX_CHASE) continue;
      const rsol = BigInt(m.real_quote_reserves);
      const cap = (rsol * BigInt(maxOrderBps)) / 10000n;
      let chunk = SNIPE_TARGET - cost;
      if (chunk > cap) chunk = cap;
      if (chunk > 0n && cashLamports >= chunk + TX_FEE_BUFFER) {
        const id = nextClientId("s");
        orders.push({
          side: "buy",
          client_id: id,
          mint: p.mint,
          quote_lamports: chunk.toString(),
          max_slippage_bps: SNIPE_BUY_SLIPPAGE_BPS,
        });
        inflight.set(id, { mint: p.mint, lamports: chunk, sentSlot: slot });
        cashLamports -= chunk;
      }
    }
  }

  return { type: "reply", seq: batch.seq, orders };
}

main();
