import { watchQuotes } from '@mathieuc/tradingview/data';

const SYMBOL = 'FX_IDC:XAUUSD';
let stopping = false;
let activeWatcher = null;

function emit(value) {
  process.stdout.write(`${JSON.stringify(value)}\n`);
}

function numberOrNull(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function stop() {
  stopping = true;
  if (activeWatcher) {
    try {
      await activeWatcher.stop();
    } catch {
      // Shutdown is best-effort; the process is exiting.
    }
  }
}

process.on('SIGINT', () => { void stop(); });
process.on('SIGTERM', () => { void stop(); });

while (!stopping) {
  try {
    process.stderr.write(`Connecting TradingView quote stream for ${SYMBOL}\n`);
    activeWatcher = await watchQuotes({ symbols: [SYMBOL] }, {
      onData(symbol, quote) {
        emit({
          type: 'quote',
          symbol,
          lp: numberOrNull(quote.lp),
          bid: numberOrNull(quote.bid),
          ask: numberOrNull(quote.ask),
          lp_time_ms: quote.lp_time ? Number(quote.lp_time) * 1000 : null,
          received_at_ms: Date.now(),
        });
      },
      onError(error) {
        process.stderr.write(`TradingView quote error: ${error?.code || ''} ${error?.message || String(error)}\n`);
      },
    });

    emit({ type: 'status', status: 'connected', symbol: SYMBOL, received_at_ms: Date.now() });
    process.stderr.write(`TradingView quote stream connected for ${SYMBOL}\n`);
    await activeWatcher.closed;
    activeWatcher = null;
    if (!stopping) {
      emit({ type: 'status', status: 'disconnected', symbol: SYMBOL, received_at_ms: Date.now() });
      process.stderr.write('TradingView quote stream closed; reconnecting shortly.\n');
    }
  } catch (error) {
    activeWatcher = null;
    process.stderr.write(`TradingView quote bridge failed: ${error?.code || ''} ${error?.message || String(error)}\n`);
    emit({ type: 'status', status: 'disconnected', symbol: SYMBOL, received_at_ms: Date.now() });
  }

  if (!stopping) await sleep(5000);
}

process.stderr.write('TradingView quote bridge stopped.\n');
