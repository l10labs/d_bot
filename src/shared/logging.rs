use super::database::{Event, Message};
use polymarket_client_sdk_v2::{clob::types::Side, types::U256};
use std::{
    collections::HashMap,
    io::IsTerminal,
    time::{SystemTime, UNIX_EPOCH},
};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

struct CompactTime;

impl tracing_subscriber::fmt::time::FormatTime for CompactTime {
    fn format_time(
        &self,
        writer: &mut tracing_subscriber::fmt::format::Writer<'_>,
    ) -> std::fmt::Result {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let seconds = now.as_secs();
        let hour = seconds / 3600 % 24;
        let meridiem = if hour < 12 { "AM" } else { "PM" };
        write!(
            writer,
            "{:02}:{:02}:{:02}.{:03} {meridiem}",
            (hour + 11) % 12 + 1,
            seconds / 60 % 60,
            seconds % 60,
            now.subsec_millis()
        )
    }
}

/// Keep both guards alive until shutdown so queued console/file logs are flushed.
pub fn setup() -> super::database::Result<(WorkerGuard, WorkerGuard)> {
    let file = tracing_appender::rolling::RollingFileAppender::builder()
        .rotation(tracing_appender::rolling::Rotation::DAILY)
        .filename_prefix("reader")
        .filename_suffix("log")
        .build("logs")?;
    let (file_writer, file_guard) = tracing_appender::non_blocking::NonBlockingBuilder::default()
        .buffered_lines_limit(25_600)
        .lossy(false)
        .finish(file);
    let (writer, guard) = tracing_appender::non_blocking::NonBlockingBuilder::default()
        .buffered_lines_limit(25_600)
        .lossy(false)
        .finish(std::io::stderr());
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(
            tracing_subscriber::fmt::layer()
                .with_timer(CompactTime)
                .with_target(false)
                .with_ansi(std::io::stderr().is_terminal())
                .with_writer(writer),
        )
        .with(
            tracing_subscriber::fmt::layer()
                .with_timer(CompactTime)
                .with_target(false)
                .with_ansi(false)
                .with_writer(file_writer),
        )
        .try_init()?;
    Ok((guard, file_guard))
}

/// Counts committed feed events; market discoveries are logged individually.
#[derive(Default, Debug, PartialEq)]
pub(crate) struct WriteSummary {
    counts: [u64; 7],
    commits: u64,
    intervals: HashMap<U256, u64>,
}

impl WriteSummary {
    pub(crate) fn committed(&mut self, messages: &[Message]) {
        self.commits += 1;
        for message in messages {
            let index = match &message.event {
                Event::Market(market) => {
                    let minutes = (market.end_time - market.start_time) / 60;
                    self.intervals.insert(market.up_token_id, minutes);
                    self.intervals.insert(market.down_token_id, minutes);
                    0
                }
                Event::BidAsk(quote) => match self.intervals.get(&quote.asset_id) {
                    Some(5) => 1,
                    Some(15) => 3,
                    _ => unreachable!("Quote without a supported market interval"),
                },
                Event::Trade(trade) => match self.intervals.get(&trade.asset_id) {
                    Some(5) => 2,
                    Some(15) => 4,
                    _ => unreachable!("Trade without a supported market interval"),
                },
                Event::Binance(_) => 5,
                Event::Chainlink(_) => 6,
            };
            self.counts[index] += 1;
        }
    }

    pub(crate) fn report(&mut self, elapsed: std::time::Duration) {
        let [
            _markets,
            quotes5,
            trades5,
            quotes15,
            trades15,
            binance,
            chainlink,
        ] = self.counts;
        let total: u64 = self.counts[1..].iter().sum();
        tracing::info!(
            "5M-Q({quotes5:03})|5M-T({trades5:03})|15M-Q({quotes15:03})|15M-T({trades15:03})|BNAN-P({binance:03})|CNLK-P({chainlink:03})|TOTAL({total:03})|COMMITS({:03})|EVENTS/SEC({:05.1})",
            self.commits,
            total as f64 / elapsed.as_secs_f64().max(f64::EPSILON),
        );
        self.counts = [0; 7];
        self.commits = 0;
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.commits == 0
    }
}

pub fn event(message: &Message, labels: &mut HashMap<U256, (String, u64, u64, &'static str)>) {
    let (asset, start, minutes, kind, outcome, details) = match &message.event {
        Event::Market(market) => {
            let minutes = (market.end_time - market.start_time) / 60;
            labels.insert(
                market.up_token_id,
                (market.asset.clone(), market.start_time, minutes, "UP"),
            );
            labels.insert(
                market.down_token_id,
                (market.asset.clone(), market.start_time, minutes, "DOWN"),
            );
            let asset = market.asset.to_ascii_uppercase();
            let interval = format!("{minutes}M");
            let start = market.start_time;
            let hour = start / 3600 % 24;
            let meridiem = if hour < 12 { "AM" } else { "PM" };
            let hour12 = (hour + 11) % 12 + 1;
            tracing::info!(
                "MARKET|{asset:>4}|{interval:>3}|{hour12:02}:{:02} {meridiem}|SLUG({:>26})",
                start / 60 % 60,
                market.slug,
            );
            return;
        }
        Event::Binance(price) => (
            price
                .symbol
                .strip_suffix("usdt")
                .unwrap_or(&price.symbol)
                .to_owned(),
            u64::try_from(price.timestamp / 1000).unwrap(),
            0,
            "BINANCE",
            price.value.to_string(),
            String::new(),
        ),
        Event::Chainlink(price) => (
            price.symbol.split('/').next().unwrap().to_owned(),
            u64::try_from(price.timestamp / 1000).unwrap(),
            0,
            "CHAINLINK",
            price.value.to_string(),
            String::new(),
        ),
        Event::BidAsk(quote) => {
            let (asset, start, minutes, outcome) = labels.get(&quote.asset_id).unwrap();
            (
                asset.clone(),
                *start,
                *minutes,
                "QUOTE",
                outcome.to_string(),
                format!(
                    "     | {:<12} | {:<12} | SP({})",
                    format!("BD({})", quote.best_bid),
                    format!("AK({})", quote.best_ask),
                    quote.spread
                ),
            )
        }
        Event::Trade(trade) => {
            let (asset, start, minutes, outcome) = labels.get(&trade.asset_id).unwrap();
            let side = match trade.side {
                Some(Side::Buy) => "BUY",
                Some(Side::Sell) => "SELL",
                _ => "-",
            };
            let size = trade
                .size
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".into());
            (
                asset.clone(),
                *start,
                *minutes,
                "TRADE",
                outcome.to_string(),
                format!(
                    "{side:<4} | {:<12} | {:<12}",
                    format!("PR({})", trade.price),
                    format!("SZ({size})")
                ),
            )
        }
    };
    let asset = asset.to_ascii_uppercase();
    let hour = start / 3600 % 24;
    let meridiem = if hour < 12 { "AM" } else { "PM" };
    let time = format!(
        "{:02}:{:02} {meridiem}",
        (hour + 11) % 12 + 1,
        start / 60 % 60
    );
    let interval = if minutes == 0 {
        "-".to_owned()
    } else {
        format!("{minutes}m")
    };
    if details.is_empty() {
        tracing::info!("{asset:<5} | {interval:<3} | {time} | {kind:<9} | {outcome}");
    } else {
        tracing::info!(
            "{asset:<5} | {interval:<3} | {time} | {kind:<9} | {outcome:<4} | {details}"
        );
    }
}
