use super::database::{Event, Message};
use polymarket_client_sdk_v2::{clob::types::Side, types::U256};
use std::{
    collections::HashMap,
    io::IsTerminal,
    time::{SystemTime, UNIX_EPOCH},
};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::EnvFilter;

struct CompactTime;

impl tracing_subscriber::fmt::time::FormatTime for CompactTime {
    fn format_time(
        &self,
        writer: &mut tracing_subscriber::fmt::format::Writer<'_>,
    ) -> std::fmt::Result {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let seconds = now.as_secs();
        write!(
            writer,
            "{:02}:{:02}:{:02}.{:03}",
            seconds / 3600 % 24,
            seconds / 60 % 60,
            seconds % 60,
            now.subsec_millis()
        )
    }
}

pub fn setup() -> WorkerGuard {
    let (writer, guard) = tracing_appender::non_blocking::NonBlockingBuilder::default()
        .buffered_lines_limit(25_600)
        .lossy(false)
        .finish(std::io::stderr());
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_timer(CompactTime)
        .with_target(false)
        .with_ansi(std::io::stderr().is_terminal())
        .with_writer(writer)
        .init();
    guard
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
            (
                market.asset.clone(),
                market.start_time,
                minutes,
                "MARKET",
                "-".to_owned(),
                format!("slug={}", market.slug),
            )
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
    let time = format!("{:02}:{:02}", start / 3600 % 24, start / 60 % 60);
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
