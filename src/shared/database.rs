use crate::shared::MarketContext;
use polymarket_client_sdk_v2::{
    clob::{
        types::Side,
        ws::types::response::{BestBidAsk, LastTradePrice},
    },
    rtds::{ChainlinkPrice, CryptoPrice},
    types::{B256, U256},
};
use std::collections::HashMap;
use tokio::sync::mpsc;
use turso::{Connection, params};

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Debug)]
pub struct Message {
    pub received_at: i64,
    pub event: Event,
}

#[derive(Debug)]
pub enum Event {
    Market(MarketContext),
    Binance(CryptoPrice),
    Chainlink(ChainlinkPrice),
    BidAsk(BestBidAsk),
    Trade(LastTradePrice),
}

pub async fn setup(path: &str) -> turso::Result<turso::Connection> {
    let database = turso::Builder::new_local(path).build().await?;
    let connection = database.connect()?;
    connection.execute("PRAGMA foreign_keys = ON", ()).await?;
    let mut journal_mode = connection.query("PRAGMA journal_mode = WAL", ()).await?;
    while journal_mode.next().await?.is_some() {}
    connection
        .execute_batch(concat!(
            "BEGIN;\n",
            include_str!("../../schema.sql"),
            "\nCOMMIT;"
        ))
        .await?;
    Ok(connection)
}

pub async fn writer(connection: Connection, receiver: mpsc::Receiver<Message>) -> Result<()> {
    writer_with_verbose(connection, receiver, false).await
}

pub async fn writer_with_verbose(
    mut connection: Connection,
    mut receiver: mpsc::Receiver<Message>,
    verbose: bool,
) -> Result<()> {
    let mut token_ids = HashMap::new();
    let mut batch = Vec::with_capacity(256);
    let mut summary = super::logging::WriteSummary::default();
    let mut labels = HashMap::new();
    let period = std::time::Duration::from_secs(1);
    let mut last_report = tokio::time::Instant::now();
    let mut ticker = tokio::time::interval_at(last_report + period, period);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let result: Result<()> = async {
        loop {
            tokio::select! {
                biased;
                _ = ticker.tick(), if !verbose => {
                    let now = tokio::time::Instant::now();
                    summary.report(now.duration_since(last_report));
                    last_report = now;
                }
                count = receiver.recv_many(&mut batch, 256) => {
                    if count == 0 {
                        break;
                    }
                    let transaction = connection.transaction().await?;
                    for message in &batch {
                        write_event(&transaction, &mut token_ids, message).await?;
                    }
                    transaction.commit().await?;
                    if verbose {
                        for message in &batch {
                            super::logging::event(message, &mut labels);
                        }
                    } else {
                        for message in &batch {
                            if matches!(&message.event, Event::Market(_)) {
                                super::logging::event(message, &mut labels);
                            }
                        }
                        summary.committed(&batch);
                    }
                    batch.clear();
                }
            }
        }
        Ok(())
    }
    .await;

    if !summary.is_empty() {
        summary.report(last_report.elapsed());
    }
    result
}

pub async fn load_all(connection: &mut Connection) -> Result<Vec<Message>> {
    let progress = indicatif::ProgressBar::new_spinner();
    progress.set_style(indicatif::ProgressStyle::with_template(
        "{spinner:.green} [{elapsed_precise}] {wide_bar:.cyan/blue} {pos}/{len} events ({per_sec}, ETA {eta}) {msg}",
    )?);
    progress.set_message("Counting events");
    progress.enable_steady_tick(std::time::Duration::from_millis(100));
    let result = async {
        let transaction = connection.transaction().await?;
        let messages = load_snapshot(&transaction, &progress).await?;
        transaction.commit().await?;
        Ok(messages)
    }
    .await;
    match &result {
        Ok(_) => progress.finish_with_message("Loaded and sorted"),
        Err(_) => progress.abandon_with_message("Loading failed"),
    }
    result
}

fn update_load_progress(progress: &indicatif::ProgressBar, loaded: usize) {
    if loaded % 4096 == 0 {
        progress.set_position(loaded as u64);
    }
}

async fn load_snapshot(
    connection: &Connection,
    progress: &indicatif::ProgressBar,
) -> Result<Vec<Message>> {
    let mut counts = connection
        .query(
            "SELECT (SELECT count(*) FROM markets) + (SELECT count(*) FROM prices_binance)
         + (SELECT count(*) FROM prices_chainlink) + (SELECT count(*) FROM market_bid_asks)
         + (SELECT count(*) FROM market_trades)",
            (),
        )
        .await?;
    let count = usize::try_from(counts.next().await?.unwrap().get::<i64>(0)?)?;
    drop(counts);
    progress.set_length(count as u64);
    progress.set_message("Loading token identities");
    let mut messages = Vec::new();
    messages.try_reserve_exact(count)?;

    let mut tokens = HashMap::<i64, (U256, B256)>::new();
    let mut market_tokens = HashMap::<i64, (Option<U256>, Option<U256>)>::new();
    let mut rows = connection
        .query(
            "SELECT t.id, t.token_id, t.market_id, t.outcome, m.condition_id
         FROM market_tokens t JOIN markets m ON m.id = t.market_id",
            (),
        )
        .await?;
    while let Some(row) = rows.next().await? {
        let id = row.get::<i64>(0)?;
        let token = row.get::<String>(1)?.parse::<U256>()?;
        let market_id = row.get::<i64>(2)?;
        let outcome = row.get::<String>(3)?;
        let condition = row.get::<String>(4)?.parse::<B256>()?;
        tokens.insert(id, (token, condition));
        let pair = market_tokens.entry(market_id).or_default();
        match outcome.as_str() {
            "up" => pair.0 = Some(token),
            "down" => pair.1 = Some(token),
            _ => return Err(std::io::Error::other("Invalid market outcome").into()),
        }
    }
    drop(rows);
    progress.set_message("Loading markets");

    let mut rows = connection
        .query(
            "SELECT id, condition_id, asset, slug, starts_at, ends_at, discovered_at FROM markets",
            (),
        )
        .await?;
    while let Some(row) = rows.next().await? {
        let pair = market_tokens
            .get(&row.get::<i64>(0)?)
            .ok_or_else(|| std::io::Error::other("Market has no tokens"))?;
        let missing = || std::io::Error::other("Market is missing an up/down token");
        messages.push(Message {
            received_at: row.get(6)?,
            event: Event::Market(MarketContext {
                condition_id: row.get::<String>(1)?.parse()?,
                asset: row.get(2)?,
                slug: row.get(3)?,
                start_time: u64::try_from(row.get::<i64>(4)?)? / 1000,
                end_time: u64::try_from(row.get::<i64>(5)?)? / 1000,
                up_token_id: pair.0.ok_or_else(missing)?,
                down_token_id: pair.1.ok_or_else(missing)?,
            }),
        });
        update_load_progress(progress, messages.len());
    }
    drop(rows);
    drop(market_tokens);

    for (table, chainlink) in [("prices_binance", false), ("prices_chainlink", true)] {
        progress.set_message(format!("Loading {table}"));
        let mut rows = connection
            .query(
                format!("SELECT symbol, price, source_at, received_at FROM {table}"),
                (),
            )
            .await?;
        while let Some(row) = rows.next().await? {
            let symbol = row.get::<String>(0)?;
            let value = row.get::<String>(1)?.parse()?;
            let timestamp = row.get::<i64>(2)?;
            let event = if chainlink {
                Event::Chainlink(
                    ChainlinkPrice::builder()
                        .symbol(symbol)
                        .value(value)
                        .timestamp(timestamp)
                        .build(),
                )
            } else {
                Event::Binance(
                    CryptoPrice::builder()
                        .symbol(symbol)
                        .value(value)
                        .timestamp(timestamp)
                        .build(),
                )
            };
            messages.push(Message {
                received_at: row.get(3)?,
                event,
            });
            update_load_progress(progress, messages.len());
        }
    }

    progress.set_message("Loading bid/ask quotes");
    let mut rows = connection.query(
        "SELECT market_token_id, best_bid, best_ask, spread, source_at, received_at FROM market_bid_asks", (),
    ).await?;
    while let Some(row) = rows.next().await? {
        let &(asset_id, market) = tokens
            .get(&row.get::<i64>(0)?)
            .ok_or_else(|| std::io::Error::other("Quote references unknown token"))?;
        let quote = BestBidAsk::builder()
            .asset_id(asset_id)
            .market(market)
            .best_bid(row.get::<String>(1)?.parse()?)
            .best_ask(row.get::<String>(2)?.parse()?)
            .spread(row.get::<String>(3)?.parse()?)
            .timestamp(row.get::<i64>(4)?)
            .build();
        messages.push(Message {
            received_at: row.get(5)?,
            event: Event::BidAsk(quote),
        });
        update_load_progress(progress, messages.len());
    }
    drop(rows);

    progress.set_message("Loading trades");
    let mut rows = connection.query(
        "SELECT market_token_id, price, size, side, fee_rate_bps, source_at, received_at FROM market_trades", (),
    ).await?;
    while let Some(row) = rows.next().await? {
        let &(asset_id, market) = tokens
            .get(&row.get::<i64>(0)?)
            .ok_or_else(|| std::io::Error::other("Trade references unknown token"))?;
        let mut trade = LastTradePrice::builder()
            .asset_id(asset_id)
            .market(market)
            .price(row.get::<String>(1)?.parse()?)
            .timestamp(row.get::<i64>(5)?)
            .build();
        trade.size = row
            .get::<Option<String>>(2)?
            .map(|s| s.parse())
            .transpose()?;
        trade.side = match row.get::<Option<String>>(3)?.as_deref() {
            Some("buy") => Some(Side::Buy),
            Some("sell") => Some(Side::Sell),
            None => None,
            _ => return Err(std::io::Error::other("Invalid trade side").into()),
        };
        trade.fee_rate_bps = row
            .get::<Option<String>>(4)?
            .map(|s| s.parse())
            .transpose()?;
        messages.push(Message {
            received_at: row.get(6)?,
            event: Event::Trade(trade),
        });
        update_load_progress(progress, messages.len());
    }
    progress.set_position(messages.len() as u64);
    progress.set_message("Sorting by receipt timestamp");
    messages.sort_unstable_by_key(|message| message.received_at);
    Ok(messages)
}

async fn write_event(
    connection: &Connection,
    token_ids: &mut HashMap<U256, i64>,
    message: &Message,
) -> Result<()> {
    let received_at = message.received_at;
    match &message.event {
        Event::Market(market) => {
            let mut rows = connection.query(
                "INSERT INTO markets (condition_id, asset, slug, starts_at, ends_at, discovered_at)
                 VALUES (?, ?, ?, ?, ?, ?)
                 ON CONFLICT(condition_id) DO UPDATE SET asset = excluded.asset, slug = excluded.slug,
                 starts_at = excluded.starts_at, ends_at = excluded.ends_at RETURNING id",
                params![market.condition_id.to_string(), market.asset.as_str(), market.slug.as_str(),
                    i64::try_from(market.start_time)? * 1000,
                    i64::try_from(market.end_time)? * 1000, received_at],
            ).await?;
            let market_id = rows.next().await?.unwrap().get::<i64>(0)?;
            while rows.next().await?.is_some() {}

            for (token_id, outcome) in [(market.up_token_id, "up"), (market.down_token_id, "down")]
            {
                let mut rows = connection
                    .query(
                        "INSERT INTO market_tokens (token_id, market_id, outcome) VALUES (?, ?, ?)
                     ON CONFLICT(token_id) DO UPDATE SET market_id = excluded.market_id,
                     outcome = excluded.outcome RETURNING id",
                        params![token_id.to_string(), market_id, outcome],
                    )
                    .await?;
                let id = rows.next().await?.unwrap().get::<i64>(0)?;
                while rows.next().await?.is_some() {}
                token_ids.insert(token_id, id);
            }
        }
        Event::Binance(price) => {
            let asset = price.symbol.strip_suffix("usdt").unwrap_or(&price.symbol);
            connection.execute(
                "INSERT INTO prices_binance (asset, symbol, price, source_at, received_at) VALUES (?, ?, ?, ?, ?)",
                params![asset, price.symbol.as_str(), price.value.to_string(), price.timestamp, received_at],
            ).await?;
        }
        Event::Chainlink(price) => {
            let asset = price.symbol.split('/').next().unwrap();
            connection.execute(
                "INSERT INTO prices_chainlink (asset, symbol, price, source_at, received_at) VALUES (?, ?, ?, ?, ?)",
                params![asset, price.symbol.as_str(), price.value.to_string(), price.timestamp, received_at],
            ).await?;
        }
        Event::BidAsk(quote) => {
            let token_id = token_ids
                .get(&quote.asset_id)
                .ok_or_else(|| std::io::Error::other("Quote received before token registration"))?;
            connection.execute(
                "INSERT INTO market_bid_asks (market_token_id, best_bid, best_ask, spread, source_at, received_at)
                 VALUES (?, ?, ?, ?, ?, ?)",
                params![*token_id, quote.best_bid.to_string(), quote.best_ask.to_string(),
                    quote.spread.to_string(), quote.timestamp, received_at],
            ).await?;
        }
        Event::Trade(trade) => {
            let token_id = token_ids
                .get(&trade.asset_id)
                .ok_or_else(|| std::io::Error::other("Trade received before token registration"))?;
            let side = match trade.side {
                Some(Side::Buy) => Some("buy"),
                Some(Side::Sell) => Some("sell"),
                None => None,
                _ => return Err(std::io::Error::other("Unknown trade side").into()),
            };
            connection.execute(
                "INSERT INTO market_trades (market_token_id, price, size, side, fee_rate_bps, source_at, received_at)
                 VALUES (?, ?, ?, ?, ?, ?, ?)",
                params![*token_id, trade.price.to_string(), trade.size.map(|v| v.to_string()), side,
                    trade.fee_rate_bps.map(|v| v.to_string()), trade.timestamp, received_at],
            ).await?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{Event, Message, load_all, setup, writer};
    use crate::shared::MarketContext;
    use polymarket_client_sdk_v2::types::{B256, U256};
    use serde_json::json;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn writes_all_event_types_and_reuses_token_ids() {
        let connection = setup(":memory:").await.unwrap();
        let (sender, receiver) = mpsc::channel(16);
        let market = MarketContext {
            asset: "btc".into(),
            slug: "btc-updown-5m-300".into(),
            condition_id: B256::ZERO,
            up_token_id: U256::from(1),
            down_token_id: U256::from(2),
            start_time: 300,
            end_time: 600,
        };
        let quote = serde_json::from_value(json!({
            "asset_id": "1", "market": B256::ZERO.to_string(),
            "best_bid": "0.45", "best_ask": "0.46", "spread": "0.01", "timestamp": "300001"
        }))
        .unwrap();
        let trade = serde_json::from_value(json!({
            "asset_id": "2", "market": B256::ZERO.to_string(),
            "price": "0.55", "size": "12.5", "side": "BUY", "timestamp": "300002"
        }))
        .unwrap();
        let binance = serde_json::from_value(json!({
            "symbol": "btcusdt", "value": "81234.12345678", "timestamp": 300003
        }))
        .unwrap();
        let chainlink = serde_json::from_value(json!({
            "symbol": "btc/usd", "value": "81230.98765432", "timestamp": 300004
        }))
        .unwrap();
        for event in [
            Event::Market(market.clone()),
            Event::Market(market),
            Event::BidAsk(quote),
            Event::Trade(trade),
            Event::Binance(binance),
            Event::Chainlink(chainlink),
        ] {
            let message = Message {
                received_at: 300005,
                event,
            };
            sender.send(message).await.unwrap();
        }
        drop(sender);
        writer(connection.clone(), receiver).await.unwrap();

        let mut reader = connection.clone();
        let loaded = load_all(&mut reader).await.unwrap();
        assert_eq!(loaded.len(), 5);
        assert!(
            loaded
                .windows(2)
                .all(|pair| pair[0].received_at <= pair[1].received_at)
        );
        for message in &loaded {
            assert_eq!(message.received_at, 300005);
            match &message.event {
                Event::Market(market) => {
                    assert_eq!(market.start_time, 300);
                    assert_eq!(market.end_time, 600);
                    assert_eq!(market.up_token_id, U256::from(1));
                    assert_eq!(market.down_token_id, U256::from(2));
                }
                Event::BidAsk(quote) => {
                    assert_eq!(quote.asset_id, U256::from(1));
                    assert_eq!(quote.market, B256::ZERO);
                    assert_eq!(quote.best_bid.to_string(), "0.45");
                    assert_eq!(quote.timestamp, 300001);
                }
                Event::Trade(trade) => {
                    assert_eq!(trade.asset_id, U256::from(2));
                    assert_eq!(trade.size.unwrap().to_string(), "12.5");
                    assert!(trade.fee_rate_bps.is_none());
                    assert_eq!(
                        trade.side,
                        Some(polymarket_client_sdk_v2::clob::types::Side::Buy)
                    );
                }
                Event::Binance(price) => assert_eq!(price.value.to_string(), "81234.12345678"),
                Event::Chainlink(price) => assert_eq!(price.value.to_string(), "81230.98765432"),
            }
        }
        for (table, count) in [
            ("markets", 1),
            ("market_tokens", 2),
            ("market_bid_asks", 1),
            ("market_trades", 1),
            ("prices_binance", 1),
            ("prices_chainlink", 1),
        ] {
            let mut rows = connection
                .query(format!("SELECT count(*) FROM {table}"), ())
                .await
                .unwrap();
            assert_eq!(
                rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
                count
            );
        }
        let mut rows = connection
            .query(
                "SELECT t.outcome, q.best_bid, q.received_at, m.starts_at FROM market_bid_asks q
             JOIN market_tokens t ON t.id = q.market_token_id JOIN markets m ON m.id = t.market_id",
                (),
            )
            .await
            .unwrap();
        let row = rows.next().await.unwrap().unwrap();
        assert_eq!(row.get::<String>(0).unwrap(), "up");
        assert_eq!(row.get::<String>(1).unwrap(), "0.45");
        assert_eq!(row.get::<i64>(2).unwrap(), 300005);
        assert_eq!(row.get::<i64>(3).unwrap(), 300000);
        let mut rows = connection
            .query("SELECT price FROM prices_binance", ())
            .await
            .unwrap();
        assert_eq!(
            rows.next()
                .await
                .unwrap()
                .unwrap()
                .get::<String>(0)
                .unwrap(),
            "81234.12345678"
        );
        let mut rows = connection
            .query("PRAGMA foreign_key_check", ())
            .await
            .unwrap();
        assert!(rows.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn loads_empty_database() {
        let mut connection = setup(":memory:").await.unwrap();
        assert!(load_all(&mut connection).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn loads_in_receipt_timestamp_order() {
        let mut connection = setup(":memory:").await.unwrap();
        connection
            .execute_batch(
                "INSERT INTO prices_binance (symbol, asset, price, source_at, received_at)
             VALUES ('btcusdt', 'btc', '1.23', 999, 30), ('ethusdt', 'eth', '4.56', 1, 10);
             INSERT INTO prices_chainlink (symbol, asset, price, source_at, received_at)
             VALUES ('btc/usd', 'btc', '7.89', 0, 20);",
            )
            .await
            .unwrap();
        let messages = load_all(&mut connection).await.unwrap();
        assert_eq!(
            messages.iter().map(|m| m.received_at).collect::<Vec<_>>(),
            [10, 20, 30]
        );
        assert!(matches!(messages[1].event, Event::Chainlink(_)));
    }

    #[tokio::test]
    async fn initializes_schema_and_can_run_again() {
        let connection = setup(":memory:").await.unwrap();
        connection
            .execute_batch(include_str!("../../schema.sql"))
            .await
            .unwrap();

        let mut rows = connection.query(
            "SELECT name FROM sqlite_schema WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
            (),
        ).await.unwrap();
        let mut names = Vec::new();
        while let Some(row) = rows.next().await.unwrap() {
            names.push(row.get::<String>(0).unwrap());
        }
        assert_eq!(
            names,
            [
                "market_bid_asks",
                "market_tokens",
                "market_trades",
                "markets",
                "prices_binance",
                "prices_chainlink"
            ]
        );

        let mut rows = connection.query("PRAGMA foreign_keys", ()).await.unwrap();
        assert_eq!(
            rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
            1
        );

        let mut rows = connection
            .query("SELECT count(*) FROM markets", ())
            .await
            .unwrap();
        assert_eq!(
            rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
            0
        );
    }
}
