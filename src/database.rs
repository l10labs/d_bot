use crate::MarketContext;
use polymarket_client_sdk_v2::{
    clob::{
        types::Side,
        ws::types::response::{BestBidAsk, LastTradePrice},
    },
    rtds::{ChainlinkPrice, CryptoPrice},
    types::U256,
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
            include_str!("../schema.sql"),
            "\nCOMMIT;"
        ))
        .await?;
    Ok(connection)
}

pub async fn writer(
    mut connection: Connection,
    mut receiver: mpsc::Receiver<Message>,
) -> Result<()> {
    let mut token_ids = HashMap::new();
    let mut batch = Vec::with_capacity(256);
    let mut labels = HashMap::new();

    while receiver.recv_many(&mut batch, 256).await > 0 {
        let transaction = connection.transaction().await?;
        for message in &batch {
            write_event(&transaction, &mut token_ids, message).await?;
        }
        transaction.commit().await?;
        for message in batch.drain(..) {
            crate::logging::event(&message, &mut labels);
        }
    }

    Ok(())
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
    use super::{Event, Message, setup, writer};
    use crate::MarketContext;
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
    async fn initializes_schema_and_can_run_again() {
        let connection = setup(":memory:").await.unwrap();
        connection
            .execute_batch(include_str!("../schema.sql"))
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
