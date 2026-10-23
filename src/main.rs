use database::{Event, Result};
use futures::StreamExt;
use polymarket_client_sdk_v2::{
    clob::ws::Client as ClobClient,
    gamma::{Client as GammaClient, types::request::MarketBySlugRequest},
    rtds::{ChainlinkPrice, Client, CryptoPrice},
    types::{B256, U256},
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;

mod database;
mod logging;

#[tokio::main]
async fn main() -> Result<()> {
    let _logging = logging::setup();
    let connection = database::setup("markets_5m.db").await?;
    let (sender, receiver) = mpsc::channel(25_600);
    let mut writer = tokio::spawn(database::writer(connection, receiver));

    let gamma = GammaClient::default();
    let client = ClobClient::default();

    let result = tokio::select! {
        result = async {
            tokio::try_join!(
                stream_binance_prices(&sender),
                stream_chainlink_prices(&sender),
                stream_15m_rolling_5m_market_window(&gamma, &client, "btc", &sender),
                stream_15m_rolling_5m_market_window(&gamma, &client, "eth", &sender),
                stream_15m_rolling_5m_market_window(&gamma, &client, "sol", &sender),
                stream_15m_rolling_5m_market_window(&gamma, &client, "xrp", &sender),
                stream_15m_rolling_5m_market_window(&gamma, &client, "bnb", &sender),
                stream_15m_rolling_5m_market_window(&gamma, &client, "doge", &sender),
                stream_15m_rolling_5m_market_window(&gamma, &client, "hype", &sender),
            ).map(|_| ())
        } => result,
        result = tokio::signal::ctrl_c() => result.map_err(Into::into),
        result = &mut writer => {
            result??;
            return Err(std::io::Error::other("Database writer stopped unexpectedly").into());
        }
    };

    drop(sender);
    writer.await??;
    result
}

#[derive(Debug, Clone)]
pub struct MarketContext {
    pub asset: String,
    pub slug: String,
    pub condition_id: B256,
    pub up_token_id: U256,
    pub down_token_id: U256,
    pub start_time: u64,
    pub end_time: u64,
}

async fn stream_15m_rolling_5m_market_window(
    gamma: &GammaClient,
    client: &ClobClient,
    asset: &str,
    sender: &mpsc::Sender<database::Message>,
) -> Result<()> {
    let start = current_market_start();
    let mut curr = get_market_context(gamma, asset, start, sender).await?;
    let mut next = get_market_context(gamma, asset, start + 300, sender).await?;

    loop {
        let end = UNIX_EPOCH + Duration::from_secs(curr.end_time);
        let remaining = end.duration_since(SystemTime::now()).unwrap_or_default();

        let result = tokio::select! {
            biased;
            _ = tokio::time::sleep(remaining) => Ok(()),
            result = async {
                tokio::try_join!(
                    stream_best_bid_ask(client, &curr, sender),
                    stream_last_trade_price(client, &curr, sender)
                ).map(|_| ())
            } => result,
        };

        let token_ids = [curr.up_token_id, curr.down_token_id];
        client.unsubscribe_orderbook(&token_ids)?;
        client.unsubscribe_orderbook(&token_ids)?;
        result?;

        curr = next;
        next = get_market_context(gamma, asset, curr.end_time, sender).await?;
    }
}

fn current_market_start() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        / 300
        * 300
}

async fn get_market_context(
    gamma: &GammaClient,
    asset: &str,
    start: u64,
    sender: &mpsc::Sender<database::Message>,
) -> Result<MarketContext> {
    let asset = asset.trim().to_ascii_lowercase();
    let slug = format!("{asset}-updown-5m-{start}");
    let market = gamma
        .market_by_slug(&MarketBySlugRequest::builder().slug(slug.clone()).build())
        .await
        .unwrap();
    let received_at = received_at_ms();
    let outcomes = market.outcomes.unwrap();
    let token_ids = market.clob_token_ids.unwrap();
    assert_eq!(outcomes.len(), 2);
    assert_eq!(token_ids.len(), 2);

    let up = outcomes
        .iter()
        .position(|outcome| outcome.eq_ignore_ascii_case("up"))
        .unwrap();
    let down = outcomes
        .iter()
        .position(|outcome| outcome.eq_ignore_ascii_case("down"))
        .unwrap();

    let context = MarketContext {
        asset,
        slug,
        condition_id: market.condition_id.unwrap(),
        up_token_id: token_ids[up],
        down_token_id: token_ids[down],
        start_time: start,
        end_time: start + 300,
    };
    sender
        .send(database::Message {
            received_at,
            event: Event::Market(context.clone()),
        })
        .await?;
    Ok(context)
}

async fn stream_best_bid_ask(
    client: &ClobClient,
    market: &MarketContext,
    sender: &mpsc::Sender<database::Message>,
) -> Result<()> {
    let quotes = client.subscribe_best_bid_ask(vec![market.up_token_id, market.down_token_id])?;
    tokio::pin!(quotes);

    while let Some(quote) = quotes.next().await {
        let quote = quote?;
        let received_at = received_at_ms();
        sender
            .send(database::Message {
                received_at,
                event: Event::BidAsk(quote),
            })
            .await?;
    }

    Ok(())
}

async fn stream_last_trade_price(
    client: &ClobClient,
    market: &MarketContext,
    sender: &mpsc::Sender<database::Message>,
) -> Result<()> {
    let trades =
        client.subscribe_last_trade_price(vec![market.up_token_id, market.down_token_id])?;
    tokio::pin!(trades);

    while let Some(trade) = trades.next().await {
        let trade = trade?;
        let received_at = received_at_ms();
        sender
            .send(database::Message {
                received_at,
                event: Event::Trade(trade),
            })
            .await?;
    }

    Ok(())
}

async fn stream_binance_prices(sender: &mpsc::Sender<database::Message>) -> Result<()> {
    let client = Client::default();
    let prices = client.subscribe_crypto_prices(None)?;
    tokio::pin!(prices);

    while let Some(price) = prices.next().await {
        let price: CryptoPrice = price?;
        let received_at = received_at_ms();
        sender
            .send(database::Message {
                received_at,
                event: Event::Binance(price),
            })
            .await?;
    }

    Ok(())
}

async fn stream_chainlink_prices(sender: &mpsc::Sender<database::Message>) -> Result<()> {
    let client = Client::default();
    let prices = client.subscribe_chainlink_prices(None)?;
    tokio::pin!(prices);

    while let Some(price) = prices.next().await {
        let price: ChainlinkPrice = price?;
        let received_at = received_at_ms();
        sender
            .send(database::Message {
                received_at,
                event: Event::Chainlink(price),
            })
            .await?;
    }

    Ok(())
}

fn received_at_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .try_into()
        .unwrap()
}
