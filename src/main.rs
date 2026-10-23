use futures::StreamExt;
use polymarket_client_sdk_v2::{
    clob::ws::Client as ClobClient,
    gamma::{Client as GammaClient, types::request::MarketBySlugRequest},
    rtds::{ChainlinkPrice, Client, CryptoPrice},
    types::{B256, U256},
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

mod database;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _database = database::setup("markets_5m.db").await?;
    let gamma = GammaClient::default();
    let client = ClobClient::default();

    tokio::try_join!(
        stream_binance_prices(),
        stream_chainlink_prices(),
        stream_15m_rolling_5m_market_window(&gamma, &client, "btc"),
        stream_15m_rolling_5m_market_window(&gamma, &client, "eth"),
        stream_15m_rolling_5m_market_window(&gamma, &client, "sol")
    )?;

    Ok(())
}

#[derive(Debug)]
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
) -> polymarket_client_sdk_v2::Result<()> {
    let start = current_market_start();
    let mut curr = get_market_context(gamma, asset, start).await;
    let mut next = get_market_context(gamma, asset, start + 300).await;

    loop {
        let end = UNIX_EPOCH + Duration::from_secs(curr.end_time);
        let remaining = end.duration_since(SystemTime::now()).unwrap_or_default();

        let result = tokio::select! {
            biased;
            _ = tokio::time::sleep(remaining) => Ok(()),
            result = async {
                tokio::try_join!(
                    stream_best_bid_ask(client, &curr),
                    stream_last_trade_price(client, &curr)
                ).map(|_| ())
            } => result,
        };

        let token_ids = [curr.up_token_id, curr.down_token_id];
        client.unsubscribe_orderbook(&token_ids)?;
        client.unsubscribe_orderbook(&token_ids)?;
        result?;

        curr = next;
        next = get_market_context(gamma, asset, curr.end_time).await;
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

async fn get_market_context(gamma: &GammaClient, asset: &str, start: u64) -> MarketContext {
    let asset = asset.trim().to_ascii_lowercase();
    let slug = format!("{asset}-updown-5m-{start}");
    let market = gamma
        .market_by_slug(&MarketBySlugRequest::builder().slug(slug.clone()).build())
        .await
        .unwrap();
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
    println!("{context:#?}");
    context
}

async fn stream_best_bid_ask(
    client: &ClobClient,
    market: &MarketContext,
) -> polymarket_client_sdk_v2::Result<()> {
    let quotes = client.subscribe_best_bid_ask(vec![market.up_token_id, market.down_token_id])?;
    tokio::pin!(quotes);

    while let Some(quote) = quotes.next().await {
        let quote = quote?;
        println!("{quote:#?}");
    }

    Ok(())
}

async fn stream_last_trade_price(
    client: &ClobClient,
    market: &MarketContext,
) -> polymarket_client_sdk_v2::Result<()> {
    let trades =
        client.subscribe_last_trade_price(vec![market.up_token_id, market.down_token_id])?;
    tokio::pin!(trades);

    while let Some(trade) = trades.next().await {
        let trade = trade?;
        println!("{trade:#?}");
    }

    Ok(())
}

async fn stream_binance_prices() -> polymarket_client_sdk_v2::Result<()> {
    let client = Client::default();
    let prices = client.subscribe_crypto_prices(None)?;
    tokio::pin!(prices);

    while let Some(price) = prices.next().await {
        let price: CryptoPrice = price?;
        println!("{price:#?}");
    }

    Ok(())
}

async fn stream_chainlink_prices() -> polymarket_client_sdk_v2::Result<()> {
    let client = Client::default();
    let prices = client.subscribe_chainlink_prices(None)?;
    tokio::pin!(prices);

    while let Some(price) = prices.next().await {
        let price: ChainlinkPrice = price?;
        println!("{price:#?}");
    }

    Ok(())
}
