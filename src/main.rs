use futures::StreamExt;
use polymarket_client_sdk_v2::{
    clob::ws::Client as ClobClient,
    gamma::{Client as GammaClient, types::request::MarketBySlugRequest},
    rtds::{ChainlinkPrice, Client, CryptoPrice},
    types::U256,
};
use std::time::{SystemTime, UNIX_EPOCH};

#[tokio::main]
async fn main() -> polymarket_client_sdk_v2::Result<()> {
    let gamma = GammaClient::default();
    let client = ClobClient::default();
    let btc_token_ids = get_token_ids(&gamma, "btc").await;
    let eth_token_ids = get_token_ids(&gamma, "eth").await;
    let sol_token_ids = get_token_ids(&gamma, "sol").await;

    tokio::try_join!(
        stream_binance_prices(),
        stream_chainlink_prices(),
        stream_best_bid_ask(&client, &btc_token_ids),
        stream_best_bid_ask(&client, &eth_token_ids),
        stream_best_bid_ask(&client, &sol_token_ids),
        stream_last_trade_price(&client, &btc_token_ids),
        stream_last_trade_price(&client, &eth_token_ids),
        stream_last_trade_price(&client, &sol_token_ids)
    )?;

    Ok(())
}

async fn get_token_ids(gamma: &GammaClient, asset: &str) -> Vec<U256> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let asset = asset.trim().to_ascii_lowercase();
    let slug = format!("{asset}-updown-5m-{}", now / 300 * 300);
    let market = gamma
        .market_by_slug(&MarketBySlugRequest::builder().slug(slug).build())
        .await
        .unwrap();
    println!("{market:#?}");

    let outcomes = market.outcomes.unwrap();
    let token_ids = market.clob_token_ids.unwrap();
    assert_eq!(outcomes.len(), 2);
    assert_eq!(token_ids.len(), 2);

    token_ids
}

async fn stream_best_bid_ask(
    client: &ClobClient,
    token_ids: &[U256],
) -> polymarket_client_sdk_v2::Result<()> {
    let quotes = client.subscribe_best_bid_ask(token_ids.to_vec())?;
    tokio::pin!(quotes);

    while let Some(quote) = quotes.next().await {
        let quote = quote?;
        println!("{quote:#?}");
    }

    Ok(())
}

async fn stream_last_trade_price(
    client: &ClobClient,
    token_ids: &[U256],
) -> polymarket_client_sdk_v2::Result<()> {
    let trades = client.subscribe_last_trade_price(token_ids.to_vec())?;
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
