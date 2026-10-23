use futures::StreamExt;
use polymarket_client_sdk_v2::{
    clob::ws::Client as ClobClient,
    gamma::{Client as GammaClient, types::request::MarketBySlugRequest},
    rtds::{ChainlinkPrice, Client, CryptoPrice},
    types::U256,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[tokio::main]
async fn main() -> polymarket_client_sdk_v2::Result<()> {
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

async fn stream_15m_rolling_5m_market_window(
    gamma: &GammaClient,
    client: &ClobClient,
    asset: &str,
) -> polymarket_client_sdk_v2::Result<()> {
    loop {
        let start = current_market_start();
        let curr = get_token_ids(gamma, asset, start).await;
        let next1 = get_token_ids(gamma, asset, start + 300).await;
        let next2 = get_token_ids(gamma, asset, start + 600).await;
        let end = UNIX_EPOCH + Duration::from_secs(start + 300);
        let remaining = end.duration_since(SystemTime::now()).unwrap_or_default();

        let result = tokio::select! {
            biased;
            _ = tokio::time::sleep(remaining) => Ok(()),
            result = async {
                tokio::try_join!(
                    stream_best_bid_ask(client, &curr),
                    stream_best_bid_ask(client, &next1),
                    stream_best_bid_ask(client, &next2),
                    stream_last_trade_price(client, &curr),
                    stream_last_trade_price(client, &next1),
                    stream_last_trade_price(client, &next2)
                ).map(|_| ())
            } => result,
        };

        for token_ids in [&curr, &next1, &next2] {
            client.unsubscribe_orderbook(token_ids)?;
            client.unsubscribe_orderbook(token_ids)?;
        }
        result?;
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

async fn get_token_ids(gamma: &GammaClient, asset: &str, start: u64) -> Vec<U256> {
    let asset = asset.trim().to_ascii_lowercase();
    let slug = format!("{asset}-updown-5m-{start}");
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
