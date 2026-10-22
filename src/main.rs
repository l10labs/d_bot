use futures::StreamExt;
use polymarket_client_sdk_v2::rtds::{ChainlinkPrice, Client, CryptoPrice};

#[tokio::main]
async fn main() -> polymarket_client_sdk_v2::Result<()> {
    tokio::try_join!(stream_binance_prices(), stream_chainlink_prices())?;
    Ok(())
}

async fn stream_binance_prices() -> polymarket_client_sdk_v2::Result<()> {
    let client = Client::default();
    let prices = client.subscribe_crypto_prices(None)?;
    tokio::pin!(prices);

    while let Some(price) = prices.next().await {
        let price: CryptoPrice = price?;
        println!("{price:?}");
    }

    Ok(())
}

async fn stream_chainlink_prices() -> polymarket_client_sdk_v2::Result<()> {
    let client = Client::default();
    let prices = client.subscribe_chainlink_prices(None)?;
    tokio::pin!(prices);

    while let Some(price) = prices.next().await {
        let price: ChainlinkPrice = price?;
        println!("{price:?}");
    }

    Ok(())
}
