//! Poll persisted, ended markets; closure alone is not evidence of resolution.
//! Resolution records are metadata, deliberately separate from tick-event replay.
use super::database::Result;
use polymarket_client_sdk_v2::clob::Client;
use std::time::Duration;
use turso::{Connection, params};

pub async fn poll(connection: Connection) -> Result<()> {
    let client = Client::default();
    let mut interval = tokio::time::interval(Duration::from_secs(60));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        interval.tick().await;
        if let Err(error) = sweep(&connection, &client).await {
            tracing::warn!(%error, "Resolution sweep failed; retrying next minute");
        }
    }
}

async fn sweep(connection: &Connection, client: &Client) -> Result<()> {
    let mut rows = connection
        .query(
            "SELECT m.id, m.condition_id FROM markets m
         LEFT JOIN market_resolutions r ON r.market_id = m.id
         WHERE m.ends_at <= ? AND r.market_id IS NULL ORDER BY m.ends_at",
            params![chrono::Utc::now().timestamp_millis()],
        )
        .await?;
    let mut pending = Vec::new();
    while let Some(row) = rows.next().await? {
        pending.push((row.get::<i64>(0)?, row.get::<String>(1)?));
    }
    drop(rows);
    for (market_id, condition_id) in pending {
        // Bound each request and pace the backlog so polling cannot flood the API.
        let response =
            tokio::time::timeout(Duration::from_secs(15), client.market(&condition_id)).await;
        match response {
            Ok(Ok(market)) => {
                let winners: Vec<_> = market.tokens.iter().filter(|token| token.winner).collect();
                if market.closed
                    && winners.len() == 1
                    && market.condition_id.map(|id| id.to_string()).as_deref()
                        == Some(condition_id.as_str())
                {
                    let changed = connection.execute(
                        "INSERT INTO market_resolutions (market_id, winning_token_id, observed_at, source)
                         SELECT ?, id, ?, 'clob' FROM market_tokens WHERE market_id = ? AND token_id = ?
                         ON CONFLICT(market_id) DO NOTHING",
                        params![market_id, chrono::Utc::now().timestamp_millis(), market_id,
                            winners[0].token_id.to_string()],
                    ).await?;
                    if changed > 0 {
                        tracing::info!(%condition_id, outcome = %winners[0].outcome, "Market resolution stored");
                    }
                }
            }
            Ok(Err(error)) => tracing::warn!(%condition_id, %error, "Resolution lookup failed"),
            Err(error) => tracing::warn!(%condition_id, %error, "Resolution lookup timed out"),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Ok(())
}
