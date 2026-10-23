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

#[cfg(test)]
mod tests {
    use super::setup;

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
                "binance_prices",
                "chainlink_prices",
                "market_bid_asks",
                "market_tokens",
                "market_trades",
                "markets"
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
