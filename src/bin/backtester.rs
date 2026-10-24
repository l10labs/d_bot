use d_bot::shared::database::{self, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let mut connection = database::setup("markets.db").await?;
    let messages = database::load_all(&mut connection).await?;

    for message in messages.iter().take(100) {
        println!("{message:?}");
    }

    Ok(())
}
