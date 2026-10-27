pub mod database;
pub mod logging;
pub mod resolutions;

use polymarket_client_sdk_v2::types::{B256, U256};

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
