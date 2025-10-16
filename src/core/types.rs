use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct EvmBlockWithdrawal {
    #[serde(rename = "index")]
    pub index: String,
    pub validator_index: String,
    pub address: String,
    pub amount: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct EvmBlockData {
    pub base_fee_per_gas: String,
    pub difficulty: String,
    pub extra_data: String,
    pub gas_limit: String,
    pub gas_used: String,
    pub hash: String,
    pub logs_bloom: String,
    pub miner: String,
    pub mix_hash: String,
    pub nonce: String,
    pub number: String,
    pub parent_hash: String,
    pub receipts_root: String,
    pub sha3_uncles: String,
    pub size: String,
    pub state_root: String,
    pub timestamp: String,
    pub total_difficulty: String,
    pub transactions: Vec<String>,
    pub transactions_root: String,
    pub uncles: Vec<String>,
    pub withdrawals: Vec<EvmBlockWithdrawal>,
    pub withdrawals_root: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct EvmLogEntry {
    pub address: String,
    pub topics: Vec<String>,
    pub data: String,
    pub block_number: String,
    pub transaction_hash: String,
    pub transaction_index: String,
    pub block_hash: String,
    pub log_index: String,
    pub removed: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct EvmBlockReceipts {
    pub block_hash: String,
    pub block_number: String,
    pub contract_address: Option<String>,
    pub cumulative_gas_used: String,
    pub effective_gas_price: String,
    pub from: String,
    pub gas_used: String,
    pub logs: Vec<EvmLogEntry>,
    pub logs_bloom: String,
    pub status: String,
    pub to: String,
    pub transaction_hash: String,
    pub transaction_index: String,
    #[serde(rename = "type")]
    pub type_field: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct EvmBlockTraceLogs {
    pub action: EvmBlockTraceAction,
    #[serde(rename = "blockHash")]
    pub block_hash: String,
    #[serde(rename = "blockNumber")]
    pub block_number: i64,
    pub result: Option<EvmBlockTraceResult>,
    pub subtraces: i32,
    #[serde(rename = "traceAddress")]
    pub trace_address: Vec<i32>,
    #[serde(rename = "transactionHash")]
    pub transaction_hash: String,
    #[serde(rename = "transactionPosition")]
    pub transaction_position: i32,
    #[serde(rename = "type")]
    pub type_field: String,
    pub error: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct EvmBlockTraceAction {
    #[serde(rename = "callType")]
    pub call_type: Option<String>,
    pub from: Option<String>,
    pub gas: Option<String>,
    pub input: Option<String>,
    pub to: Option<String>,
    pub value: Option<String>,
    pub init: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct EvmBlockTraceResult {
    #[serde(rename = "gasUsed")]
    pub gas_used: Option<String>,
    pub output: Option<String>,
    pub address: Option<String>,
    pub code: Option<String>,
}
