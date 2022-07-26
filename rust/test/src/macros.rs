use std::convert::TryInto;
use hex_literal::hex;
use num_bigint::{BigUint, TryFromBigIntError};
use crate::erc721;
use crate::eth;
use substreams::{
    store, errors, Hex, log
};


const TRACKED_CONTRACT: [u8; 20] = hex!("bc4ca0eda7647a8ab7c2061c2e118a18a936f13d");
/// keccak value for Transfer(address,address,uint256)
const TRANSFER_TOPIC: [u8; 32] =
    hex!("ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef");
pub fn is_erc721transfer_event(log: &eth::Log) -> bool {
    if log.topics.len() != 4 || log.data.len() != 0 {
        return false;
    }

    return log.topics[0] == TRANSFER_TOPIC;
}

fn generate_key(holder: &[u8]) -> String {
    return format!(
        "total:{}:{}",
        Hex::encode(holder),
        Hex::encode(&TRACKED_CONTRACT)
    );
}

#[substreams::handlers::store]
#[precondition(false)]
fn store_nfts(
    transfers: erc721::Transfers,
    pairs: &impl store::StoreGet,
    tokens: &impl store::StoreGet,
    output: &impl store::StoreAddInt64,
) {

    // let p: &dyn store::StoreGet = &store::ExternStoreGet::new(1);

    let tokens_first_opt = tokens.get_first(&"tokens".to_owned());
    let pairs_last_opt = pairs.get_first(&"pairs".to_owned());
    log::info!("tokens {:?} pairs {:?}", tokens_first_opt, pairs_last_opt);
    for transfer in transfers.transfers {
        if hex::encode(&transfer.from) != "0000000000000000000000000000000000000000" {
            log::info!("found a transfer");
            output.add(transfer.ordinal, generate_key(transfer.from.as_ref()), -1);
        }
        if hex::encode(&transfer.to) != "0000000000000000000000000000000000000000" {
            output.add(transfer.ordinal, generate_key(transfer.to.as_ref()), 1);
        }
    }
}

#[substreams::handlers::map]
fn map_transfers(blk: eth::Block) -> Result<erc721::Transfers, errors::Error > {
    let mut transfers: Vec<erc721::Transfer> = vec![];

    for trx in blk.transaction_traces {
        transfers.extend(trx.receipt.as_ref().unwrap().logs.iter().filter_map(|log| {
            if log.address != TRACKED_CONTRACT {
                return None;
            }

            log::debug!("NFT Contract {} invoked", Hex(&TRACKED_CONTRACT));

            if !is_erc721transfer_event(log) {
                return None;
            }

            let token_id: Result<u64, TryFromBigIntError<BigUint>> =
                BigUint::from_bytes_be(&log.topics[3]).try_into();

            match token_id {
                Ok(token_id) => Some(erc721::Transfer {
                    trx_hash: trx.hash.clone(),
                    from: Vec::from(&log.topics[1][12..]),
                    to: Vec::from(&log.topics[2][12..]),
                    token_id,
                    ordinal: log.block_index as u64,
                }),
                Err(e) => {
                    log::info!(
                        "The token_id value {} does not fit in a 64 bits unsigned integer: {}",
                        Hex(&log.topics[3]),
                        e
                    );

                    None
                }
            }
        }));
    }
    return Ok(erc721::Transfers { transfers });
}

// pub struct TestStoreGet {
// }
//
// impl StoreGet for TestStoreGet {
//     fn get_at(&self, ord: u64, key: &String) -> Option<Vec<u8>> {
//     }
//
//     fn get_last(&self, key: &String) -> Option<Vec<u8>> {
//     }
//
//     fn get_first(&self, key: &String) -> Option<Vec<u8>> {
//     }
// }

#[cfg(test)]
mod tests {
//     // Note this useful idiom: importing names from outer (for mod tests) scope.
//     use super::*;
//
//
//     impl StoreGet for ExternStoreGet {
//         /// Allows you to read a single key from the store. The type
//         /// of its value can be anything, and is usually declared in
//         /// the output section of the manifest. The ordinal is used here
//         /// to go query a key that might have changed mid-block by
//         /// the store module that built it.
//         fn get_at(&self, ord: u64, key: &String) -> Option<Vec<u8>> {
//             return state::get_at(self.idx, ord as i64, key);
//         }
//
//         /// Retrieves a key from the store, like `get_at`, but querying the state of
//         /// the store as of the beginning of the block being processed, before any changes
//         /// were applied within the current block. Tt does not need to rewind any changes
//         /// in the middle of the block.
//         fn get_last(&self, key: &String) -> Option<Vec<u8>> {
//             return state::get_last(self.idx, key);
//         }
//
//         /// Retrieves a key from the store, like `get_at`, but querying the state of
//         /// the store as of the beginning of the block being processed, before any changes
//         /// were applied within the current block. However, it needs to unwind any keys that
//         /// would have changed mid-block, so will be slightly less performant.
//         fn get_first(&self, key: &String) -> Option<Vec<u8>> {
//             return state::get_first(self.idx, key);
//         }
//     }
//
//
    #[test]
    fn test_wtf() {
        assert_eq!("wtf: 1", "toot");
    }
}