//! Store Implementation for Substreams.
//!
//! This crate implements the different Stores which can be used in your Substreams
//! handlers.
//!

use crate::pb;
use crate::state;
use bigdecimal::BigDecimal;
use num_bigint::BigInt;
use substreams_macro::StoreWriter;

/// Delta is a struct that defined StoreDeltas
pub type Deltas = Vec<pb::substreams::StoreDelta>;

pub trait StoreDeletePrefix {
    fn delete_prefix(&self, ord: i64, prefix: &String);
}
/// StoreSet is a struct representing a `store` with
/// `updatePolicy` equal to `set`
pub trait StoreSet {
    fn set(&self, ord: u64, key: String, value: &Vec<u8>);
    fn set_many(&self, ord: u64, keys: &Vec<String>, value: &Vec<u8>);
}

#[derive(StoreWriter)]
pub struct ExternStoreSet {}
impl StoreSet for ExternStoreSet {
    /// Set a given key to a given value, if the key existed before, it will be replaced.
    fn set(&self, ord: u64, key: String, value: &Vec<u8>) {
        state::set(ord as i64, key, value);
    }

    /// Set many keys to a given values, if the key existed before, it will be replaced.
    fn set_many(&self, ord: u64, keys: &Vec<String>, value: &Vec<u8>) {
        for key in keys {
            state::set(ord as i64, key.to_string(), value);
        }
    }
}

/// StoreSetIfNotExists is a struct representing a `store` module with
/// `updatePolicy` equal to `set_if_not_exists`
pub trait StoreSetIfNotExists {
    fn set_if_not_exists(&self, ord: u64, key: String, value: &Vec<u8>);
    fn set_if_not_exists_many(&self, ord: u64, keys: &Vec<String>, value: &Vec<u8>);
}

#[derive(StoreWriter)]
pub struct ExternStoreSetIfNotExists {}
impl StoreSetIfNotExists for ExternStoreSetIfNotExists {
    /// Set a given key to a given value, if the key existed before, it will be ignored and not set.
    fn set_if_not_exists(&self, ord: u64, key: String, value: &Vec<u8>) {
        state::set_if_not_exists(ord as i64, key, value);
    }

    /// Set given keys to given values, if the key existed before, it will be ignored and not set.
    fn set_if_not_exists_many(&self, ord: u64, keys: &Vec<String>, value: &Vec<u8>) {
        for key in keys {
            state::set_if_not_exists(ord as i64, key.to_string(), value);
        }
    }
}

/// StoreAddInt64 is a struct representing a `store` module with
/// `updatePolicy` equal to `add` and a valueType of `int64`
pub trait StoreAddInt64 {
    fn add(&self, ord: u64, key: String, value: i64);
    fn add_many(&self, ord: u64, keys: &Vec<String>, value: i64);
}

#[derive(StoreWriter)]
pub struct ExternStoreAddInt64 {}
impl StoreAddInt64 for ExternStoreAddInt64 {
    /// Will add the value to the already present value at the key (or default to
    /// zero if the key was not set)
    fn add(&self, ord: u64, key: String, value: i64) {
        state::add_int64(ord as i64, key, value);
    }

    /// Will add the value to the already present value of the keys (or default to
    /// zero if the key was not set)
    fn add_many(&self, ord: u64, keys: &Vec<String>, value: i64) {
        for key in keys {
            state::add_int64(ord as i64, key.to_string(), value);
        }
    }
}

/// StoreAddFloat64 is a struct representing a `store` module with
/// `updatePolicy` equal to `add` and a valueType of `float64`
pub trait StoreAddFloat64 {
    fn add(&self, ord: u64, key: String, value: f64);
    fn add_many(&self, ord: u64, keys: &Vec<String>, value: f64);
}

#[derive(StoreWriter)]
pub struct ExternStoreAddFloat64 {}
impl StoreAddFloat64 for ExternStoreAddFloat64 {
    /// Will add the value to the already present value at the key (or default to
    /// zero if the key was not set)
    fn add(&self, ord: u64, key: String, value: f64) {
        state::add_float64(ord as i64, key, value);
    }

    /// Will add the value to the already present value of the keys (or default to
    /// zero if the key was not set)
    fn add_many(&self, ord: u64, keys: &Vec<String>, value: f64) {
        for key in keys {
            state::add_float64(ord as i64, key.to_string(), value);
        }
    }
}

/// StoreAddBigFloat is a struct representing a `store` module with
/// `updatePolicy` equal to `add` and a valueType of `bigfloat`
pub trait StoreAddBigFloat {
    fn add(&self, ord: u64, key: String, value: &BigDecimal);
    fn add_many(&self, ord: u64, keys: &Vec<String>, value: &BigDecimal);
}

#[derive(StoreWriter)]
pub struct ExternStoreAddBigFloat {}
impl StoreAddBigFloat for ExternStoreAddBigFloat {
    /// Will add the value to the already present value at the key (or default to
    /// zero if the key was not set)
    fn add(&self, ord: u64, key: String, value: &BigDecimal) {
        state::add_bigfloat(ord as i64, key, value);
    }

    /// Will add the value to the already present value of the keys (or default to
    /// zero if the key was not set)
    fn add_many(&self, ord: u64, keys: &Vec<String>, value: &BigDecimal) {
        for key in keys {
            state::add_bigfloat(ord as i64, key.to_string(), value);
        }
    }
}

/// StoreAddBigInt is a struct representing a `store` module with
/// `updatePolicy` equal to `add` and a valueType of `bigint`
pub trait StoreAddBigInt {
    fn add(&self, ord: u64, key: String, value: &BigInt);
    fn add_many(&self, ord: u64, keys: &Vec<String>, value: &BigInt);
}

#[derive(StoreWriter)]
pub struct ExternStoreAddBigInt {}
impl StoreAddBigInt for ExternStoreAddBigInt {
    /// Will add the value to the already present value of the keys (or default to
    /// zero if the key was not set)
    fn add(&self, ord: u64, key: String, value: &BigInt) {
        state::add_bigint(ord as i64, key, value);
    }

    /// Will add the value to the already present value of the keys (or default to
    /// zero if the key was not set)
    fn add_many(&self, ord: u64, keys: &Vec<String>, value: &BigInt) {
        for key in keys {
            state::add_bigint(ord as i64, key.to_string(), value);
        }
    }
}

/// StoreMaxInt64 is a struct representing a `store` module with
/// `updatePolicy` equal to `max` and a valueType of `int64`
pub trait StoreMaxInt64 {
    fn max(&self, ord: u64, key: String, value: i64);
}

#[derive(StoreWriter)]
pub struct ExternStoreMaxInt64 {}
impl StoreMaxInt64 for ExternStoreMaxInt64{
    /// max will set the provided key in the store only if the value received in
    /// parameter is bigger than the one already present in the store, with
    /// a default of the zero value when the key is absent.
    fn max(&self, ord: u64, key: String, value: i64) {
        state::set_max_int64(ord as i64, key, value);
    }
}

/// StoreMaxBigInt is a struct representing a `store` module with
/// `updatePolicy` equal to `max` and a valueType of `bigint`
pub trait StoreMaxBigInt {
    fn max(&self, ord: u64, key: String, value: &BigInt);
}

#[derive(StoreWriter)]
pub struct ExternStoreMaxBigInt {}
impl StoreMaxBigInt for ExternStoreMaxBigInt {
    /// Will set the provided key in the store only if the value received in
    /// parameter is bigger than the one already present in the store, with
    /// a default of the zero value when the key is absent.
    fn max(&self, ord: u64, key: String, value: &BigInt) {
        state::set_max_bigint(ord as i64, key, value);
    }
}

/// StoreMaxFloat64 is a struct representing a `store` module with
/// `updatePolicy` equal to `max` and a valueType of `float64`
pub trait StoreMaxFloat64 {
    fn max(&self, ord: u64, key: String, value: f64);
}

#[derive(StoreWriter)]
pub struct ExternStoreMaxFloat64 {}
impl StoreMaxFloat64 for ExternStoreMaxFloat64{
    /// Will set the provided key in the store only if the value received in
    /// parameter is bigger than the one already present in the store, with
    /// a default of the zero value when the key is absent.
    fn max(&self, ord: u64, key: String, value: f64) {
        state::set_max_float64(ord as i64, key, value);
    }
}

/// StoreMaxBigFloat is a struct representing a `store` module with
/// `updatePolicy` equal to `max` and a valueType of `bigfloat`
pub trait StoreMaxBigFloat {
    fn max(&self, ord: u64, key: String, value: &BigDecimal);
}

#[derive(StoreWriter)]
pub struct ExternStoreMaxBigFloat {}
impl StoreMaxBigFloat for ExternStoreMaxBigFloat{
    /// Will set the provided key in the store only if the value received in
    /// parameter is bigger than the one already present in the store, with
    /// a default of the zero value when the key is absent.
    fn max(&self, ord: u64, key: String, value: &BigDecimal) {
        state::set_max_bigfloat(ord as i64, key, value);
    }
}

/// `StoreMinInt64` is a struct representing a `store` module with
/// `updatePolicy` equal to `min` and a valueType of `int64`
pub trait StoreMinInt64 {
    fn min(&self, ord: u64, key: String, value: i64);
}

#[derive(StoreWriter)]
pub struct ExternStoreMinInt64 {}
impl StoreMinInt64 for ExternStoreMinInt64 {
    /// Will set the provided key in the store only if the value received in
    /// parameter is smaller than the one already present in the store, with
    /// a default of the zero value when the key is absent.
    fn min(&self, ord: u64, key: String, value: i64) {
        state::set_min_int64(ord as i64, key, value);
    }
}

/// StoreMinBigInt is a struct representing a `store` module with
/// `updatePolicy` equal to `min` and a valueType of `bigint`
pub trait StoreMinBigInt {
    fn min(&self, ord: u64, key: String, value: &BigInt);
}

#[derive(StoreWriter)]
pub struct ExternStoreMinBigInt {}
impl StoreMinBigInt for ExternStoreMinBigInt {
    /// Will set the provided key in the store only if the value received in
    /// parameter is smaller than the one already present in the store, with
    /// a default of the zero value when the key is absent.
    fn min(&self, ord: u64, key: String, value: &BigInt) {
        state::set_min_bigint(ord as i64, key, value);
    }
}

/// StoreMinFloat64 is a struct representing a `store` module with
/// `updatePolicy` equal to `min` and a valueType of `float64`
pub trait StoreMinFloat64 {
    fn min(&self, ord: u64, key: String, value: f64);
}

#[derive(StoreWriter)]
pub struct ExternStoreMinFloat64 {}
impl StoreMinFloat64 for ExternStoreMinFloat64 {
    /// Will set the provided key in the store only if the value received in
    /// parameter is smaller than the one already present in the store, with
    /// a default of the zero value when the key is absent.
    fn min(&self, ord: u64, key: String, value: f64) {
        state::set_min_float64(ord as i64, key, value);
    }
}

/// StoreMinBigFloat is a struct representing a `store` module with
/// `updatePolicy` equal to `min` and a valueType of `bigfloat`
pub trait StoreMinBigFloat {
    fn min(&self, ord: u64, key: String, value: &BigDecimal);
}

#[derive(StoreWriter)]
pub struct ExternStoreMinBigFloat {}
impl StoreMinBigFloat for ExternStoreMinBigFloat{
    /// Will set the provided key in the store only if the value received in
    /// parameter is smaller than the one already present in the store, with
    /// a default of the zero value when the key is absent.
    fn min(&self, ord: u64, key: String, value: &BigDecimal) {
        state::set_min_bigfloat(ord as i64, key, value);
    }
}

/// StoreAppend is a struct representing a `store` with
/// `updatePolicy` equal to `append`
pub trait StoreAppend {
    fn append(&self, ord: u64, key: String, value: &String);
    fn append_bytes(&self, ord: u64, key: String, value: &Vec<u8>);
}

#[derive(StoreWriter)]
pub struct ExternStoreAppend {}
impl StoreAppend for ExternStoreAppend{
    /// Concatenates a given value at the end of the key's current value
    fn append(&self, ord: u64, key: String, value: &String) {
        state::append(ord as i64, key, &value.as_bytes().to_vec());
    }

    /// Concatenates a given value at the end of the key's current value
    fn append_bytes(&self, ord: u64, key: String, value: &Vec<u8>) {
        state::append(ord as i64, key, value);
    }
}

/// StoreGet is a struct representing a read only store `store`
pub trait StoreGet {
    fn get_at(&self, ord: u64, key: &String) -> Option<Vec<u8>>;
    fn get_last(&self, key: &String) -> Option<Vec<u8>>;
    fn get_first(&self, key: &String) -> Option<Vec<u8>>;
}

pub struct ExternStoreGet {
    idx: u32,
}

impl ExternStoreGet {
    /// Return a StoreGet object with a store index set
    pub fn new(idx: u32) -> ExternStoreGet {
        ExternStoreGet { idx }
    }
}

impl StoreGet for ExternStoreGet {
    /// Allows you to read a single key from the store. The type
    /// of its value can be anything, and is usually declared in
    /// the output section of the manifest. The ordinal is used here
    /// to go query a key that might have changed mid-block by
    /// the store module that built it.
    fn get_at(&self, ord: u64, key: &String) -> Option<Vec<u8>> {
        return state::get_at(self.idx, ord as i64, key);
    }

    /// Retrieves a key from the store, like `get_at`, but querying the state of
    /// the store as of the beginning of the block being processed, before any changes
    /// were applied within the current block. Tt does not need to rewind any changes
    /// in the middle of the block.
    fn get_last(&self, key: &String) -> Option<Vec<u8>> {
        return state::get_last(self.idx, key);
    }

    /// Retrieves a key from the store, like `get_at`, but querying the state of
    /// the store as of the beginning of the block being processed, before any changes
    /// were applied within the current block. However, it needs to unwind any keys that
    /// would have changed mid-block, so will be slightly less performant.
    fn get_first(&self, key: &String) -> Option<Vec<u8>> {
        return state::get_first(self.idx, key);
    }
}
