//! Public guest-facing automation SDK.
//!
//! This crate intentionally re-exports the underlying protocol and Wasm guest
//! helper crates so application-level automation projects have one stable
//! dependency surface inside the Mango monorepo.

pub use mango_automation_guest_sdk::*;
