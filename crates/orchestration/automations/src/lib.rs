//! Durable control plane for versioned Wasm automations.
//!
//! The core boundary is intentionally explicit:
//! - guest automations are immutable Wasm artifacts
//! - a revision must be registered before it can be activated
//! - the control plane owns persistence, wakeups, and effect mediation
//! - guests communicate only through the protocol types exported by
//!   `mango-automation-protocol`

mod clock;
mod control_plane;
mod domain;
mod error;
mod guest;
mod pocket_universe;
mod store;
mod supervisor;

pub use clock::*;
pub use control_plane::*;
pub use domain::*;
pub use error::*;
pub use guest::*;
pub use pocket_universe::*;
pub use store::*;
pub use supervisor::*;
