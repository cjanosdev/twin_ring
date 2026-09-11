//! TwinRing experiment infrastructure.
pub mod experiment;
pub mod measurements;

// Keep the old module path available while downstream code adopts the clearer name.
pub use measurements as metrics;
