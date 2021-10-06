#[macro_use]
pub mod macros {
    macro_rules! ok (($result:expr) => ($result.unwrap()));
}
