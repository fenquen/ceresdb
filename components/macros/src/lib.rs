// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

//! Contains all needed macros

/// Define result for given Error type
#[macro_export]
macro_rules! define_result {
    ($t:ty) => {
        pub type Result<T> = std::result::Result<T, $t>;
    };
}

#[macro_export]
macro_rules! hash_map(
    { $($key:expr => $value:expr),+ } => {
        {
            let mut m = ::std::collections::HashMap::new();
            $(
                m.insert($key, $value);
            )+
            m
        }
     };
);