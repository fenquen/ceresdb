// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

use std::thread;

use log::error;

/// fork from https://github.com/tikv/tikv/blob/83d173a2c0058246631f0e71de74238ccff670fd/components/tikv_util/src/lib.rs#L429
/// Exit the whole process when panic.
pub fn set_panic_hook(panic_abort: bool) {
    use std::{panic, process};

    // HACK! New a backtrace ahead for caching necessary elf sections of this
    // tikv-server, in case it can not open more files during panicking
    // which leads to no stack info (0x5648bdfe4ff2 - <no info>).
    //
    // Crate backtrace caches debug info in a static variable `STATE`,
    // and the `STATE` lives forever once it has been created.
    // See more: https://github.com/alexcrichton/backtrace-rs/blob/\
    //           597ad44b131132f17ed76bf94ac489274dd16c7f/\
    //           src/symbolize/libbacktrace.rs#L126-L159
    // Caching is slow, spawn it in another thread to speed up.
    thread::Builder::new()
        .name("backtrace-loader".to_owned())
        .spawn(backtrace::Backtrace::new)
        .unwrap();

    panic::set_hook(Box::new(move |info: &panic::PanicInfo<'_>| {
        let msg = match info.payload().downcast_ref::<&'static str>() {
            Some(s) => *s,
            None => match info.payload().downcast_ref::<String>() {
                Some(s) => &s[..],
                None => "Box<Any>",
            },
        };

        let thread = thread::current();
        let name = thread.name().unwrap_or("<unnamed>");
        let loc = info
            .location()
            .map(|l| format!("{}:{}", l.file(), l.line()));
        let bt = backtrace::Backtrace::new();
        error!(
            "thread '{}' panicked '{}' at {:?}\n{:?}",
            name,
            msg,
            loc.unwrap_or_else(|| "<unknown>".to_owned()),
            bt
        );

        // There might be remaining logs in the async logger.
        // To collect remaining logs and also collect future logs, replace the old one
        // with a terminal logger.
        // When the old global async logger is replaced, the old async guard will be
        // taken and dropped. In the drop() the async guard, it waits for the
        // finish of the remaining logs in the async logger.
        if let Some(level) = ::log::max_level().to_level() {
            let drainer = logger::term_drainer();
            let _ = logger::init_log_from_drain(
                drainer,
                logger::convert_log_level_to_slog_level(level),
                false, // Use sync logger to avoid an unnecessary log thread.
                0,
                false, // It is initialized already.
            );
        }

        if panic_abort {
            process::abort();
        } else {
            unsafe {
                // Calling process::exit would trigger global static to destroy, like C++
                // static variables of RocksDB, which may cause other threads encounter
                // pure virtual method call. So calling libc::_exit() instead to skip the
                // cleanup process.
                libc::_exit(1);
            }
        }
    }))
}