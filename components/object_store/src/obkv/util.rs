// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

use table_kv::{KeyBoundary, ScanRequest};

/// Generate ScanRequest with prefix
pub fn scan_request_with_prefix(prefix_bytes: &[u8]) -> ScanRequest {
    let mut start_key = Vec::with_capacity(prefix_bytes.len());
    start_key.extend(prefix_bytes);
    let start = KeyBoundary::included(start_key.as_ref());

    let mut end_key = Vec::with_capacity(prefix_bytes.len());
    end_key.extend(prefix_bytes);
    let carry = inc_by_one(&mut end_key);
    // Check add one operation overflow.
    let end = if carry == 1 {
        KeyBoundary::MaxIncluded
    } else {
        KeyBoundary::excluded(end_key.as_ref())
    };
    table_kv::ScanRequest {
        start,
        end,
        reverse: false,
    }
}

/// Increment one to the byte array, and return the carry.
fn inc_by_one(nums: &mut [u8]) -> u8 {
    let mut carry = 1;
    for i in (0..nums.len()).rev() {
        let sum = nums[i].wrapping_add(carry);
        nums[i] = sum;
        if sum == 0 {
            carry = 1;
        } else {
            carry = 0;
            break;
        }
    }
    carry
}
