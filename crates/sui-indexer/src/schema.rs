// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
// @generated automatically by Diesel CLI.

diesel::table! {
    events (tx_sequence_number, event_sequence_number) {
        tx_sequence_number -> Bigint,
        event_sequence_number -> Bigint,
        transaction_digest -> Blob,
        checkpoint_sequence_number -> Bigint,
        senders -> Json,
        package -> Blob,
        module -> Text,
        event_type -> Text,
        timestamp_ms -> Bigint,
        bcs -> Blob,
    }
}
