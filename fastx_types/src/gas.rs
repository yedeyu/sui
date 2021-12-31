// Copyright (c) Facebook, Inc. and its affiliates.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    error::{FastPayError, FastPayResult},
    gas_coin::GasCoin,
    messages::{Order, OrderKind},
    object::Object,
};
use std::convert::TryFrom;

macro_rules! ok_or_gas_error {
    ($cond:expr, $e:expr) => {
        if !($cond) {
            Err(FastPayError::InsufficientGas { error: $e })
        } else {
            Ok(())
        }
    };
}

const MIN_MOVE_CALL_GAS: u64 = 10;
const MIN_MOVE_PUBLISH_GAS: u64 = 10;

pub fn check_gas_requirement(order: &Order, gas_object: &Object) -> FastPayResult {
    match &order.kind {
        OrderKind::Transfer(_) => {
            // TODO: Add gas logic for transfer orders.
            Ok(())
        }
        OrderKind::Publish(publish) => {
            assert_eq!(publish.gas_payment.0, gas_object.id());
            let balance = get_gas_balance(gas_object)?;
            ok_or_gas_error!(
                balance >= MIN_MOVE_PUBLISH_GAS,
                format!(
                    "Gas balance is {}, smaller than minimum requirement of {} for module publish.",
                    balance, MIN_MOVE_PUBLISH_GAS
                )
            )
        }
        OrderKind::Call(call) => {
            assert_eq!(call.gas_payment.0, gas_object.id());
            ok_or_gas_error!(
                call.gas_budget >= MIN_MOVE_CALL_GAS,
                format!(
                    "Gas budget is {}, smaller than minimum requirement of {} for move call.",
                    call.gas_budget, MIN_MOVE_CALL_GAS
                )
            )?;
            let balance = get_gas_balance(gas_object)?;
            ok_or_gas_error!(
                balance >= call.gas_budget,
                format!(
                    "Gas balance is {}, smaller than the budget {} for move call.",
                    balance, MIN_MOVE_CALL_GAS
                )
            )
        }
    }
}

pub fn deduct_gas(gas_object: &mut Object, amount: u64) -> FastPayResult {
    let gas_coin = GasCoin::try_from(&*gas_object)?;
    let balance = gas_coin.value();
    ok_or_gas_error!(
        balance >= amount,
        format!("Gas balance is {}, not enough to pay {}", balance, amount)
    )?;
    let new_gas_coin = GasCoin::new(*gas_coin.id(), balance - amount);
    gas_object.data.as_move_mut().unwrap().contents = bcs::to_bytes(&new_gas_coin).unwrap();
    let sequence_number = gas_object.next_sequence_number.increment()?;
    gas_object.next_sequence_number = sequence_number;
    Ok(())
}

pub fn get_gas_balance(gas_object: &Object) -> FastPayResult<u64> {
    Ok(GasCoin::try_from(gas_object)?.value())
}

pub fn calculate_module_publish_gas(module_bytes: &[Vec<u8>]) -> u64 {
    // TODO: Figure out module publish gas formula.
    // Currently just use the size in bytes of the modules plus a default minimum.
    module_bytes.iter().map(|v| v.len() as u64).sum::<u64>() + MIN_MOVE_PUBLISH_GAS
}
