// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

import { toB58 } from '@mysten/bcs';
import type { Input } from 'valibot';
import { parse } from 'valibot';

import { bcs } from '../bcs/index.js';
import { normalizeSuiAddress } from '../utils/sui-types.js';
import { transactionBlockStateFromV1BlockData } from './blockData/v1.js';
import type { SerializedTransactionDataBuilderV1 } from './blockData/v1.js';
import type {
	Argument,
	CallArg,
	GasData,
	Transaction,
	TransactionExpiration,
} from './blockData/v2.js';
import { TransactionBlockState } from './blockData/v2.js';
import { hashTypedData } from './hash.js';

function prepareSuiAddress(address: string) {
	return normalizeSuiAddress(address).replace('0x', '');
}

export class TransactionBlockDataBuilder implements TransactionBlockState {
	static fromKindBytes(bytes: Uint8Array) {
		const kind = bcs.TransactionKind.parse(bytes);

		const programmableTx = kind.ProgrammableTransaction;
		if (!programmableTx) {
			throw new Error('Unable to deserialize from bytes.');
		}

		return TransactionBlockDataBuilder.restore({
			version: 2,
			features: [],
			sender: null,
			expiration: null,
			gasData: {
				budget: null,
				owner: null,
				payment: null,
				price: null,
			},
			inputs: programmableTx.inputs,
			transactions: programmableTx.transactions,
		});
	}

	static fromBytes(bytes: Uint8Array) {
		const rawData = bcs.TransactionData.parse(bytes);
		const data = rawData?.V1;
		const programmableTx = data.kind.ProgrammableTransaction;

		if (!data || !programmableTx) {
			throw new Error('Unable to deserialize from bytes.');
		}

		return TransactionBlockDataBuilder.restore({
			version: 2,
			features: [],
			sender: data.sender,
			expiration: data.expiration,
			gasData: data.gasData,
			inputs: programmableTx.inputs,
			transactions: programmableTx.transactions,
		});
	}

	static restore(
		data: Input<typeof TransactionBlockState> | Input<typeof SerializedTransactionDataBuilderV1>,
	) {
		if (data.version === 2) {
			return new TransactionBlockDataBuilder(parse(TransactionBlockState, data));
		} else {
			return new TransactionBlockDataBuilder(
				parse(TransactionBlockState, transactionBlockStateFromV1BlockData(data)),
			);
		}
	}

	/**
	 * Generate transaction digest.
	 *
	 * @param bytes BCS serialized transaction data
	 * @returns transaction digest.
	 */
	static getDigestFromBytes(bytes: Uint8Array) {
		const hash = hashTypedData('TransactionData', bytes);
		return toB58(hash);
	}

	// @deprecated use gasData instead
	get gasConfig() {
		return this.gasData;
	}
	// @deprecated use gasData instead
	set gasConfig(value) {
		this.gasData = value;
	}

	features: string[];
	version = 2 as const;
	sender: string | null;
	expiration: TransactionExpiration | null;
	gasData: GasData;
	inputs: CallArg[];
	transactions: Transaction[];

	constructor(clone?: TransactionBlockState) {
		this.features = clone?.features ?? [];
		this.sender = clone?.sender ?? null;
		this.expiration = clone?.expiration ?? null;
		this.inputs = clone?.inputs ?? [];
		this.transactions = clone?.transactions ?? [];
		this.gasData = clone?.gasData ?? {
			budget: null,
			price: null,
			owner: null,
			payment: null,
		};
	}

	build({
		maxSizeBytes = Infinity,
		overrides,
		onlyTransactionKind,
	}: {
		maxSizeBytes?: number;
		overrides?: {
			expiration?: TransactionExpiration;
			sender?: string;
			// @deprecated use gasData instead
			gasConfig?: Partial<GasData>;
			gasData?: Partial<GasData>;
		};
		onlyTransactionKind?: boolean;
	} = {}) {
		// TODO validate that inputs and intents are actually resolved
		const inputs = this.inputs as Extract<CallArg, { Object: unknown } | { Pure: unknown }>[];
		const transactions = this.transactions as Extract<
			Transaction<Exclude<Argument, { IntentResult: unknown } | { NestedIntentResult: unknown }>>,
			{ Upgrade: unknown }
		>[];

		const kind = {
			ProgrammableTransaction: {
				inputs,
				transactions,
			},
		};

		if (onlyTransactionKind) {
			return bcs.TransactionKind.serialize(kind, { maxSize: maxSizeBytes }).toBytes();
		}

		const expiration = overrides?.expiration ?? this.expiration;
		const sender = overrides?.sender ?? this.sender;
		const gasData = { ...this.gasData, ...overrides?.gasConfig, ...overrides?.gasData };

		if (!sender) {
			throw new Error('Missing transaction sender');
		}

		if (!gasData.budget) {
			throw new Error('Missing gas budget');
		}

		if (!gasData.payment) {
			throw new Error('Missing gas payment');
		}

		if (!gasData.price) {
			throw new Error('Missing gas price');
		}

		const transactionData = {
			sender: prepareSuiAddress(sender),
			expiration: expiration ? expiration : { None: true },
			gasData: {
				payment: gasData.payment,
				owner: prepareSuiAddress(this.gasData.owner ?? sender),
				price: BigInt(gasData.price),
				budget: BigInt(gasData.budget),
			},
			kind: {
				ProgrammableTransaction: {
					inputs,
					transactions,
				},
			},
		};

		return bcs.TransactionData.serialize(
			{ V1: transactionData },
			{ maxSize: maxSizeBytes },
		).toBytes();
	}

	addInput<T extends 'object' | 'pure'>(type: T, arg: CallArg) {
		const index = this.inputs.length;
		this.inputs.push(arg);
		return { Input: index, type, $kind: 'Input' as const };
	}

	mapArguments(fn: (arg: Argument) => Argument) {
		for (const tx of this.transactions) {
			switch (tx.$kind) {
				case 'MoveCall':
					tx.MoveCall.arguments = tx.MoveCall.arguments.map((arg) => fn(arg));
					break;
				case 'TransferObjects':
					tx.TransferObjects[0] = tx.TransferObjects[0].map((arg) => fn(arg));
					tx.TransferObjects[1] = fn(tx.TransferObjects[1]);
					break;
				case 'SplitCoins':
					tx.SplitCoins[0] = fn(tx.SplitCoins[0]);
					tx.SplitCoins[1] = tx.SplitCoins[1].map((arg) => fn(arg));
					break;
				case 'MergeCoins':
					tx.MergeCoins[0] = fn(tx.MergeCoins[0]);
					tx.MergeCoins[1] = tx.MergeCoins[1].map((arg) => fn(arg));
					break;
				case 'MakeMoveVec':
					tx.MakeMoveVec = [tx.MakeMoveVec[0], tx.MakeMoveVec[1].map((arg) => fn(arg))];
					break;
				case 'Upgrade':
					tx.Upgrade[3] = fn(tx.Upgrade[3]);
					break;
				case 'TransactionIntent':
					const inputs = tx.TransactionIntent.inputs;
					tx.TransactionIntent.inputs = {};

					for (const [key, value] of Object.entries(inputs)) {
						tx.TransactionIntent.inputs[key] = Array.isArray(value)
							? value.map((arg) => fn(arg))
							: fn(value);
					}

					break;
				case 'Publish':
					break;
				default:
					throw new Error(`Unexpected transaction kind: ${(tx as { $kind: unknown }).$kind}`);
			}
		}
	}

	replaceTransaction(index: number, replacement: Transaction | Transaction[]) {
		if (!Array.isArray(replacement)) {
			this.transactions[index] = replacement;
			return;
		}

		const sizeDiff = replacement.length - 1;
		this.transactions.splice(index, 1, ...replacement);

		if (sizeDiff !== 0) {
			this.mapArguments((arg) => {
				switch (arg.$kind) {
					case 'IntentResult':
						if (arg.IntentResult > index) {
							arg.IntentResult += sizeDiff;
						}
						break;
					case 'Result':
						if (arg.Result > index) {
							arg.Result += sizeDiff;
						}
						break;
					case 'NestedIntentResult':
						if (arg.NestedIntentResult[0] > index) {
							arg.NestedIntentResult[0] += sizeDiff;
						}
						break;
					case 'NestedResult':
						if (arg.NestedResult[0] > index) {
							arg.NestedResult[0] += sizeDiff;
						}
						break;
				}
				return arg;
			});
		}
	}

	getDigest() {
		const bytes = this.build({ onlyTransactionKind: false });
		return TransactionBlockDataBuilder.getDigestFromBytes(bytes);
	}

	snapshot(): TransactionBlockState {
		return parse(TransactionBlockState, this);
	}
}
