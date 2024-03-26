// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

import type { SerializedBcs } from '@mysten/bcs';
import { fromB64, isSerializedBcs } from '@mysten/bcs';
import type { Input } from 'valibot';
import { is, parse } from 'valibot';

import type { SuiClient } from '../client/index.js';
import type { SignatureWithBytes, Signer } from '../cryptography/index.js';
import { normalizeSuiAddress } from '../utils/sui-types.js';
import { v1BlockDataFromTransactionBlockState } from './blockData/v1.js';
import type { CallArg, Transaction, TypeTag } from './blockData/v2.js';
import { Argument, NormalizedCallArg, ObjectRef, TransactionExpiration } from './blockData/v2.js';
import {
	normalizeInputs,
	resolveObjectReferences,
	setGasBudget,
	setGasPayment,
	setGasPrice,
	validate,
} from './buildSteps.js';
import { getIdFromCallArg, Inputs } from './Inputs.js';
import { createPure } from './pure.js';
import { TransactionBlockDataBuilder } from './TransactionBlockData.js';
import type {
	BuildTransactionBlockOptions,
	SerializeTransactionBlockOptions,
	TransactionBlockDataResolver,
	TransactionBlockStep,
} from './TransactionBlockDataResolver.js';
import { SuiClientTransactionBlockDataResolver } from './TransactionBlockDataResolver.js';
import type { TransactionArgument } from './Transactions.js';
import { Transactions } from './Transactions.js';

export type TransactionObjectArgument =
	| Exclude<Argument, { Input: unknown; type?: 'pure' }>
	| ((txb: TransactionBlock) => Exclude<Argument, { Input: unknown; type?: 'pure' }>);

export type TransactionResult = Extract<Argument, { Result: unknown }> &
	Extract<Argument, { NestedResult: unknown }>[];

function createTransactionResult(index: number): TransactionResult {
	const baseResult: TransactionArgument = { $kind: 'Result', Result: index };

	const nestedResults: TransactionArgument[] = [];
	const nestedResultFor = (resultIndex: number): TransactionArgument =>
		(nestedResults[resultIndex] ??= {
			$kind: 'NestedResult',
			NestedResult: [index, resultIndex],
		});

	return new Proxy(baseResult, {
		set() {
			throw new Error(
				'The transaction result is a proxy, and does not support setting properties directly',
			);
		},
		// TODO: Instead of making this return a concrete argument, we should ideally
		// make it reference-based (so that this gets resolved at build-time), which
		// allows re-ordering transactions.
		get(target, property) {
			// This allows this transaction argument to be used in the singular form:
			if (property in target) {
				return Reflect.get(target, property);
			}

			// Support destructuring:
			if (property === Symbol.iterator) {
				return function* () {
					let i = 0;
					while (true) {
						yield nestedResultFor(i);
						i++;
					}
				};
			}

			if (typeof property === 'symbol') return;

			const resultIndex = parseInt(property, 10);
			if (Number.isNaN(resultIndex) || resultIndex < 0) return;
			return nestedResultFor(resultIndex);
		},
	}) as TransactionResult;
}

const TRANSACTION_BRAND = Symbol.for('@mysten/transaction');

interface SignOptions extends BuildTransactionBlockOptions {
	signer: Signer;
}

export function isTransactionBlock(obj: unknown): obj is TransactionBlock {
	return !!obj && typeof obj === 'object' && (obj as any)[TRANSACTION_BRAND] === true;
}

export type TransactionObjectInput = string | CallArg | TransactionObjectArgument;

/**
 * Transaction Builder
 */
export class TransactionBlock {
	#serializationSteps: TransactionBlockStep[] = [];
	#buildSteps: TransactionBlockStep[] = [];
	#intentResolvers = new Map<string, TransactionBlockStep>();

	/**
	 * Converts from a serialize transaction kind (built with `build({ onlyTransactionKind: true })`) to a `Transaction` class.
	 * Supports either a byte array, or base64-encoded bytes.
	 */
	static fromKind(serialized: string | Uint8Array) {
		const tx = new TransactionBlock();

		tx.#blockData = TransactionBlockDataBuilder.fromKindBytes(
			typeof serialized === 'string' ? fromB64(serialized) : serialized,
		);

		return tx;
	}

	/**
	 * Converts from a serialized transaction format to a `Transaction` class.
	 * There are two supported serialized formats:
	 * - A string returned from `Transaction#serialize`. The serialized format must be compatible, or it will throw an error.
	 * - A byte array (or base64-encoded bytes) containing BCS transaction data.
	 */
	static from(serialized: string | Uint8Array) {
		const tx = new TransactionBlock();

		// Check for bytes:
		if (typeof serialized !== 'string' || !serialized.startsWith('{')) {
			tx.#blockData = TransactionBlockDataBuilder.fromBytes(
				typeof serialized === 'string' ? fromB64(serialized) : serialized,
			);
		} else {
			tx.#blockData = TransactionBlockDataBuilder.restore(JSON.parse(serialized));
		}

		return tx;
	}

	addSerializationStep(step: TransactionBlockStep) {
		this.#serializationSteps.push(step);
	}

	addBuildStep(step: TransactionBlockStep) {
		this.#buildSteps.push(step);
	}

	addIntentResolver(intent: string, resolver: TransactionBlockStep) {
		if (this.#intentResolvers.has(intent) && this.#intentResolvers.get(intent) !== resolver) {
			throw new Error(`Intent resolver for ${intent} already exists`);
		}

		this.#intentResolvers.set(intent, resolver);
	}

	setSender(sender: string) {
		this.#blockData.sender = sender;
	}
	/**
	 * Sets the sender only if it has not already been set.
	 * This is useful for sponsored transaction flows where the sender may not be the same as the signer address.
	 */
	setSenderIfNotSet(sender: string) {
		if (!this.#blockData.sender) {
			this.#blockData.sender = sender;
		}
	}
	setExpiration(expiration?: Input<typeof TransactionExpiration> | null) {
		this.#blockData.expiration = expiration ? parse(TransactionExpiration, expiration) : null;
	}
	setGasPrice(price: number | bigint) {
		this.#blockData.gasConfig.price = String(price);
	}
	setGasBudget(budget: number | bigint) {
		this.#blockData.gasConfig.budget = String(budget);
	}
	setGasOwner(owner: string) {
		this.#blockData.gasConfig.owner = owner;
	}
	setGasPayment(payments: ObjectRef[]) {
		this.#blockData.gasConfig.payment = payments.map((payment) => parse(ObjectRef, payment));
	}

	#blockData: TransactionBlockDataBuilder;
	/** @deprecated Use `getBlockData()` instead. */

	get blockData() {
		return v1BlockDataFromTransactionBlockState(this.#blockData.snapshot());
	}

	/** Get a snapshot of the transaction data, in JSON form: */
	getBlockData() {
		return this.#blockData.snapshot();
	}

	// Used to brand transaction classes so that they can be identified, even between multiple copies
	// of the builder.
	get [TRANSACTION_BRAND]() {
		return true;
	}

	// Temporary workaround for the wallet interface accidentally serializing transaction blocks via postMessage
	get pure(): ReturnType<typeof createPure> {
		Object.defineProperty(this, 'pure', {
			enumerable: false,
			value: createPure((value): Argument => {
				if (isSerializedBcs(value)) {
					return this.#blockData.addInput('pure', {
						$kind: 'Pure',
						Pure: Array.from(value.toBytes()),
					});
				}

				// TODO: we can also do some deduplication here
				return this.#blockData.addInput(
					'pure',
					is(NormalizedCallArg, value)
						? parse(NormalizedCallArg, value)
						: value instanceof Uint8Array
						? Inputs.Pure(value)
						: { $kind: 'RawValue', RawValue: { value } },
				);
			}),
		});

		return this.pure;
	}

	constructor(transaction?: TransactionBlock) {
		this.#blockData = new TransactionBlockDataBuilder(
			transaction ? transaction.getBlockData() : undefined,
		);
	}

	/** Returns an argument for the gas coin, to be used in a transaction. */
	get gas() {
		return { $kind: 'GasCoin' as const, GasCoin: true as const };
	}

	/**
	 * Add a new object input to the transaction.
	 */
	object(value: TransactionObjectInput): { $kind: 'Input'; Input: number; type?: 'object' } {
		if (typeof value === 'function') {
			return this.object(value(this));
		}

		if (typeof value === 'object' && is(Argument, value)) {
			return value as { $kind: 'Input'; Input: number; type?: 'object' };
		}

		const id = getIdFromCallArg(value);

		const inserted = this.#blockData.inputs.find((i) => id === getIdFromCallArg(i));

		// Upgrade shared object inputs to mutable if needed:
		if (inserted?.Object?.SharedObject && typeof value === 'object' && value.Object?.SharedObject) {
			inserted.Object.SharedObject.mutable =
				inserted.Object.SharedObject.mutable || value.Object.SharedObject.mutable;
		}

		return inserted
			? { $kind: 'Input', Input: this.#blockData.inputs.indexOf(inserted), type: 'object' }
			: this.#blockData.addInput(
					'object',
					typeof value === 'string'
						? {
								$kind: 'UnresolvedObject',
								UnresolvedObject: { value: normalizeSuiAddress(value), typeSignatures: [] },
						  }
						: value,
			  );
	}

	/**
	 * Add a new object input to the transaction using the fully-resolved object reference.
	 * If you only have an object ID, use `builder.object(id)` instead.
	 */
	objectRef(...args: Parameters<(typeof Inputs)['ObjectRef']>) {
		return this.object(Inputs.ObjectRef(...args));
	}

	/**
	 * Add a new receiving input to the transaction using the fully-resolved object reference.
	 * If you only have an object ID, use `builder.object(id)` instead.
	 */
	receivingRef(...args: Parameters<(typeof Inputs)['ReceivingRef']>) {
		return this.object(Inputs.ReceivingRef(...args));
	}

	/**
	 * Add a new shared object input to the transaction using the fully-resolved shared object reference.
	 * If you only have an object ID, use `builder.object(id)` instead.
	 */
	sharedObjectRef(...args: Parameters<(typeof Inputs)['SharedObjectRef']>) {
		return this.object(Inputs.SharedObjectRef(...args));
	}

	/** Add a transaction to the transaction block. */
	add(transaction: Transaction) {
		const index = this.#blockData.transactions.push(transaction);
		return createTransactionResult(index - 1);
	}

	#normalizeTransactionArgument(arg: TransactionArgument | SerializedBcs<any>) {
		if (isSerializedBcs(arg)) {
			return this.pure(arg);
		}

		return this.resolveArgument(arg as TransactionArgument);
	}

	resolveArgument(arg: TransactionArgument): Argument {
		if (typeof arg === 'function') {
			return arg(this);
		}

		return arg;
	}

	// Method shorthands:

	splitCoins(
		coin: TransactionObjectArgument | string,
		amounts: (TransactionArgument | SerializedBcs<any> | number | string | bigint)[],
	) {
		return this.add(
			Transactions.SplitCoins(
				typeof coin === 'string' ? this.object(coin) : this.resolveArgument(coin),
				amounts.map((amount) =>
					typeof amount === 'number' || typeof amount === 'bigint' || typeof amount === 'string'
						? this.pure.u64(amount)
						: this.#normalizeTransactionArgument(amount),
				),
			),
		);
	}
	mergeCoins(
		destination: TransactionObjectArgument | string,
		sources: (TransactionObjectArgument | string)[],
	) {
		return this.add(
			Transactions.MergeCoins(
				this.object(destination),
				sources.map((src) => this.object(src)),
			),
		);
	}
	publish({ modules, dependencies }: { modules: number[][] | string[]; dependencies: string[] }) {
		return this.add(
			Transactions.Publish({
				modules,
				dependencies,
			}),
		);
	}
	upgrade({
		modules,
		dependencies,
		packageId,
		ticket,
	}: {
		modules: number[][] | string[];
		dependencies: string[];
		packageId: string;
		ticket: TransactionObjectArgument | string;
	}) {
		return this.add(
			Transactions.Upgrade({
				modules,
				dependencies,
				packageId,
				ticket: this.object(ticket),
			}),
		);
	}
	moveCall({
		arguments: args,
		...input
	}:
		| {
				package: string;
				module: string;
				function: string;
				arguments?: (TransactionArgument | SerializedBcs<any>)[];
				typeArguments?: (string | TypeTag)[];
		  }
		| {
				target: string;
				arguments?: (TransactionArgument | SerializedBcs<any>)[];
				typeArguments?: (string | TypeTag)[];
		  }) {
		return this.add(
			Transactions.MoveCall({
				...input,
				arguments: args?.map((arg) => this.#normalizeTransactionArgument(arg)),
			} as Parameters<typeof Transactions.MoveCall>[0]),
		);
	}
	transferObjects(
		objects: (TransactionObjectArgument | string)[],
		address: TransactionArgument | SerializedBcs<any> | string,
	) {
		return this.add(
			Transactions.TransferObjects(
				objects.map((obj) => this.object(obj)),
				typeof address === 'string'
					? this.pure.address(address)
					: this.#normalizeTransactionArgument(address),
			),
		);
	}
	makeMoveVec({
		type,
		objects,
	}: {
		objects: (TransactionObjectArgument | string)[];
		type?: string;
	}) {
		return this.add(
			Transactions.MakeMoveVec({
				type,
				objects: objects.map((obj) => this.object(obj)),
			}),
		);
	}

	/**
	 * Serialize the transaction to a string so that it can be sent to a separate context.
	 * This is different from `build` in that it does not serialize to BCS bytes, and instead
	 * uses a separate format that is unique to the transaction builder. This allows
	 * us to serialize partially-complete transactions, that can then be completed and
	 * built in a separate context.
	 *
	 * For example, a dapp can construct a transaction, but not provide gas objects
	 * or a gas budget. The transaction then can be sent to the wallet, where this
	 * information is automatically filled in (e.g. by querying for coin objects
	 * and performing a dry run).
	 */
	serialize() {
		return JSON.stringify(v1BlockDataFromTransactionBlockState(this.#blockData.snapshot()));
	}

	async toJSON(options: SerializeTransactionBlockOptions = {}): Promise<string> {
		await this.#prepareForSerialization(options, await this.getDataResolver(options));
		return JSON.stringify(this.#blockData.snapshot());
	}

	/** Build the transaction to BCS bytes, and sign it with the provided keypair. */
	async sign(options: SignOptions): Promise<SignatureWithBytes> {
		const { signer, ...buildOptions } = options;
		const bytes = await this.build(buildOptions);
		return signer.signTransactionBlock(bytes);
	}

	/** Build the transaction to BCS bytes. */
	async build(options: BuildTransactionBlockOptions = {}): Promise<Uint8Array> {
		const dataResolver = await this.getDataResolver(options);

		await this.#prepareBuild(options, dataResolver);
		return this.#blockData.build({
			maxSizeBytes: dataResolver.getLimit('maxTxSizeBytes'),
			onlyTransactionKind: options.onlyTransactionKind,
		});
	}

	/** Derive transaction digest */
	async getDigest(
		options: {
			client?: SuiClient;
		} = {},
	): Promise<string> {
		await this.#prepareBuild(options, await this.getDataResolver(options));
		return this.#blockData.getDigest();
	}

	/**
	 * Prepare the transaction by validating the transaction data and resolving all inputs
	 * so that it can be built into bytes.
	 */
	async #prepareBuild(
		options: BuildTransactionBlockOptions,
		dataResolver: TransactionBlockDataResolver,
	) {
		if (!options.onlyTransactionKind && !this.#blockData.sender) {
			throw new Error('Missing transaction sender');
		}

		const steps = [
			...this.#buildSteps,
			normalizeInputs,
			resolveObjectReferences,
			...(options.onlyTransactionKind ? [] : [setGasPrice, setGasBudget, setGasPayment]),
			validate,
		];

		for (const step of steps) {
			await step(this.#blockData, dataResolver);
		}
	}

	async #prepareForSerialization(
		options: SerializeTransactionBlockOptions,
		dataResolver: TransactionBlockDataResolver,
	) {
		const intents = new Set<string>();
		for (const transaction of this.#blockData.transactions) {
			if (transaction.TransactionIntent && options.supportedIntents) {
				intents.add(transaction.TransactionIntent.name);
			}
		}

		for (const intent of intents) {
			if (options.supportedIntents?.includes(intent)) {
				continue;
			}

			if (!this.#intentResolvers.has(intent)) {
				throw new Error(`Missing intent resolver for ${intent}`);
			}

			await this.#intentResolvers.get(intent)!(this.#blockData, dataResolver);
		}
	}

	async getDataResolver(options: SerializeTransactionBlockOptions = {}) {
		let protocolConfig = options.protocolConfig;
		if (!options.protocolConfig && !options.limits && options.client) {
			protocolConfig = await options.client.getProtocolConfig();
		}

		return new SuiClientTransactionBlockDataResolver({
			...options,
			protocolConfig,
		});
	}
}
