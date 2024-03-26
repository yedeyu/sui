// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

import type { BcsType } from '@mysten/bcs';
import { parse } from 'valibot';

import { bcs } from '../bcs/index.js';
import type { SuiClient } from '../client/client.js';
import { SUI_TYPE_ARG } from '../utils/index.js';
import { normalizeSuiAddress, normalizeSuiObjectId } from '../utils/sui-types.js';
import type { Argument, CallArg, OpenMoveTypeSignature, Transaction } from './blockData/v2.js';
import { ObjectRef } from './blockData/v2.js';
import { Inputs, isMutableSharedObjectInput } from './Inputs.js';
import { getPureBcsSchema, isTxContext, normalizedTypeToMoveTypeSignature } from './serializer.js';
import type { TransactionBlockDataBuilder } from './TransactionBlockData.js';

export type MaybePromise<T> = T | Promise<T>;
export type TransactionBlockPluginMethod<Options> = (
	blockData: TransactionBlockDataBuilder,
	options: Options,
	next: (options?: Options) => MaybePromise<void>,
) => MaybePromise<void>;

export interface TransactionBlockPlugin {
	normalizeInputs?: TransactionBlockPluginMethod<object>;
	resolveObjectReferences?: TransactionBlockPluginMethod<object>;
	setGasPrice?: TransactionBlockPluginMethod<object>;
	setGasBudget?: TransactionBlockPluginMethod<{
		maxTxGas: number;
		maxTxSizeBytes: number;
	}>;
	setGasPayment?: TransactionBlockPluginMethod<{
		maxGasObjects: number;
	}>;
	resolveIntent?: TransactionBlockPluginMethod<{
		name: string;
	}>;
	validate?: TransactionBlockPluginMethod<{
		maxPureArgumentSize: number;
	}>;
}
// The maximum objects that can be fetched at once using multiGetObjects.
const MAX_OBJECTS_PER_FETCH = 50;

// An amount of gas (in gas units) that is added to transactions as an overhead to ensure transactions do not fail.
const GAS_SAFE_OVERHEAD = 1000n;

const chunk = <T>(arr: T[], size: number): T[][] =>
	Array.from({ length: Math.ceil(arr.length / size) }, (_, i) =>
		arr.slice(i * size, i * size + size),
	);

type BasePluginMethod<T extends keyof TransactionBlockPlugin> = NonNullable<
	TransactionBlockPlugin[T]
> extends (blockData: TransactionBlockDataBuilder, options: infer O, next: any) => unknown
	? object extends O
		? (blockData: TransactionBlockDataBuilder, options?: O) => Promise<void>
		: (blockData: TransactionBlockDataBuilder, options: O) => Promise<void>
	: never;

export class DefaultTransactionBlockFeatures implements TransactionBlockPlugin {
	#plugins: TransactionBlockPlugin[];
	#getClient: () => SuiClient;

	constructor(plugins: TransactionBlockPlugin[], getClient: () => SuiClient) {
		this.#plugins = plugins;
		this.#getClient = getClient;
	}

	#runHook = async <T extends keyof TransactionBlockPlugin>(
		hook: T,
		blockData: TransactionBlockDataBuilder,
		options: Parameters<NonNullable<TransactionBlockPlugin[T]>>[1],
		last: (options?: Parameters<NonNullable<TransactionBlockPlugin[T]>>[1]) => MaybePromise<void>,
	) => {
		const plugins = this.#plugins.filter((plugin) => plugin[hook]);

		return runNext(0, options);

		function runNext(
			i: number,
			nextOptions: Parameters<NonNullable<TransactionBlockPlugin[T]>>[1],
		) {
			const plugin = plugins[i];

			if (!plugin) {
				return last(nextOptions);
			}

			(plugin[hook]! as TransactionBlockPluginMethod<typeof nextOptions>)(
				blockData,
				nextOptions,
				(cbOptions) => {
					return runNext(i + 1, cbOptions ?? nextOptions);
				},
			);
		}
	};

	setGasPrice: BasePluginMethod<'setGasPrice'> = async (blockData) => {
		await this.#runHook('setGasPrice', blockData, {}, async () => {
			if (blockData.gasConfig.price) {
				return;
			}

			blockData.gasConfig.price = String(await this.#getClient().getReferenceGasPrice());
		});
	};

	setGasBudget: BasePluginMethod<'setGasBudget'> = async (blockData, options) => {
		await this.#runHook('setGasBudget', blockData, options, async () => {
			if (!blockData.gasConfig.budget) {
				const dryRunResult = await this.#getClient().dryRunTransactionBlock({
					transactionBlock: blockData.build({
						maxSizeBytes: options.maxTxSizeBytes,
						overrides: {
							gasData: {
								budget: String(options.maxTxGas),
								payment: [],
							},
						},
					}),
				});

				if (dryRunResult.effects.status.status !== 'success') {
					throw new Error(
						`Dry run failed, could not automatically determine a budget: ${dryRunResult.effects.status.error}`,
						{ cause: dryRunResult },
					);
				}

				const safeOverhead = GAS_SAFE_OVERHEAD * BigInt(blockData.gasConfig.price || 1n);

				const baseComputationCostWithOverhead =
					BigInt(dryRunResult.effects.gasUsed.computationCost) + safeOverhead;

				const gasBudget =
					baseComputationCostWithOverhead +
					BigInt(dryRunResult.effects.gasUsed.storageCost) -
					BigInt(dryRunResult.effects.gasUsed.storageRebate);

				// Set the budget to max(computation, computation + storage - rebate)
				blockData.gasConfig.budget = String(
					gasBudget > baseComputationCostWithOverhead ? gasBudget : baseComputationCostWithOverhead,
				);
			}
		});
	};

	// The current default is just picking _all_ coins we can which may not be ideal.
	setGasPayment: BasePluginMethod<'setGasPayment'> = async (blockData, options) => {
		await this.#runHook('setGasPayment', blockData, options, async () => {
			if (blockData.gasConfig.payment) {
				if (blockData.gasConfig.payment.length > options.maxGasObjects) {
					throw new Error(`Payment objects exceed maximum amount: ${options.maxGasObjects}`);
				}
			}

			// Early return if the payment is already set:
			if (blockData.gasConfig.payment) {
				return;
			}

			const gasOwner = blockData.gasConfig.owner ?? blockData.sender;

			const coins = await this.#getClient().getCoins({
				owner: gasOwner!,
				coinType: SUI_TYPE_ARG,
			});

			const paymentCoins = coins.data
				// Filter out coins that are also used as input:
				.filter((coin) => {
					const matchingInput = blockData.inputs.find((input) => {
						if (input.Object?.ImmOrOwnedObject) {
							return coin.coinObjectId === input.Object.ImmOrOwnedObject.objectId;
						}

						return false;
					});

					return !matchingInput;
				})
				.slice(0, options.maxGasObjects - 1)
				.map((coin) => ({
					objectId: coin.coinObjectId,
					digest: coin.digest,
					version: coin.version,
				}));

			if (!paymentCoins.length) {
				throw new Error('No valid gas coins found for the transaction.');
			}

			blockData.gasConfig.payment = paymentCoins.map((payment) => parse(ObjectRef, payment));
		});
	};

	resolveObjectReferences: BasePluginMethod<'resolveObjectReferences'> = async (blockData) => {
		await this.#runHook('resolveObjectReferences', blockData, {}, async () => {
			// Keep track of the object references that will need to be resolved at the end of the transaction.
			// We keep the input by-reference to avoid needing to re-resolve it:
			const objectsToResolve = blockData.inputs.filter((input) => {
				return input.UnresolvedObject;
			}) as Extract<CallArg, { UnresolvedObject: unknown }>[];

			if (objectsToResolve.length) {
				const dedupedIds = [
					...new Set(
						objectsToResolve.map((input) => normalizeSuiObjectId(input.UnresolvedObject.value)),
					),
				];
				const objectChunks = chunk(dedupedIds, MAX_OBJECTS_PER_FETCH);
				const objects = (
					await Promise.all(
						objectChunks.map((chunk) =>
							this.#getClient().multiGetObjects({
								ids: chunk,
								options: { showOwner: true },
							}),
						),
					)
				).flat();

				let objectsById = new Map(
					dedupedIds.map((id, index) => {
						return [id, objects[index]];
					}),
				);

				const invalidObjects = Array.from(objectsById)
					.filter(([_, obj]) => obj.error)
					.map(([id, _]) => id);
				if (invalidObjects.length) {
					throw new Error(`The following input objects are invalid: ${invalidObjects.join(', ')}`);
				}

				objectsToResolve.forEach((input) => {
					let updated: CallArg | undefined;
					const id = normalizeSuiAddress(input.UnresolvedObject.value);
					const typeSignatures = input.UnresolvedObject.typeSignatures;
					const object = objectsById.get(id)!;
					const owner = object.data?.owner;
					const initialSharedVersion =
						owner && typeof owner === 'object' && 'Shared' in owner
							? owner.Shared.initial_shared_version
							: undefined;
					const isMutable = typeSignatures.some((typeSignature) => {
						// There could be multiple transactions that reference the same shared object.
						// If one of them is a mutable reference or taken by value, then we should mark the input
						// as mutable.
						const isByValue = !typeSignature.ref;
						return isMutableSharedObjectInput(input) || isByValue || typeSignature.ref === '&mut';
					});
					const isReceiving = !initialSharedVersion && typeSignatures.some(isReceivingType);

					if (initialSharedVersion) {
						updated = Inputs.SharedObjectRef({
							objectId: id,
							initialSharedVersion,
							mutable: isMutable,
						});
					} else if (isReceiving) {
						updated = Inputs.ReceivingRef(object.data!);
					}

					blockData.inputs[blockData.inputs.indexOf(input)] =
						updated ?? Inputs.ObjectRef(object.data!);
				});
			}
		});
	};

	normalizeInputs: BasePluginMethod<'normalizeInputs'> = async (blockData) => {
		await this.#runHook('normalizeInputs', blockData, {}, async () => {
			const { inputs, transactions } = blockData;
			const moveModulesToResolve: Extract<Transaction, { MoveCall: unknown }>['MoveCall'][] = [];

			transactions.forEach((transaction) => {
				// Special case move call:
				if (transaction.MoveCall) {
					// Determine if any of the arguments require encoding.
					// - If they don't, then this is good to go.
					// - If they do, then we need to fetch the normalized move module.

					const inputs = transaction.MoveCall.arguments.map((arg) => {
						if (arg.$kind === 'Input') {
							return blockData.inputs[arg.Input];
						}
						return null;
					});
					const needsResolution = inputs.some(
						(input) => input && (input.RawValue || input.UnresolvedObject),
					);

					if (needsResolution) {
						moveModulesToResolve.push(transaction.MoveCall);
					}
				}

				// Special handling for values that where previously encoded using the wellKnownEncoding pattern.
				// This should only happen when transaction block data was hydrated from an old version of the SDK
				switch (transaction.$kind) {
					case 'SplitCoins':
						transaction.SplitCoins[1].forEach((amount) => {
							this.#normalizeRawArgument(amount, bcs.U64, blockData);
						});
						break;
					case 'TransferObjects':
						this.#normalizeRawArgument(transaction.TransferObjects[1], bcs.Address, blockData);
						break;
				}
			});

			if (moveModulesToResolve.length) {
				await Promise.all(
					moveModulesToResolve.map(async (moveCall) => {
						const normalized = await this.#getClient().getNormalizedMoveFunction({
							package: moveCall.package,
							module: moveCall.module,
							function: moveCall.function,
						});

						// Entry functions can have a mutable reference to an instance of the TxContext
						// struct defined in the TxContext module as the last parameter. The caller of
						// the function does not need to pass it in as an argument.
						const hasTxContext =
							normalized.parameters.length > 0 && isTxContext(normalized.parameters.at(-1)!);

						const params = hasTxContext
							? normalized.parameters.slice(0, normalized.parameters.length - 1)
							: normalized.parameters;

						if (params.length !== moveCall.arguments.length) {
							throw new Error('Incorrect number of arguments.');
						}

						params.forEach((param, i) => {
							const arg = moveCall.arguments[i];
							if (arg.$kind !== 'Input') return;
							const input = inputs[arg.Input];
							// Skip if the input is already resolved
							if (!input.RawValue && !input.UnresolvedObject) return;

							const inputValue = input.RawValue?.value ?? input.UnresolvedObject?.value!;

							const typeSignature = normalizedTypeToMoveTypeSignature(param);

							const schema = getPureBcsSchema(typeSignature.body);
							if (schema) {
								inputs[inputs.indexOf(input)] = Inputs.Pure(schema.serialize(inputValue));
								return;
							}

							if (typeof inputValue !== 'string') {
								throw new Error(
									`Expect the argument to be an object id string, got ${JSON.stringify(
										inputValue,
										null,
										2,
									)}`,
								);
							}

							if (input.$kind === 'RawValue') {
								inputs[inputs.indexOf(input)] = {
									$kind: 'UnresolvedObject',
									UnresolvedObject: {
										value: inputValue,
										typeSignatures: [typeSignature],
									},
								};
							} else {
								input.UnresolvedObject.typeSignatures.push(typeSignature);
							}
						});
					}),
				);
			}
		});
	};

	resolveIntent: BasePluginMethod<'resolveIntent'> = async (blockData, options) => {
		this.#runHook('resolveIntent', blockData, options, async () => {
			for (const transaction of blockData.transactions) {
				if (
					transaction.$kind === 'TransactionIntent' &&
					transaction.TransactionIntent.name === options.name
				) {
					throw new Error(`Transaction intent ${options.name} has not been resolved`);
				}
			}
		});
	};

	async resolveUnsupportedIntents(
		blockData: TransactionBlockDataBuilder,
		supportedIntents: string[],
	) {
		const intentsToResolve = new Set<string>(supportedIntents);

		for (const transaction of blockData.transactions) {
			if (
				transaction.$kind === 'TransactionIntent' &&
				!supportedIntents.includes(transaction.TransactionIntent.name)
			) {
				intentsToResolve.add(transaction.TransactionIntent.name);
			}
		}

		for (const intent of intentsToResolve) {
			await this.resolveIntent(blockData, { name: intent });
		}
	}

	validate: BasePluginMethod<'validate'> = async (blockData, options) => {
		await this.#runHook('validate', blockData, options, async () => {
			// Validate all inputs are the correct size:
			blockData.inputs.forEach((input, index) => {
				if (input.Pure) {
					if (input.Pure.length > options.maxPureArgumentSize) {
						throw new Error(
							`Input at index ${index} is too large, max pure input size is ${options.maxPureArgumentSize} bytes, got ${input.Pure.length} bytes`,
						);
					}
				}
			});
		});
	};

	#normalizeRawArgument = (
		arg: Argument,
		schema: BcsType<any>,
		blockData: TransactionBlockDataBuilder,
	) => {
		if (arg.$kind !== 'Input') {
			return;
		}
		const input = blockData.inputs[arg.Input];

		if (input.$kind !== 'RawValue') {
			return;
		}

		blockData.inputs[arg.Input] = Inputs.Pure(schema.serialize(input.RawValue.value));
	};
}

function isReceivingType(type: OpenMoveTypeSignature): boolean {
	if (typeof type.body !== 'object' || !('datatype' in type.body)) {
		return false;
	}

	return (
		type.body.datatype.package === '0x2' &&
		type.body.datatype.module === 'transfer' &&
		type.body.datatype.type === 'Receiving'
	);
}
