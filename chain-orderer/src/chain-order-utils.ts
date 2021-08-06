import { Block, ChainOrderedTransaction } from "./database/bmt-db";
import { ChainRangeExtended } from "./database/chain-range-extended";
import { toU64String } from "./u64string";

export class ChainOrderUtils {
    static get_chain_range_chain_orders(chain_range: ChainRangeExtended): ChainRangeChainOrders {
        const master_order = toU64String(chain_range.master_block.seq_no);

        return {
            master_block: {
                id: chain_range.master_block.id,
                chain_order: `${master_order}m`,
                transactions: this.get_transactions_chain_orders_for_block(`${master_order}m`, chain_range.master_block),
            },
            shard_blocks: new Map(chain_range.shard_blocks.map(block => {
                const workchain_order = block.workchain_id >= 0 ? toU64String(block.workchain_id) : toU64String(-block.workchain_id);
                const seq_no_order = toU64String(block.seq_no);
                const shard_order = shard_to_reversed_to_U64String(block.shard);
                const inner_order = `${workchain_order}${seq_no_order}${shard_order}`;
                const block_order = `${master_order}${inner_order}`;

                return [
                    block.id, 
                    {
                        id: block.id,
                        chain_order: block_order,
                        transactions: this.get_transactions_chain_orders_for_block(block_order, block),
                    }
                ];
            })),
        }
    }

    private static get_transactions_chain_orders_for_block(block_order: string, block: Block): ChainOrderedTransaction[] {
        const transactions = [...block.transactions];
        transactions.sort((t1, t2) => {
            if (t1.lt < t2.lt) {
                return -1;
            }
            if (t1.lt > t2.lt) {
                return 1;
            }
            
            if (t1.account_addr < t2.account_addr) {
                return -1;
            }
            if (t1.account_addr > t2.account_addr) {
                return 1;
            }

            throw new Error(`Duplicate transaction lt (${t1.lt}) and account_addr (${t1.account_addr})`);
        });

        return transactions.map((t, index) => {
            return {
                id: t.id,
                chain_order: `${block_order}${toU64String(index)}`,
            };
        });
    }
}

export type ChainRangeChainOrders = {
    master_block: {
        id: string,
        chain_order: string,
        transactions: ChainOrderedTransaction[],
    },
    shard_blocks: Map<string, {
        id: string,
        chain_order: string,
        transactions: ChainOrderedTransaction[],
    }>,
};

function shard_to_reversed_to_U64String(shard: string) {
    let result = "";
    for (let i = 0; i < shard.length; i++) {
        const hex_symbol = shard[shard.length - i - 1];
        
        if (result.length == 0 && hex_symbol == "0") {
            continue; // skip zeros from the end
        }

        const reverse_hex_symbol = reverse_hex_symbol_map.get(hex_symbol);

        if (!reverse_hex_symbol) {
            throw new Error(`Reverse hex for ${hex_symbol} not found while processing shard ${shard}`);
        }

        result = `${result}${reverse_hex_symbol}`;
    }

    result = (result.length - 1).toString(16) + result;
    return result;
}

const reverse_hex_symbol_map = (function () {
    const result = new Map<string, string>();
    for (let i = 0; i < 16; i++) {
        const bit_string = i.toString(2).padStart(4, "0");
        const splitted = bit_string.split("");
        const reversedSplitted = splitted.reverse();
        const reversed_string = reversedSplitted.join("");
        const reversed_number = parseInt(reversed_string, 2);
        
        result.set(i.toString(16), reversed_number.toString(16));
    }

    return result;
})();
