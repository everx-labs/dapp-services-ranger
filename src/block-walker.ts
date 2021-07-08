import { BMT_IDs } from "./bmt-types";
import { IDbWrapper, MasterChainBlockInfo } from "./db-wrapper";
import { RangerConfig } from "./ranger-config";

export class BlockWalker {
    static MAX_DEPTH = 10;
    readonly buffer_db: IDbWrapper;
    readonly ordered_db: IDbWrapper;
    readonly masterchain_block_id: string;

    masterchain_block_info?: MasterChainBlockInfo;
    prev_masterchain_block_info?: MasterChainBlockInfo;

    // Graph
    root_shardchain_block_ids?: ReadonlyArray<string>;
    top_shardchain_block_ids?: ReadonlyArray<string>;
    blocks?: Map<string, ShardChainBlockGraphNode>;
    
    result?: BMT_IDs;

    constructor(buffer_db: IDbWrapper, ordered_db: IDbWrapper, masterchain_block_id: string) {
        this.buffer_db = buffer_db;
        this.ordered_db = ordered_db;
        this.masterchain_block_id = masterchain_block_id;
    }

    async run(): Promise<void> {
        await this.init_masterchain_block_info();
        await this.init_previous_masterchain_block_info();
        await this.init_graph();
        await this.walk_shardchain_block_and_fill_graph();
        await this.flatten_graph_and_prepare_result();
    }

    private async init_masterchain_block_info(): Promise<void> {
        this.masterchain_block_info = await this.buffer_db.get_masterchain_block_info_by_id(this.masterchain_block_id);

        if (!this.masterchain_block_info) {
            throw new Error(`Masterblock with id ${this.masterchain_block_id} not found in buffer DB`);
        }
    }

    private async init_previous_masterchain_block_info(): Promise<void> {
        const prev_masterblock_id = this.masterchain_block_info!.prev_block_id;
        this.prev_masterchain_block_info = await this.ordered_db.get_masterchain_block_info_by_id(prev_masterblock_id); 
        
        if (!this.prev_masterchain_block_info) {
            throw new Error(`Masterblock with id ${prev_masterblock_id} not found in ordered DB`);
        }
    }

    private async init_graph(): Promise<void> {
        this.root_shardchain_block_ids = this.prev_masterchain_block_info!.shard_block_ids;
        this.top_shardchain_block_ids = this.masterchain_block_info!.shard_block_ids;

        this.blocks = new Map<string, ShardChainBlockGraphNode>();

        this.root_shardchain_block_ids!.forEach(sh_id => {
            this.blocks!.set(sh_id, {
                type: "Root",
                id: sh_id,
            });
        });
    }

    private async walk_shardchain_block_and_fill_graph(): Promise<void> {
        const blocks = this.blocks!;

        let current_shardchain_block_ids = this.top_shardchain_block_ids!
            .filter(id => !blocks.get(id)) // skip root blocks
            .map(id => { return { id: id, depth: 0}; });

        while (true) {
            const current_block_info = current_shardchain_block_ids.shift();
            if (!current_block_info) {
                break;
            }

            if (current_block_info.depth > BlockWalker.MAX_DEPTH)
                throw new Error(`Max search depth of ${BlockWalker.MAX_DEPTH} exceeded`);

            let current_block_graph_node = blocks.get(current_block_info.id) as TopShardChainBlockGraphNode | MidShardChainBlockGraphNode;
            if (!current_block_graph_node) {
                current_block_graph_node = {
                    type: "Top",
                    id: current_block_info.id,
                };
                blocks.set(current_block_info.id, current_block_graph_node);
            }

            const current_block = await this.buffer_db.get_shardchain_block_info_by_id(current_block_info.id);
            if (!current_block) {
                throw new Error(`Shardchain block with id "${current_block_info.id}" not found`);
            }

            current_block_graph_node.gen_utime = current_block.gen_utime;
            current_block_graph_node.shard = current_block.shard;
            current_block_graph_node.workchain_id = current_block.workchain_id;
            current_block_graph_node.message_ids = current_block.message_ids;
            current_block_graph_node.transaction_ids = current_block.transaction_ids;

            const prev_block_id = current_block.prev_block_id;
            let prev_graph_node = get_prev_graph_node_and_attach_current_as_next(prev_block_id, current_block_graph_node, current_shardchain_block_ids, current_block_info.depth);
            current_block_graph_node.prev = prev_graph_node;

            const prev_alt_block_id = current_block.prev_alt_block_id;
            if (prev_alt_block_id) {
                let prev_alt_graph_node = get_prev_graph_node_and_attach_current_as_next(prev_alt_block_id, current_block_graph_node, current_shardchain_block_ids, current_block_info.depth);
                current_block_graph_node.prev_alt = prev_alt_graph_node;
            }
        }

        // Helper functions
        function get_prev_graph_node_and_attach_current_as_next(
            prev_block_id: string, 
            current_block_graph_node: TopShardChainBlockGraphNode | MidShardChainBlockGraphNode,
            current_shardchain_block_ids: { id: string, depth: number }[],
            current_depth: number,
        ) {
            let prev_graph_node = blocks.get(prev_block_id) as RootShardchainBlockGraphNode | MidShardChainBlockGraphNode;
            if (prev_graph_node) {
                if (!prev_graph_node.next) {
                    // This case is for "Root" nodes
                    prev_graph_node.next = current_block_graph_node;
                } else if (!prev_graph_node.next_alt) {
                    prev_graph_node.next_alt = current_block_graph_node;
                } else {
                    throw new Error(`Triple next block for block "${prev_graph_node.id}"`);
                }
            } else {
                prev_graph_node = {
                    type: "Mid",
                    id: prev_block_id,
                    next: current_block_graph_node,
                };
                blocks.set(prev_block_id, prev_graph_node);
                current_shardchain_block_ids.push({ id: prev_block_id, depth: current_depth + 1 });
            }
            return prev_graph_node;
        }
    }

    private async flatten_graph_and_prepare_result(): Promise<void> {
        this.result = Array.from(this.blocks!.values())
            .filter(f => f.type != "Root")
            .sort((a, b) =>
                a.gen_utime != b.gen_utime
                ? a.gen_utime! - b.gen_utime!
                : (a.workchain_id != b.workchain_id
                ? a.workchain_id!.localeCompare(b.workchain_id!)
                : a.shard!.localeCompare(b.shard!)))
            .map(node => {
                return {
                    block_id: node.id,
                    transaction_ids: node.transaction_ids,
                    message_ids: node.message_ids,
                };
            })
            .concat({
                block_id: this.masterchain_block_info!._key,
                transaction_ids: this.masterchain_block_info!.transaction_ids,
                message_ids: this.masterchain_block_info!.message_ids,
            })
            .reduce((result, current) => {
                result.block_ids.push(current.block_id);
                result.message_ids.push(...current.message_ids!);
                result.transaction_ids.push(...current.transaction_ids!);
                return result;
            }, {
                block_ids: [] as string[],
                transaction_ids: [] as string[],
                message_ids: [] as string[],
            });

        if (RangerConfig.debug) {
            console.log(this.result);
        }
    }
}

export type RootShardchainBlockGraphNode = {
    type: "Root",
    id: string,
    next?: ShardChainBlockGraphNode,
    next_alt?: ShardChainBlockGraphNode,
    gen_utime?: number,
    workchain_id?: string,
    shard?: string,
    message_ids?: string[],
    transaction_ids?: string[],
};

export type TopShardChainBlockGraphNode = {
    type: "Top",
    id: string,
    prev?: ShardChainBlockGraphNode,
    prev_alt?: ShardChainBlockGraphNode,
    gen_utime?: number,
    workchain_id?: string,
    shard?: string,
    message_ids?: string[],
    transaction_ids?: string[],
};

export type MidShardChainBlockGraphNode = {
    type: "Mid",
    id: string,
    next: ShardChainBlockGraphNode,
    next_alt?: ShardChainBlockGraphNode,
    prev?: ShardChainBlockGraphNode,
    prev_alt?: ShardChainBlockGraphNode,
    gen_utime?: number,
    workchain_id?: string,
    shard?: string,
    message_ids?: string[],
    transaction_ids?: string[],
};

export type ShardChainBlockGraphNode = RootShardchainBlockGraphNode | MidShardChainBlockGraphNode | TopShardChainBlockGraphNode;