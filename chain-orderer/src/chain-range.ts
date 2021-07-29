import { MasterChainBlock, ShardChainBlock } from "./bmt-db";

export type ChainRange = {
    master_block: MasterChainBlock,
    shard_blocks: ShardChainBlock[],
};