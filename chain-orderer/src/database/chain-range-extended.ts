import { MasterChainBlock, ShardChainBlock } from "./bmt-db";

export type ChainRangeExtended = {
    master_block: MasterChainBlock,
    shard_blocks: ShardChainBlock[],
};
