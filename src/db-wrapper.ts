import { Database, aql } from "arangojs";
import { DocumentCollection } from "arangojs/collection";
import { Config } from "arangojs/connection";
import { ArrayCursor } from "arangojs/cursor";

export enum Collection {
    accounts = "accounts",
    blocks = "blocks",
    messages = "messages",
    transactions = "transactions",
    blocks_signatures = "blocks_signatures",
    zerostates = "zerostates",
}

export type MasterChainBlockInfo = {
    _key: string;
    seq_no: number,
    gen_utime: number,
    prev_ref: {
        seq_no: number,
        root_hash: string,
    },
    shards: ShardChainBlockRef[],
};

export type ShardChainBlockRef = {
    root_hash: string,
    seq_no: number,
    before_split: boolean,
    before_merge: boolean,
    workchain_id: string,
    shard: string,
}

export type ShardChainBlockInfo = {
    _key: string,
    gen_utime: number,
    workchain_id: string;
    seq_no: number,
    shard: string,
    after_split: boolean,
    after_merge: boolean,
    prev_ref: {
        seq_no: number,
        root_hash: string,
    },
    prev_alt_ref: {
        seq_no: number | null,
        root_hash: string | null,
    },
};

export class DbWrapper {
    readonly db: Database;
    readonly blocks_collection: DocumentCollection;

    constructor(config: Config) {
        this.db = new Database(config);
        this.blocks_collection = this.db.collection(Collection.blocks);
    }

    async get_last_masterchain_block_seqno(): Promise<number> {
        const get_last_masterchain_block_seqno = await this.db.query(aql`
            FOR b IN ${this.blocks_collection}
            FILTER b.workchain_id == -1
            SORT b.seq_no DESC
            LIMIT 1
            RETURN b.seq_no
        `);

        return await get_last_masterchain_block_seqno.next();
    }

    async get_last_masterchain_block_id(): Promise<string> {
        const get_last_masterchain_block_seqno = await this.db.query(aql`
            FOR b IN ${this.blocks_collection}
            FILTER b.workchain_id == -1
            SORT b.seq_no DESC
            LIMIT 1
            RETURN b._key
        `);

        return await get_last_masterchain_block_seqno.next();
    }

    async get_first_masterchain_block_id_with_seq_no_not_less_than(seq_no: number): Promise<string | undefined> {
        const get_last_masterchain_block_seqno = await this.db.query(aql`
            FOR b IN ${this.blocks_collection}
            FILTER b.workchain_id == -1 && b.seq_no >= ${seq_no}
            SORT b.seq_no ASC
            LIMIT 1
            RETURN b._key
        `);

        return await get_last_masterchain_block_seqno.next();
    }

    async get_masterchain_block_info_by_id(id: string): Promise<MasterChainBlockInfo | undefined> {
        const masterchain_block_query: ArrayCursor<MasterChainBlockInfo> = await this.db.query(aql`
            FOR b IN ${this.blocks_collection}
            FILTER b.workchain_id == -1 && b._key == ${id}
            RETURN { 
                "_key": b._key,
                "gen_utime": b.gen_utime,
                "seq_no": b.seq_no,
                "prev_ref": {
                    "seq_no": b.prev_ref.seq_no,
                    "root_hash": b.prev_ref.root_hash,
                },
                "shards": (
                    FOR sh IN b.master.shard_hashes
                    RETURN {
                        "seq_no": sh.descr.seq_no,
                        "root_hash": sh.descr.root_hash,
                        "workchain_id": sh.workchain_id,
                        "shard": sh.shard,
                        "before_split": sh.descr.before_split,
                        "before_merge": sh.descr.before_merge,
                    }
                ),
            }
        `);

        return await masterchain_block_query.next();
    }

    async get_shardchain_block_info_by_id(id: string): Promise<ShardChainBlockInfo | undefined> {
        const shardchain_block_query: ArrayCursor<ShardChainBlockInfo> = await this.db.query(aql`
            FOR b IN ${this.db.collection(Collection.blocks)}
            FILTER b._key == ${id}
            RETURN { 
                "_key": b._key,
                "gen_utime": b.gen_utime,
                "workchain_id": b.workchain_id,
                "seq_no": b.seq_no,
                "shard": b.shard,
                "after_merge": b.after_merge,
                "after_split": b.after_split,
                "prev_ref": {
                    "seq_no": b.prev_ref.seq_no,
                    "root_hash": b.prev_ref.root_hash,
                },
                "prev_alt_ref": {
                    "seq_no": b.prev_alt_ref.seq_no,
                    "root_hash": b.prev_alt_ref.root_hash,
                },
            }
        `);

        return await shardchain_block_query.next();
    }
}