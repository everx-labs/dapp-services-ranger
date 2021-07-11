import { Database, aql } from "arangojs";
import { DocumentCollection } from "arangojs/collection";
import { Config } from "arangojs/connection";

export interface IBmtDb {
    get_existing_message_ids_from_id_array(message_ids: string[]): Promise<string[]>;
    get_existing_transaction_ids_from_id_array(transaction_ids: string[]): Promise<string[]>;
    get_first_masterchain_block_id_with_seq_no_not_less_than(min_seq_no: number): Promise<string | undefined>;
    get_masterchain_block_info_by_id(id: string): Promise<MasterChainBlockInfo | undefined>;
    get_shardchain_block_info_by_id(id: string): Promise<ShardChainBlockInfo | undefined>; // not used at the time of writing
    get_shardchain_block_infos_by_ids(ids: string[]): Promise<ShardChainBlockInfo[]>;
    get_last_masterchain_block_seq_no(): Promise<number | undefined>; // not used at the time of writing
}

export type MasterChainBlockInfo = {
    _key: string;
    seq_no: number,
    gen_utime: number,
    prev_block_id: string,
    shard_block_ids: string[],
    message_ids: string[],
    transaction_ids: string[],
};

export type ShardChainBlockInfo = {
    _key: string,
    gen_utime: number,
    workchain_id: string;
    seq_no: number,
    shard: string,
    prev_block_id: string,
    prev_alt_block_id: string | null,
    message_ids: string[],
    transaction_ids: string[],
};

export class BmtDb implements IBmtDb {
    readonly arango_db: Database;
    readonly blocks: DocumentCollection;
    readonly messages: DocumentCollection;
    readonly transactions: DocumentCollection;
    
    constructor(config: Config) {
        this.arango_db = new Database(config);
        this.blocks = this.arango_db.collection("blocks");
        this.messages = this.arango_db.collection("messages");
        this.transactions = this.arango_db.collection("transactions");
    }

    async get_existing_message_ids_from_id_array(message_ids: string[]): Promise<string[]> {
        const query = await this.arango_db.query(aql`
            FOR m IN ${this.messages}
            FILTER m._key IN ${message_ids}
            RETURN m._key
        `);

        return await query.all();
    }
    
    async get_existing_transaction_ids_from_id_array(transaction_ids: string[]): Promise<string[]> {
        const query = await this.arango_db.query(aql`
            FOR t IN ${this.transactions}
            FILTER t._key IN ${transaction_ids}
            RETURN t._key
        `);

        return await query.all();   
    }

    async get_first_masterchain_block_id_with_seq_no_not_less_than(min_seq_no: number): Promise<string | undefined> {
        const query = await this.arango_db.query(aql`
            FOR b IN ${this.blocks}
            FILTER b.workchain_id == -1 && b.seq_no >= ${min_seq_no}
            SORT b.seq_no ASC
            LIMIT 1
            RETURN b._key
        `);

        return await query.next();
    }

    async get_masterchain_block_info_by_id(id: string): Promise<MasterChainBlockInfo | undefined> {
        const query = await this.arango_db.query(aql`
            FOR b IN ${this.blocks}
            FILTER b.workchain_id == -1 && b._key == ${id}
            RETURN { 
                "_key": b._key,
                "seq_no": b.seq_no,
                "gen_utime": b.gen_utime,
                "prev_block_id": b.prev_ref.root_hash,
                "shard_block_ids": (
                    FOR sh IN b.master.shard_hashes
                    RETURN sh.descr.root_hash
                ),
                "message_ids": (FOR m_id IN UNION_DISTINCT(
                        b.out_msg_descr[*].msg_id,
                        b.out_msg_descr[*].out_msg.msg_id,
                        b.out_msg_descr[*].reimport.msg_id,
                        b.in_msg_descr[*].msg_id,
                        b.in_msg_descr[*].in_msg.msg_id
                    ) FILTER m_id != null RETURN m_id),
                "transaction_ids": (FOR t_id IN UNION_DISTINCT(
                        b.out_msg_descr[*].transaction_id,
                        b.out_msg_descr[*].reimport.transaction_id
                    ) FILTER t_id != null RETURN t_id),
            }
        `);

        return await query.next();
    }

    async get_shardchain_block_info_by_id(id: string): Promise<ShardChainBlockInfo | undefined> {
        const query = await this.arango_db.query(aql`
            FOR b IN ${this.blocks}
            FILTER b.workchain_id != -1 && b._key == ${id}
            RETURN { 
                "_key": b._key,
                "gen_utime": b.gen_utime,
                "workchain_id": b.workchain_id,
                "seq_no": b.seq_no,
                "shard": b.shard,
                "prev_block_id": b.prev_ref.root_hash,
                "prev_alt_block_id": b.prev_alt_ref.root_hash,
                "message_ids": (FOR m_id IN UNION_DISTINCT(
                        b.out_msg_descr[*].msg_id,
                        b.out_msg_descr[*].out_msg.msg_id,
                        b.out_msg_descr[*].reimport.msg_id,
                        b.in_msg_descr[*].msg_id,
                        b.in_msg_descr[*].in_msg.msg_id
                    ) FILTER m_id != null RETURN m_id),
                "transaction_ids": (FOR t_id IN UNION_DISTINCT(
                        b.out_msg_descr[*].transaction_id,
                        b.out_msg_descr[*].reimport.transaction_id
                    ) FILTER t_id != null RETURN t_id),
            }
        `);

        return await query.next();
    }

    async get_shardchain_block_infos_by_ids(ids: string[]): Promise<ShardChainBlockInfo[]> {
        const query = await this.arango_db.query(aql`
            FOR b IN ${this.blocks}
            FILTER b._key IN ${ids} && b.workchain_id != -1
            RETURN { 
                "_key": b._key,
                "gen_utime": b.gen_utime,
                "workchain_id": b.workchain_id,
                "seq_no": b.seq_no,
                "shard": b.shard,
                "prev_block_id": b.prev_ref.root_hash,
                "prev_alt_block_id": b.prev_alt_ref.root_hash,
                "message_ids": (FOR m_id IN UNION_DISTINCT(
                        b.out_msg_descr[*].msg_id,
                        b.out_msg_descr[*].out_msg.msg_id,
                        b.out_msg_descr[*].reimport.msg_id,
                        b.in_msg_descr[*].msg_id,
                        b.in_msg_descr[*].in_msg.msg_id
                    ) FILTER m_id != null RETURN m_id),
                "transaction_ids": (FOR t_id IN UNION_DISTINCT(
                        b.out_msg_descr[*].transaction_id,
                        b.out_msg_descr[*].reimport.transaction_id
                    ) FILTER t_id != null RETURN t_id),
            }
        `);

        return await query.all();
    }

    async get_last_masterchain_block_seq_no(): Promise<number | undefined> {
        const query = await this.arango_db.query(aql`
            FOR b IN ${this.blocks}
            FILTER b.workchain_id == -1
            SORT b.seq_no DESC
            LIMIT 1
            RETURN b.seq_no
        `);

        return await query.next();
    }
}