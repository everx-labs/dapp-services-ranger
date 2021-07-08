import { Database, aql } from "arangojs";
import { DocumentCollection } from "arangojs/collection";
import { Config } from "arangojs/connection";

import { BMT, BMT_IDs } from "./bmt-types";
import { RangerConfig } from "./ranger-config";

export interface IBmtDb {
    get_existing_message_ids_from_id_array(message_ids: string[]): Promise<string[]>;
    get_existing_transaction_ids_from_id_array(transaction_ids: string[]): Promise<string[]>;
    get_first_masterchain_block_id_with_seq_no_not_less_than(min_seq_no: number): Promise<string | undefined>;
    get_masterchain_block_info_by_id(id: string): Promise<MasterChainBlockInfo | undefined>;
    get_shardchain_block_info_by_id(id: string): Promise<ShardChainBlockInfo | undefined>;
    get_last_masterchain_block_seq_no(): Promise<number | undefined>;
    init_from(source: IBmtDb, init_seq_no: number): Promise<void>;

    add_BMT(bmt: BMT): Promise<void>;
    get_BMT_by_ids(bmt_ids: BMT_IDs): Promise<BMT>;
    remove_BMT_by_ids(bmt_ids: BMT_IDs): Promise<void>;
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
            FILTER b._key == ${id}
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

    async init_from(source: IBmtDb, init_seq_no: number): Promise<void> {
        const collections = await this.arango_db.collections();

        if (!collections.find(c => c.name == "blocks")) {
            await this.arango_db.createCollection("blocks");
        }
        
        if (!collections.find(c => c.name == "messages")) {
            await this.arango_db.createCollection("messages");
        }
        
        if (!collections.find(c => c.name == "transactions")) {
            await this.arango_db.createCollection("transactions");
        }

        const masterchain_block_id = await source.get_first_masterchain_block_id_with_seq_no_not_less_than(init_seq_no);
        if (!masterchain_block_id) {
            throw new Error(`There is no masterchain block with seq_no >= ${init_seq_no} to init from`);
        }

        const masterchain_block = await source.get_masterchain_block_info_by_id(masterchain_block_id);
        const bmt_ids = {
            block_ids: [ masterchain_block_id ],
            message_ids: masterchain_block!.message_ids,
            transaction_ids: masterchain_block!.transaction_ids,
        };
        const bmt = await source.get_BMT_by_ids(bmt_ids);
        await this.add_BMT(bmt);
        
        if (!RangerConfig.test_mode) {
            await source.remove_BMT_by_ids(bmt_ids);
        }
    }
    
    async add_BMT(bmt: BMT): Promise<void> {
        const transaction = await this.arango_db.beginTransaction({
            write: [ "blocks", "transactions", "messages" ],
        });
        await transaction.step(() => this.blocks.saveAll(bmt.blocks, { overwriteMode: "ignore" }));
        await transaction.step(() => this.transactions.saveAll(bmt.transactions, { overwriteMode: "ignore" }));
        await transaction.step(() => this.messages.saveAll(bmt.messages, { overwriteMode: "ignore" }));
        await transaction.commit();
    }

    async get_BMT_by_ids(bmt_ids: BMT_IDs): Promise<BMT> {
        const query = await this.arango_db.query(aql`
            RETURN {
                blocks: (
                    FOR b IN ${this.blocks}
                    FILTER b._key IN ${bmt_ids.block_ids}
                    RETURN b
                ),
                messages: (
                    FOR m IN ${this.messages}
                    FILTER m._key IN ${bmt_ids.message_ids}
                    RETURN m
                ),
                transactions: (
                    FOR t IN ${this.transactions}
                    FILTER t._key IN ${bmt_ids.transaction_ids}
                    RETURN t
                )
            }
        `);

        return await query.next();
    }

    async remove_BMT_by_ids(bmt_ids: BMT_IDs): Promise<void> {
        const transaction = await this.arango_db.beginTransaction({
            write: [ "blocks", "transactions", "messages" ],
        });
        await transaction.step(() => this.blocks.removeAll(bmt_ids.block_ids));
        await transaction.step(() => this.transactions.removeAll(bmt_ids.transaction_ids));
        await transaction.step(() => this.messages.removeAll(bmt_ids.message_ids));
        await transaction.commit();
    }
}