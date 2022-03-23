import { aql, Database } from "arangojs";
import { Config } from "arangojs/connection";

export class BmtDb {
    readonly config: Config;
    readonly arango_db: Database;
    summary: DbSummary;

    private constructor(config: Config) {
        this.config = config;
        this.arango_db = new Database(config);
        this.summary = {
            min_time: -1,
            max_time: -1,
            min_mc_seq_no: null,
            max_mc_seq_no: null,
        }
    }

    static async create(config: Config): Promise<BmtDb> {
        const db = new BmtDb(config);
        await db.update_summary();
        return db;
    }

    async update_summary(): Promise<void> {
        const cursor = await this.arango_db.query(aql`
            RETURN {
                min_time: (
                    FOR b IN blocks
                        SORT b.gen_utime ASC
                        LIMIT 1
                        RETURN b.gen_utime)[0],
                max_time: (
                    FOR b IN blocks
                        SORT b.gen_utime DESC
                        LIMIT 1
                        RETURN b.gen_utime)[0],
                min_mc_seq_no: (
                    FOR b IN blocks
                        FILTER b.workchain_id == -1
                        SORT b.seq_no ASC
                        LIMIT 1
                        RETURN b.seq_no)[0],
                max_mc_seq_no: (
                    FOR b IN blocks
                        FILTER b.workchain_id == -1
                        SORT b.seq_no DESC
                        LIMIT 1
                        RETURN b.seq_no)[0],
            }
        `);
        this.summary = await cursor.next() as DbSummary;

        if (!this.summary.min_time || !this.summary.max_time) {
            throw new Error(`Invalid database ${this.arango_db.name}: there is no blocks`);
        }
    }

    async get_masterchain_block_by_seq_no(seq_no: number): Promise<MasterChainBlock> {
        const cursor = await this.arango_db.query(`
            FOR b IN blocks
                FILTER b.workchain_id == -1 && b.seq_no == ${seq_no} && !b.technical_fork
                LET message_ids = (
                    FOR m_id IN UNION_DISTINCT(
                        // OutMsg::Immediately, OutMsg::New
                        FOR m IN b.out_msg_descr
                        FILTER m.msg_type == 1 || m.msg_type == 2
                        RETURN m.out_msg.msg_id,
                        
                        // OutMsg::External
                        FOR m IN b.out_msg_descr
                        FILTER m.msg_type == 0
                        RETURN m.msg_id,
                        
                        // InMsg::External
                        FOR m IN b.in_msg_descr
                        FILTER m.msg_type == 0
                        RETURN m.msg_id,
                        
                        // special messages with src "-1:00..00"
                        [b.master.recover_create_msg.in_msg.msg_id, b.master.mint_msg.in_msg.msg_id])
                    FILTER m_id != null
                    RETURN m_id)
                RETURN { 
                    "id": b._key,
                    "chain_order": b.chain_order,
                    "gen_utime": b.gen_utime,
                    "seq_no": b.seq_no,
                    "prev_block_id": b.prev_ref.root_hash,
                    "shard_block_ids": (
                        FOR sh IN b.master.shard_hashes
                        RETURN sh.descr.root_hash
                    ),
                    "message_ids": message_ids,
                    "messages": (
                        FOR m IN messages
                        FILTER m._key IN message_ids
                        RETURN {
                            "id": m._key,
                            "created_lt": m.created_lt,
                            "chain_order": m.chain_order,
                        }),
                    "transactions": (
                        FOR a_b IN b.account_blocks || []
                            FOR t IN a_b.transactions
                            RETURN {
                                "id": t.transaction_id,
                                "lt": t.lt,
                                "account_addr": a_b.account_addr,
                            }),
                }
        `);
        const block = await cursor.next() as MasterChainBlock | null;

        if (!block) {
            throw new Error(`Masterblock with seq_no ${seq_no} not found`);
        }

        if (block.message_ids.length != block.messages.length) {
            const found = new Set(block.messages.map(m => m.id));
            const not_found_ids = block.message_ids.filter(id => !found.has(id));
            throw new Error(`Messages not found: ${not_found_ids.join(", ")} for block ${block.id}`);
        }

        return block;
    }

    async find_shardchain_blocks_by_ids(ids: string[]): Promise<ShardChainBlock[]> {
        const cursor = await this.arango_db.query(aql`
            FOR b IN blocks
                FILTER b._key IN ${ids} && b.workchain_id != -1 && !b.technical_fork
                LET message_ids = (
                    FOR m_id IN UNION_DISTINCT(
                        // OutMsg::Immediately, OutMsg::New
                        FOR m IN b.out_msg_descr
                        FILTER m.msg_type == 1 || m.msg_type == 2
                        RETURN m.out_msg.msg_id,
                        
                        // OutMsg::External
                        FOR m IN b.out_msg_descr
                        FILTER m.msg_type == 0
                        RETURN m.msg_id,
                        
                        // InMsg::External
                        FOR m IN b.in_msg_descr
                        FILTER m.msg_type == 0
                        RETURN m.msg_id)
                    FILTER m_id != null
                    RETURN m_id)
                RETURN { 
                    "id": b._key,
                    "chain_order": b.chain_order,
                    "gen_utime": b.gen_utime,
                    "workchain_id": b.workchain_id,
                    "seq_no": b.seq_no,
                    "shard": b.shard,
                    "prev_block_id": b.prev_ref.root_hash,
                    "prev_alt_block_id": b.prev_alt_ref.root_hash,
                    "message_ids": message_ids,
                    "messages": (
                        FOR m IN messages
                        FILTER m._key IN message_ids
                        RETURN {
                            "id": m._key,
                            "created_lt": m.created_lt,
                            "chain_order": m.chain_order,
                        }),
                    "transactions": (
                        FOR a_b IN b.account_blocks || []
                            FOR t IN a_b.transactions
                            RETURN {
                                "id": t.transaction_id,
                                "lt": t.lt,
                                "account_addr": a_b.account_addr,
                            }),
                }
        `);
        const blocks = await cursor.all() as ShardChainBlock[];

        for (const block of blocks) {       
            if (block.message_ids.length != block.messages.length) {
                const found = new Set(block.messages.map(m => m.id));
                const not_found_ids = block.message_ids.filter(id => !found.has(id));
                throw new Error(`Messages not found: ${not_found_ids.join(", ")} for block ${block.id}`);
            }
        }

        return blocks;
    }

    async set_mt_chain_orders(messages: ChainOrderedEntity[], transactions: ChainOrderedEntity[]): Promise<void> {
        const cursor = await this.arango_db.query(aql`
            RETURN {
                "messages": (
                    FOR c_o IN ${messages}
                    UPDATE c_o.id 
                    WITH { "chain_order": c_o.chain_order } 
                    IN messages
                    OPTIONS { waitForSync: true, ignoreErrors: true }
                    RETURN OLD._key
                ),
                "transactions": (
                    FOR c_o IN ${transactions}
                    UPDATE c_o.id 
                    WITH { "chain_order": c_o.chain_order } 
                    IN transactions
                    OPTIONS { waitForSync: true, ignoreErrors: true }
                    RETURN OLD._key
                )
            }
        `);
        const result = await cursor.next() as {
            messages: string[],
            transactions: string[],
        };
        if (result.messages.length != messages.length) {
            const not_found_ids = messages
                .filter(c_o => !result.messages.find(id => c_o.id == id))
                .map(c_o => c_o.id);
            throw new Error(`Messages not found: ${not_found_ids.join(", ")}`);
        }
        if (result.transactions.length != transactions.length) {
            const not_found_ids = transactions
                .filter(c_o => !result.transactions.find(id => c_o.id == id))
                .map(c_o => c_o.id);
            throw new Error(`Transactions not found: ${not_found_ids.join(", ")}`);
        }
    }
    
    async get_transactions_chain_orders_by_ids(ids: string[]): Promise<ChainOrderedEntity[]> {
        const cursor = await this.arango_db.query(aql`
            FOR t IN transactions
                FILTER t._key IN ${ids}
                RETURN {
                    "id": t._key,
                    "chain_order": t.chain_order,
                }
        `);
        const transactions = await cursor.all() as ChainOrderedEntity[];
        if (transactions.length != ids.length) {
            const not_found_ids = ids
                .filter(id => !transactions.find(t => t.id == id));
            throw new Error(`Transactions not found: ${not_found_ids.join(", ")}`);
        }
        
        return transactions;
    }

    async set_chain_order_for_block(block: Block, chain_order: string): Promise<void> {
        await this.arango_db.query(aql`
            FOR b IN blocks
            FILTER b._key == ${block.id}
            UPDATE b._key
                WITH { "chain_order": ${chain_order} } 
                IN blocks
                OPTIONS { waitForSync: true }
        `);
    }

    async verify_bmt_existance_for_block(block: Block): Promise<void> {
        const message_ids = block.messages.map(m => m.id);
        const transaction_ids = block.transactions.map(t => t.id);
        const cursor = await this.arango_db.query(aql`
            RETURN {
                b_count: (
                    FOR b IN blocks
                    FILTER b._key == ${block.id}
                    COLLECT WITH COUNT INTO c
                    RETURN c)[0],
                m_count: (
                    FOR m IN messages
                    FILTER t._key IN ${message_ids}
                    COLLECT WITH COUNT INTO c
                    RETURN c)[0]
                t_count: (
                    FOR t IN transactions
                    FILTER t._key IN ${transaction_ids}
                    COLLECT WITH COUNT INTO c
                    RETURN c)[0]
            }
        `);
        const queryResult = await cursor.next() as {
            b_count: number,
            m_count: number,
            t_count: number,
        };
        if (queryResult.b_count != 1) {
            throw new Error(`Block with id ${block.id} not found. Time: ${block.gen_utime}.`);
        }
        if (queryResult.m_count != message_ids.length) {
            throw new Error(`While verifying block with id ${block.id} expected ${message_ids.length} messages but got ${queryResult.m_count}. Time: ${block.gen_utime}.`);
        }
        if (queryResult.t_count != transaction_ids.length) {
            throw new Error(`While verifying block with id ${block.id} expected ${transaction_ids.length} transactions but got ${queryResult.t_count}. Time: ${block.gen_utime}.`);
        }
    }

    async ensure_bmt_chain_order_indexes(): Promise<void> {
        const blocks = this.arango_db.collection("blocks");
        await blocks.ensureIndex({ type: "persistent", fields: ["chain_order"]});
        await blocks.ensureIndex({ type: "persistent", fields: ["gen_utime", "chain_order"]});

        const transactions = this.arango_db.collection("transactions");
        await transactions.ensureIndex({ type: "persistent", fields: ["chain_order"]});
        await transactions.ensureIndex({ type: "persistent", fields: ["account_addr", "chain_order"]});
        await transactions.ensureIndex({ type: "persistent", fields: ["workchain_id", "chain_order"]});

        const messages = this.arango_db.collection("messages");
        await messages.ensureIndex({ type: "persistent", fields: ["chain_order"]});
    }
}

export type DbSummary = {
    min_time: number,
    max_time: number,
    min_mc_seq_no: number | null,
    max_mc_seq_no: number | null,
}

export type Message = {
    id: string,
    created_lt: string | null,
    chain_order: string | null,
};

export type Transaction = {
    id: string,
    lt: string,
    account_addr: string,
};

export type ChainOrderedEntity = {
    id: string,
    chain_order: string,
};

export type Block = {
    id: string,
    chain_order: string | null,
    gen_utime: number,
    seq_no: number,
    transactions: Transaction[],
    messages: Message[],
    message_ids: string[],
};

export type MasterChainBlock = Block & {
    prev_block_id: string,
    shard_block_ids: string[],
};

export type ShardChainBlock = Block & {
    workchain_id: number;
    shard: string,
    prev_block_id: string,
    prev_alt_block_id: string | null,
};
