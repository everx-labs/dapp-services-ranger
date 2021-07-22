import { aql, Database } from "arangojs";
import { Config } from "arangojs/connection";
import { toU64String } from "./u64string";

export class DistributedBmtDb {
    readonly config: Config[];
    readonly databases: Promise<{
        db: Database,
        min_time: number,
        max_time: number,
        min_mc_seq_no: number | null,
        max_mc_seq_no: number | null,
        last_chain_ordered_mc_seq_no: number | null,
        bmt_summary_exists: boolean,
    }[]>;

    constructor(config: Config[]) {
        this.config = config;
        this.databases = this.init_databases(config);
    }

    async init_databases(config: Config[]) {
        const databases = await Promise.all(
            config.map(async db_config => {
                const db = new Database(db_config);

                const cursor = await db.query(aql`
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
                const db_info = await cursor.next();

                let last_chain_ordered_mc_seq_no = null;
                if (await db.collection("bmt").exists()) {
                    const cursor2 = await db.query(aql`
                        FOR info IN bmt
                        FILTER info._key == "summary"
                        RETURN info.chain_range.end
                    `);

                    const chain_range_end = await cursor2.next() as string | null;
                    if (chain_range_end) {
                        const length = (+chain_range_end[0]) + 1;
                        const seq_no_string = chain_range_end.slice(1, length + 1);
                        last_chain_ordered_mc_seq_no = parseInt(seq_no_string, 16) - 1;
                    }
                }

                if (!db_info.min_time || !db_info.max_time) {
                    throw new Error(`There is no blocks in database ${db.name}`);
                }

                return {
                    db,
                    min_time: db_info.min_time,
                    max_time: db_info.max_time,
                    min_mc_seq_no: db_info.min_mc_seq_no,
                    max_mc_seq_no: db_info.max_mc_seq_no,
                    last_chain_ordered_mc_seq_no,
                    bmt_summary_exists: last_chain_ordered_mc_seq_no != null,
                };
            }));

        databases.sort(db => db.min_time);

        return databases;
    }

    async get_max_processed_seq_no() {
        const databases = await this.databases;
        return databases.reduce<number>((max_processed_mc_seq_no, curr) => {
            // max_mc_seq_no comparisson is needed for the case 
            // when the script failed while updating bmt collection
            // in two or more databases sequentially
            return (
                curr.last_chain_ordered_mc_seq_no &&
                curr.last_chain_ordered_mc_seq_no > max_processed_mc_seq_no &&
                curr.max_mc_seq_no && 
                curr.max_mc_seq_no >= curr.last_chain_ordered_mc_seq_no)
                ? curr.last_chain_ordered_mc_seq_no
                : max_processed_mc_seq_no;
        }, -1)
    }

    async get_max_mc_seq_no() {
        const databases = await this.databases;
        return databases.reduce<number>((max_mc_seq_no, curr) => {
            return (
                curr.max_mc_seq_no && 
                curr.max_mc_seq_no > max_mc_seq_no)
                ? curr.max_mc_seq_no
                : max_mc_seq_no;
        }, -1)
    }

    async get_masterchain_block_by_seq_no(seq_no: number): Promise<MasterChainBlock> {
        const query = aql`
            FOR b IN blocks
            FILTER b.workchain_id == -1 && b.seq_no == ${seq_no}
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
                "message_ids": (
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
                    RETURN m_id),
                "transactions": (
                    FOR a_b IN b.account_blocks || []
                        FOR t IN a_b.transactions
                        RETURN {
                            "id": t.transaction_id,
                            "lt": t.lt,
                            "account_addr": a_b.account_addr,
                        }),
            }
        `;

        const databases = await this.databases;
        const db = databases.find(db => 
            db.min_mc_seq_no && seq_no >= db.min_mc_seq_no && 
            db.max_mc_seq_no && seq_no <= db.max_mc_seq_no);

        if (!db) {
            throw new Error(`Database with mc_seq_no ${seq_no} not found`);
        }

        const cursor = await db.db.query(query);
        const block = await cursor.next();

        if (!block) {
            throw new Error(`Block with ${seq_no} not found`);
        }

        return block;
    }

    async get_shardchain_blocks_by_ids(ids: string[], max_gen_utime: number): Promise<ShardChainBlock[]> {
        const query = aql`
            FOR b IN blocks
            FILTER b._key IN ${ids} && b.workchain_id != -1
            RETURN { 
                "id": b._key,
                "chain_order": b.chain_order,
                "gen_utime": b.gen_utime,
                "workchain_id": b.workchain_id,
                "seq_no": b.seq_no,
                "shard": b.shard,
                "prev_block_id": b.prev_ref.root_hash,
                "prev_alt_block_id": b.prev_alt_ref.root_hash,
                "message_ids": (
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
                    RETURN m_id),
                "transactions": (
                    FOR a_b IN b.account_blocks || []
                        FOR t IN a_b.transactions
                        RETURN {
                            "id": t.transaction_id,
                            "lt": t.lt,
                            "account_addr": a_b.account_addr,
                        }),
            }
        `;

        const databases = await this.databases;
        let db_index = databases.findIndex(db => max_gen_utime >= db.min_time && max_gen_utime <= db.max_time);
        db_index = (db_index > -1) ? db_index : databases.length - 1;

        const blocks = [] as ShardChainBlock[];
        while (blocks.length < ids.length) {
            if (db_index < 0) {
                throw new Error(`Blocks not found: ${ids.filter(id => !blocks.find(b => b.id == id))}`)
            }

            const cursor = await databases[db_index].db.query(query);
            const current_blocks = await cursor.all();
            blocks.push(...current_blocks);
            db_index--;
        }
        
        return blocks;
    }

    async get_messages(ids: string[], time: number): Promise<Message[]> {
        const query = aql`
            FOR m IN messages
            FILTER m._key IN ${ids}
            RETURN {
                "id": m._key,
                "created_lt": m.created_lt,
            }
        `;

        const databases = await this.databases;
        const db = databases.find(db => time >= db.min_time && time <= db.max_time);
        
        if (!db) {
            throw new Error(`Database with time ${time} not found`);
        }

        const cursor = await db.db.query(query);
        const messages = await cursor.all();

        if (messages.length != ids.length) {
            throw new Error(`Messages not found: ${ids.filter(id => !messages.find(b => b.id == id))}`);
        }

        return messages;
    }

    async set_message_chain_orders(chain_orders: ChainOrderedMessage[], time: number): Promise<void> {
        const query = aql`
            FOR c_o IN ${chain_orders}
            UPDATE c_o.id
                WITH { "chain_order": c_o.chain_order }
                IN messages
                OPTIONS { waitForSync: true }
        `;

        const databases = await this.databases;
        const db = databases.find(db => time >= db.min_time && time <= db.max_time);
        
        if (!db) {
            throw new Error(`Database with time ${time} not found`);
        }

        await db.db.query(query);
    }

    async set_transaction_chain_orders(chain_orders: ChainOrderedTransaction[], time: number): Promise<void> {
        const query = aql`
            FOR c_o IN ${chain_orders}
            UPDATE c_o.id 
                WITH { "chain_order": c_o.chain_order } 
                IN transactions
                OPTIONS { waitForSync: true }
        `;

        const databases = await this.databases;
        const db = databases.find(db => time >= db.min_time && time <= db.max_time);
        
        if (!db) {
            throw new Error(`Database with time ${time} not found`);
        }

        try {
            await db.db.query(query);
        } catch(err) {
            // Check for "transaction not found" error
            const query = aql`
                FOR t IN transactions
                FILTER t._key IN ${chain_orders.map(c_o => c_o.id)}
                RETURN t._key
            `;
            const cursor = await db.db.query(query);
            const found_ids = await cursor.all();
            if (found_ids.length == chain_orders.length) {
                throw err;
            } else {
                const not_found_ids = chain_orders
                    .filter(c_o => !found_ids.find(id => c_o.id == id))
                    .map(c_o => c_o.id);
                throw new Error(`Transactions not found: ${not_found_ids}`);
            }
        }
    }

    async set_chain_order_for_block(block: Block, chain_order: string) {
        const query = aql`
            FOR b IN blocks
            FILTER b._key == ${block.id}
            UPDATE b._key
                WITH { "chain_order": ${chain_order} } 
                IN blocks
                OPTIONS { waitForSync: true }
        `;

        const databases = await this.databases;
        const db = databases.find(db => block.gen_utime >= db.min_time && block.gen_utime <= db.max_time);
        
        if (!db) {
            throw new Error(`Database with time ${block.gen_utime} not found`);
        }

        await db.db.query(query);
    }

    async update_bmt_summary(chain_ordered_mc_seq_no: number, start_time: number, end_time: number) {
        const databases = await this.databases;
        const databases_to_process = databases.filter(db => 
            db.min_time >= start_time && db.min_time <= end_time ||
            db.max_time >= start_time && db.max_time <= end_time);

        const master_order = toU64String(chain_ordered_mc_seq_no);
        const next_master_order = toU64String(chain_ordered_mc_seq_no + 1);
            

        for (let i = 0; i < databases_to_process.length; i++) {
            const db = databases_to_process[i];
            
            if (db.bmt_summary_exists) {
                const query = aql`
                    UPDATE "summary" 
                        WITH { chain_range: { end: ${next_master_order} } }
                        IN bmt
                        OPTIONS { waitForSync: true }
                `;
                await db.db.query(query);
            } else {
                if (!await db.db.collection("bmt").exists()) {
                    await db.db.createCollection("bmt");
                }

                const query = aql`
                    INSERT {
                        _key: "summary",
                        chain_range: {
                            start: ${master_order},
                            end: ${next_master_order},
                        },
                    } INTO users OPTIONS { waitForSync: true }
                `;
                await db.db.query(query);
            }

            db.last_chain_ordered_mc_seq_no = chain_ordered_mc_seq_no;
        }
    }
}


export type Message = {
    id: string,
    created_lt: string,
};

export type ChainOrderedMessage = {
    id: string,
    chain_order: string,
};

export type Transaction = {
    id: string,
    lt: string,
    account_addr: string,
};

export type ChainOrderedTransaction = {
    id: string,
    chain_order: string,
};

export type Block = {
    id: string,
    chain_order: string | null,
    gen_utime: number,
    seq_no: number,
    message_ids: string[],
    transactions: Transaction[],
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
