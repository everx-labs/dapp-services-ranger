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
            min_chain_order: "0",
            max_chain_order: "g",
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
                min_chain_order: (
                    FOR b IN blocks
                        FILTER b.chain_order
                        SORT b.chain_order ASC
                        LIMIT 1
                        RETURN b.chain_order)[0],
                max_chain_order: (
                    FOR b IN blocks
                        FILTER b.chain_order
                        SORT b.chain_order DESC
                        LIMIT 1
                        RETURN b.chain_order)[0],
            }
        `);
        this.summary = await cursor.next() as DbSummary;
        if (!this.summary.min_chain_order || !this.summary.max_chain_order) {
            throw new Error(`Invalid database ${this.arango_db.name}: there is no blocks`);
        }

        this.summary.max_chain_order = `${this.summary.max_chain_order}g`;
    }

    async get_transactions(chain_order_lt: string, max_count: number): Promise<Transaction[]> {
        const cursor = await this.arango_db.query(aql`
            FOR t IN transactions
            FILTER t.chain_order < ${chain_order_lt}
            SORT t.chain_order DESC
            LIMIT ${max_count}
            RETURN {
                chain_order: t.chain_order,
                in_msg: t.in_msg,
                out_msgs: t.out_msgs,
            }
        `);
        const transactions = await cursor.all() as Transaction[];
        return transactions;
    }

    async set_messages_chain_orders(
        messages_src: SrcChainOrderedEntity[],
        messages_dst: DstChainOrderedEntity[],
    ) : Promise<{ messages_src: string[], messages_dst: string[] }> {
        const src_cursor = await this.arango_db.query(aql`
            FOR c_o IN ${messages_src}
            UPDATE c_o.id 
            WITH { "src_chain_order": c_o.src_chain_order } 
            IN messages
            OPTIONS { ignoreErrors: true }
            RETURN OLD._key
        `);
        const src_result = await src_cursor.all() as string[];
        const dst_cursor = await this.arango_db.query(aql`
            FOR c_o IN ${messages_dst}
            UPDATE c_o.id 
            WITH { "dst_chain_order": c_o.dst_chain_order } 
            IN messages
            OPTIONS { ignoreErrors: true }
            RETURN OLD._key
        `);
        const dst_result = await dst_cursor.all() as string[];
        return {
            messages_src: src_result,
            messages_dst: dst_result,
        };
    }

    async ensure_messages_chain_order_indexes(): Promise<void> {
        const messages = this.arango_db.collection("messages");
        await messages.ensureIndex({ type: "persistent", fields: ['dst_chain_order']});
        await messages.ensureIndex({ type: "persistent", fields: ['src_chain_order']});
        await messages.ensureIndex({ type: "persistent", fields: ['msg_type', 'dst_chain_order']});
        await messages.ensureIndex({ type: "persistent", fields: ['msg_type', 'src_chain_order']});
        await messages.ensureIndex({ type: "persistent", fields: ['dst', 'msg_type', 'dst_chain_order']});
        await messages.ensureIndex({ type: "persistent", fields: ['dst', 'msg_type', 'src', 'dst_chain_order']});
        await messages.ensureIndex({ type: "persistent", fields: ['src', 'src_chain_order']});
        await messages.ensureIndex({ type: "persistent", fields: ['src', 'msg_type', 'src_chain_order']});
        await messages.ensureIndex({ type: "persistent", fields: ['src', 'msg_type', 'dst', 'src_chain_order']});
    }
}

export type DbSummary = {
    min_time: number,
    max_time: number,
    min_chain_order: string,
    max_chain_order: string,
}

export type Transaction = {
    chain_order: string | null,
    in_msg: string | null,
    out_msgs: string[] | null,
};

export type DstChainOrderedEntity = {
    id: string,
    dst_chain_order: string,
};

export type SrcChainOrderedEntity = {
    id: string,
    src_chain_order: string,
};

