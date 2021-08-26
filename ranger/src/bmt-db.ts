import { aql, Database } from "arangojs";
import { Config } from "arangojs/connection";

export class BmtDb {
    readonly arango_db: Database;

    constructor(config: Config) {
        this.arango_db = new Database(config);
    }

    async find_blocks_by_ids(ids: string[]): Promise<Block[]> {
        const cursor = await this.arango_db.query(aql`
            FOR b IN blocks
                FILTER b._key IN ${ids}
                RETURN { 
                    "id": b._key,
                    "chain_order": b.chain_order,
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
                            [b.master.recover_create_msg.in_msg.msg_id, b.master.mint_msg.in_msg.msg_id]
                        )
                            FILTER m_id != null
                            RETURN m_id),
                    "transaction_ids": (
                        FOR a_b IN b.account_blocks || []
                            FOR t IN a_b.transactions
                                RETURN t.transaction_id
                    ),
                }
        `);

        return await cursor.all() as Block[];
    }

    async find_existing_mt_ids(messages: string[], transactions: string[]): Promise<{ transactions: string[], messages: string[]}> {
        const cursor = await this.arango_db.query(aql`
            RETURN {
                messages: (
                    FOR m IN messages
                    FILTER m._key IN ${messages}
                    RETURN m._key
                ),
                transactions: (
                    FOR t IN transactions
                    FILTER t._key IN ${transactions}
                    RETURN t._key
                ),
            }
        `);
        const result = await cursor.all() as { transactions: string[], messages: string[]}[];
        return result[0];
    }
}

export type Block = {
    id: string,
    chain_order: string | null,
    message_ids: string[],
    transaction_ids: string[],
};
