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
                    "transaction_ids": (
                        FOR a_b IN b.account_blocks || []
                            FOR t IN a_b.transactions
                                RETURN t.transaction_id
                    ),
                }
        `);

        return await cursor.all() as Block[];
    }

    async find_existing_transaction_ids(ids: string[]): Promise<string[]> {
        const cursor = await this.arango_db.query(aql`
            FOR t IN transactions
                FILTER t._key IN ${ids}
                RETURN t._key
        `);

        return await cursor.all() as string[];
    }
}

export type Block = {
    id: string,
    chain_order: string | null,
    transaction_ids: string[],
};
