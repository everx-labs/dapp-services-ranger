import os from "os";
import path from "path";
import { BmtDb } from "./bmt-db";
import { Ranger } from "./ranger";
import { RangerConfig } from "./ranger-config";

// CAUTION: block correction via vertical blockchain wasn't considered
// TODO: Add ordered_db switching

(async () => {
    try { 
        const configPath = process.argv.length > 1 ? process.argv[2] : path.resolve(os.homedir(), ".tonlabs", "ranger.config.json");
        RangerConfig.loadFromFile(configPath);

        const buffer_db = new BmtDb(RangerConfig.get_buffer_db_config());
        const ordered_db = new BmtDb(RangerConfig.get_ordered_db_config());
        const ranger = new Ranger(buffer_db, ordered_db);
        await ranger.run();
    } catch (e) {
        console.error(e);
        process.exit(1);
    }
})();

