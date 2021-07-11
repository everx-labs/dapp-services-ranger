import os from "os";
import path from "path";
import { BmtDb } from "./bmt-db";
import { Ranger } from "./ranger";
import { RangerConfig } from "./ranger-config";

// CAUTION: block correction via vertical blockchain wasn't considered
// TODO: Add ordered_db switching

(async () => {
    try { 
        const configPath = process.argv.length > 2 ? process.argv[2] : path.resolve(os.homedir(), ".tonlabs", "ranger.config.json");
        RangerConfig.loadFromFile(configPath);

        const bmt_db = new BmtDb(RangerConfig.get_db_config());
        const ranger = new Ranger(bmt_db);
        await ranger.run();
    } catch (e) {
        console.error(e);
        process.exit(1);
    }
})();

