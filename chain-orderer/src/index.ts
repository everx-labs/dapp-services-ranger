import fs from "fs";
import os from "os";
import path from "path";
import { ChainOrderer } from "./chain-orderer";

// CAUTION: block correction via vertical blockchain wasn't considered

(async () => {
    try { 
        const configPath = process.argv.length > 2 ? process.argv[2] : path.resolve(os.homedir(), ".tonlabs", "chain-orderer.config.json");
        const config = JSON.parse(fs.readFileSync(configPath, "utf8"));

        const chain_orderer = new ChainOrderer(config);
        await chain_orderer.run();
    } catch (e) {
        console.error(e);
        process.exit(1);
    }
})();

