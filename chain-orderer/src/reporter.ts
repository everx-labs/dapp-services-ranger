const REPORT_PERIOD_MS = 1000;

export class Reporter {
    started_at = Date.now()
    last_report = Date.now();

    report_step(mc_seq_no: number): void {
        if (Date.now() - this.last_report >= REPORT_PERIOD_MS) {
            this.last_report = Date.now();
            console.log(`${this.last_report - this.started_at} ms, mc_seq_no: ${mc_seq_no}`);
        }
    }

    report_finish(mc_seq_no: number): void {
        console.log(`Finished at seq_no ${mc_seq_no}`);
    }
}
