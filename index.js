/**
 * ============================================================
 * CONDENSE PIPELINE - AEROSPACE MANUFACTURING
 * APP: Aerospace Reference Data  (CDC source for the Enrich Utility)
 * ============================================================
 * Role     : Custom Input Connector on Condense
 * Function : Stands in for the plant's master-data systems (PLM / MES / CMMS /
 *            calibration DB). In production a CDC tool (Debezium etc.) would stream
 *            row changes from those databases into Kafka; here we publish the same
 *            shape of records ourselves.
 * Output   : two "CDC" topics, one record per key, latest record wins:
 *              aerospace-machine-master-cdc  key/field: asset_id
 *              aerospace-process-master-cdc  key/field: process_key
 *
 * The Enrich Utility's CDC step requires the join key to be a FIELD IN THE PAYLOAD
 * ("Join Key (CDC): field from CDC payload"), so each record repeats its key as a field.
 *
 * Live demo of change-data-capture:
 *   curl -X POST :3002/master/machine/ACNC_001 -H 'content-type: application/json' \
 *        -d '{"plant_code":"PLT-B7","plant_name":"Relocated Plant B7"}'
 *   → record is merged and re-published immediately → cache updates → next event
 *     for ACNC_001 on the dashboard shows the new plant. No pipeline redeploy.
 * ============================================================
 */

const { Kafka } = require('kafkajs');
const express   = require('express');
const fs        = require('fs');
const path      = require('path');
require('dotenv').config();

const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
const TOPIC_MACHINE = process.env.KAFKA_TOPIC_MACHINE_MASTER || 'aerospace-machine-master-cdc';
const TOPIC_PROCESS = process.env.KAFKA_TOPIC_PROCESS_MASTER || 'aerospace-process-master-cdc';
const REPUBLISH_S   = parseInt(process.env.REPUBLISH_INTERVAL_S || '60');
const PORT          = parseInt(process.env.PORT || '3002');

const data = JSON.parse(fs.readFileSync(path.join(__dirname, 'master-data.json'), 'utf8'));
const store = {
  machine: new Map(data.machine_master.map(r => [r.asset_id, r])),
  process: new Map(data.process_master.map(r => [r.process_key, r])),
};
const TOPICS = { machine: [TOPIC_MACHINE, 'asset_id'], process: [TOPIC_PROCESS, 'process_key'] };

const kafka    = new Kafka({ clientId: 'aerospace-reference-data', brokers: KAFKA_BROKERS, retry: { retries: 5 } });
const producer = kafka.producer();

const toMessage = (kind, rec) => ({ key: String(rec[TOPICS[kind][1]]), value: JSON.stringify({ ...rec, record_updated_at: new Date().toISOString() }) });

async function publishAll() {
  for (const kind of Object.keys(store)) {
    const messages = [...store[kind].values()].map(r => toMessage(kind, r));
    await producer.send({ topic: TOPICS[kind][0], messages });
  }
  console.log(`[Aero-Reference] published ${store.machine.size} machine + ${store.process.size} process records`);
}

const app = express();
app.use(express.json());
app.get('/health', (_, res) => res.json({ status: 'ok', machines: store.machine.size, processes: store.process.size }));
app.get('/master', (_, res) => res.json({ machine_master: [...store.machine.values()], process_master: [...store.process.values()] }));

// Simulated database change — merge fields into one record and emit the new state right away.
for (const kind of Object.keys(store)) {
  app.post(`/master/${kind}/:key`, async (req, res) => {
    const cur = store[kind].get(req.params.key);
    if (!cur) return res.status(404).json({ error: 'unknown key' });
    const idField = TOPICS[kind][1];
    const next = { ...cur, ...req.body, [idField]: cur[idField] }; // join key is immutable
    store[kind].set(req.params.key, next);
    try { await producer.send({ topic: TOPICS[kind][0], messages: [toMessage(kind, next)] }); }
    catch (e) { return res.status(502).json({ error: e.message }); }
    res.json(next);
  });
}

async function start() {
  await producer.connect();
  await publishAll();
  setInterval(() => publishAll().catch(e => console.error('[Aero-Reference] publish error:', e.message)), REPUBLISH_S * 1000);
}
process.on('SIGTERM', async () => { await producer.disconnect(); process.exit(0); });
process.on('SIGINT',  async () => { await producer.disconnect(); process.exit(0); });
app.listen(PORT, () => console.log(`[Aero-Reference] HTTP on :${PORT}`));
start().catch(err => { console.error('[Aero-Reference] Fatal:', err); process.exit(1); });
