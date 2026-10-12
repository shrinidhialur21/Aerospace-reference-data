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
 * Messages are wrapped in a CDC envelope ({before, after, op, ts_ms}) — see toMessage().
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

// Kafka bootstrap servers: accept the common variable names and say clearly what was found.
const BROKER_VARS = ["KAFKA_BROKERS", "KAFKA_BOOTSTRAP_SERVERS", "KAFKA_BOOTSTRAP_SERVER", "BOOTSTRAP_SERVERS", "BROKERS"];
function resolveBrokers() {
  const found = BROKER_VARS.find((v) => (process.env[v] || "").trim());
  if (found) { console.log(JSON.stringify({ level: "INFO", msg: "Kafka brokers from env", var: found, brokers: process.env[found].trim() })); return process.env[found].split(",").map((s) => s.trim()); }
  console.error(JSON.stringify({ level: "ERROR", msg: "No Kafka broker env var set — falling back to localhost:9092",
    expected_any_of: BROKER_VARS, kafka_like_env_names_present: Object.keys(process.env).filter((k) => /kafka|broker|bootstrap/i.test(k)) }));
  return ["localhost:9092"];
}

const KAFKA_BROKERS = resolveBrokers();
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

// Condense's Enrich Utility expects the CDC topic in change-data-capture envelope form
// (Debezium-style): {"before": <old row|null>, "after": <new row>, "op": ..., "ts_ms": ...}
// The message KEY stays the bare join key (asset_id / process_key) and is what the cache is keyed on.
// CDC_ENVELOPE=false publishes the bare record instead; CDC_PAYLOAD_AS_STRING=true makes
// before/after JSON *strings* rather than nested objects (in case the platform expects that).
const ENVELOPE = process.env.CDC_ENVELOPE !== 'false';
const PAYLOAD_AS_STRING = process.env.CDC_PAYLOAD_AS_STRING === 'true';
const wrap = (rec) => (rec && PAYLOAD_AS_STRING ? JSON.stringify(rec) : rec);

// op: 'r' = snapshot/re-publish of current state, 'u' = update (from the POST endpoints)
const toMessage = (kind, rec, before = null, op = 'r') => ({
  key: String(rec[TOPICS[kind][1]]),
  value: JSON.stringify(ENVELOPE
    ? { before: wrap(before), after: wrap(rec), op, ts_ms: Date.now(), source: { name: 'aerospace-reference-data', table: kind + '_master' } }
    : { ...rec, record_updated_at: new Date().toISOString() }),
});

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
    try { await producer.send({ topic: TOPICS[kind][0], messages: [toMessage(kind, next, cur, 'u')] }); }
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
