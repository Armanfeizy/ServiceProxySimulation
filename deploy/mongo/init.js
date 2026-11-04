// Payload store: variable-shape request/response bodies, one document per trace_id.
const db = db.getSiblingDB("gateway");
db.createCollection("audit_payloads");
// TTL: payloads are bulky and needed for days, not forever.
db.audit_payloads.createIndex({ received_at: 1 }, { expireAfterSeconds: 7 * 24 * 3600 });
db.audit_payloads.createIndex({ tenant_id: 1, received_at: -1 });
