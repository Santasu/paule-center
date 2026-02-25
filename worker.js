"use strict";

require("dotenv").config();

const { Worker } = require("bullmq");
const IORedis = require("ioredis");

const {
  sb,
  nowIso,
  decryptJson,
  claimOutbox,
  renderOutbound,
  smtpSend,
  advanceEnrollmentAfterSent,
  imapPollAndStore,
  stopEnrollmentsByEmail
} = require("./shared");

const redisUrl = process.env.REDIS_URL || "";
if (!redisUrl) {
  console.error("Missing REDIS_URL. Add Redis in Railway.");
  process.exit(1);
}

const connection = new IORedis(redisUrl, { maxRetriesPerRequest: null });

const SEND_CONCURRENCY = parseInt(process.env.SEND_CONCURRENCY || "10", 10);
const IMAP_CONCURRENCY = parseInt(process.env.IMAP_CONCURRENCY || "5", 10);

async function loadMailbox(tenantId, mailboxId) {
  const { data, error } = await sb()
    .from("em_mailboxes")
    .select("*")
    .eq("tenant_id", tenantId)
    .eq("id", mailboxId)
    .limit(1);

  if (error || !data || !data[0]) return null;
  return data[0];
}

/* -------- email-send worker -------- */
const sendWorker = new Worker(
  "email-send",
  async (job) => {
    const outboxId = job.data.outbox_id;
    if (!outboxId) return { ok: false, error: "missing outbox_id" };

    // claim row to avoid duplicates
    const claimed = await claimOutbox(outboxId);
    if (!claimed.ok) return { ok: false, skipped: true };

    const outRow = claimed.row;
    const tenantId = outRow.tenant_id;

    // load mailbox
    const mb = await loadMailbox(tenantId, outRow.mailbox_id);
    if (!mb) {
      await sb().from("em_outbox")
        .update({ status: "FAILED", error: "Mailbox not found" })
        .eq("id", outboxId)
        .eq("claim_token", claimed.token);
      return { ok: false, error: "mailbox not found" };
    }

    // decrypt password
    let pass;
    try {
      const secret = decryptJson(process.env.EMAIL_MASTER_KEY, mb.pass_enc);
      pass = secret.password;
    } catch (e) {
      await sb().from("em_outbox")
        .update({ status: "FAILED", error: "Decrypt failed" })
        .eq("id", outboxId)
        .eq("claim_token", claimed.token);
      return { ok: false, error: "decrypt failed" };
    }

    try {
      const { subject, html } = await renderOutbound({ tenantId, mailboxRow: mb, password: pass, outRow });
      const msgId = await smtpSend({ mailboxRow: mb, password: pass }, outRow.to_email, subject, html);

      const sentAt = nowIso();
      await sb().from("em_outbox")
        .update({ status: "SENT", sent_at: sentAt, message_id: msgId, error: null })
        .eq("id", outboxId)
        .eq("claim_token", claimed.token);

      // advance enrollment if exists
      if (outRow.enrollment_id && outRow.step_index) {
        await advanceEnrollmentAfterSent(tenantId, outRow.enrollment_id, outRow.step_index, sentAt);
      }

      // best effort mailbox counter increment (optional)
      await sb().rpc("increment_mailbox_sent_today", { p_mailbox_id: mb.id }).catch(() => {});

      return { ok: true, msgId };
    } catch (e) {
      await sb().from("em_outbox")
        .update({ status: "FAILED", error: String(e.message || e) })
        .eq("id", outboxId)
        .eq("claim_token", claimed.token);
      return { ok: false, error: String(e.message || e) };
    }
  },
  { connection, concurrency: SEND_CONCURRENCY }
);

/* -------- imap-poll worker -------- */
const imapWorker = new Worker(
  "imap-poll",
  async (job) => {
    const tenantId = job.data.tenant_id;
    const mailboxId = job.data.mailbox_id;
    if (!tenantId || !mailboxId) return { ok: false, error: "missing tenant_id/mailbox_id" };

    const mb = await loadMailbox(tenantId, mailboxId);
    if (!mb) return { ok: false, error: "mailbox not found" };

    let pass;
    try {
      const secret = decryptJson(process.env.EMAIL_MASTER_KEY, mb.pass_enc);
      pass = secret.password;
    } catch (e) {
      return { ok: false, error: "decrypt failed" };
    }

    try {
      const p = await imapPollAndStore({ tenantId, mailboxRow: mb, password: pass }, 25);

      // stop enrollments for any reply senders
      for (const fe of (p.fromEmails || [])) {
        await stopEnrollmentsByEmail(tenantId, fe, "REPLIED").catch(() => {});

        // mark last SENT to this sender as REPLIED (best effort)
        const { data: last } = await sb().from("em_outbox")
          .select("id")
          .eq("tenant_id", tenantId)
          .eq("to_email", fe)
          .eq("status", "SENT")
          .order("sent_at", { ascending: false })
          .limit(1);

        if (last && last[0]) {
          await sb().from("em_outbox").update({ status: "REPLIED" }).eq("id", last[0].id).catch(() => {});
        }
      }

      return { ok: true, stored: p.stored || 0 };
    } catch (e) {
      return { ok: false, error: String(e.message || e) };
    }
  },
  { connection, concurrency: IMAP_CONCURRENCY }
);

sendWorker.on("failed", (job, err) => console.error("send failed", job?.id, err?.message));
imapWorker.on("failed", (job, err) => console.error("imap failed", job?.id, err?.message));

console.log("Workers running. send:", SEND_CONCURRENCY, "imap:", IMAP_CONCURRENCY);

/* graceful shutdown */
process.on("SIGTERM", async () => {
  try { await sendWorker.close(); } catch {}
  try { await imapWorker.close(); } catch {}
  try { await connection.quit(); } catch {}
  process.exit(0);
});
