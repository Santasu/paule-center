"use strict";

require("dotenv").config();

const express = require("express");
const cors = require("cors");
const { Queue } = require("bullmq");
const IORedis = require("ioredis");

const {
  cleanStr, toInt, toBool, normEmail, nowIso, addDaysIso, safeJsonParse,
  sb,
  encryptJson,
  parseUnsubToken,
  mailboxConnectTest,
  isSuppressed,
  stopEnrollmentsByEmail,
  refreshMailboxCountersIfNeeded,
  ensureSequenceOutboxForMailbox,
  findLeadIdByEmail
} = require("./shared");

const app = express();
app.use(cors({ origin: "*", methods: ["GET","POST","OPTIONS"], allowedHeaders: ["Content-Type","Authorization"] }));
app.use(express.json({ limit: "2mb" }));
app.use(express.urlencoded({ extended: true }));

const PORT = process.env.PORT || 3000;

const redisUrl = process.env.REDIS_URL || "";
const hasRedis = !!redisUrl;
const connection = hasRedis ? new IORedis(redisUrl, { maxRetriesPerRequest: null }) : null;

const sendQueue = hasRedis ? new Queue("email-send", { connection, defaultJobOptions: { removeOnComplete: true, removeOnFail: 500 } }) : null;
const imapQueue = hasRedis ? new Queue("imap-poll", { connection, defaultJobOptions: { removeOnComplete: true, removeOnFail: 500 } }) : null;

function bad(res, msg, extra = {}) { return res.status(400).json({ ok: false, error: msg, ...extra }); }
function ok(res, obj) { return res.json({ ok: true, ...obj }); }

function tenantIdFrom(req) {
  const k = cleanStr(req.query.k ?? req.body.k);
  const tenant_id = cleanStr(req.query.tenant_id ?? req.body.tenant_id);
  return tenant_id || k;
}

function requireTickKey(req) {
  const key = cleanStr(req.query.key ?? req.body.key);
  return key && key === String(process.env.EMAIL_TICK_KEY || "");
}

/* ---------- Tick logic (enqueue jobs) ---------- */
async function tickTenant(tenantId) {
  const now = nowIso();

  const { data: mailboxes, error } = await sb()
    .from("em_mailboxes")
    .select("*")
    .eq("tenant_id", tenantId)
    .eq("is_enabled", true)
    .limit(200);

  if (error) throw new Error("mailboxes fetch failed: " + error.message);

  let genQueuedTotal = 0;
  let sendEnqueued = 0;
  let pollEnqueued = 0;

  for (const mb of (mailboxes || [])) {
    await refreshMailboxCountersIfNeeded(mb);

    // 1) create outbox from due enrollments
    const gen = await ensureSequenceOutboxForMailbox(mb);
    genQueuedTotal += (gen.queued || 0);

    // 2) enqueue send jobs (respect daily + per minute)
    const dailyLimit = Number(mb.daily_limit || 0);
    const sentToday = Number(mb.sent_today || 0);
    const remaining = Math.max(0, dailyLimit - sentToday);
    const perMin = Math.max(0, Number(mb.per_minute_limit || 0));
    const take = Math.min(remaining || 0, perMin || 0);

    if (take > 0) {
      const { data: outRows } = await sb()
        .from("em_outbox")
        .select("id,to_email")
        .eq("tenant_id", mb.tenant_id)
        .eq("mailbox_id", mb.id)
        .eq("status", "QUEUED")
        .lte("due_at", now)
        .order("created_at", { ascending: true })
        .limit(take);

      for (const row of (outRows || [])) {
        if (row.to_email && (await isSuppressed(mb.tenant_id, String(row.to_email).toLowerCase()))) {
          await sb().from("em_outbox").update({ status: "CANCELLED", error: "Suppressed" }).eq("id", row.id);
          continue;
        }
        if (sendQueue) {
          await sendQueue.add("send", { outbox_id: row.id }, { jobId: "send:" + row.id });
          sendEnqueued++;
        }
      }
    }

    // 3) enqueue IMAP poll
    if (imapQueue) {
      await imapQueue.add("poll", { tenant_id: mb.tenant_id, mailbox_id: mb.id }, { jobId: "poll:" + mb.id });
      pollEnqueued++;
    }
  }

  return { tenantId, genQueuedTotal, sendEnqueued, pollEnqueued, mailboxes: (mailboxes || []).length };
}

async function tickAllTenants() {
  const { data, error } = await sb()
    .from("em_mailboxes")
    .select("tenant_id")
    .eq("is_enabled", true)
    .limit(2000);

  if (error) throw new Error("tenant list failed: " + error.message);

  const unique = Array.from(new Set((data || []).map(x => x.tenant_id).filter(Boolean)));
  const out = [];
  for (const t of unique) out.push(await tickTenant(t));
  return out;
}

/* ---------- endpoints ---------- */
app.get("/", (_req, res) => res.type("text/plain").send("paule-email-center (railway) alive"));

app.all("/email_api", async (req, res) => {
  try {
    let op = cleanStr(req.query.op ?? req.body.op) || "health";
    const tenantId = tenantIdFrom(req);

    // aliases from portal variants
    if (op === "quick.send") op = "email.enqueue";
    if (op === "steps.list") op = "steps.get";
    if (op === "steps.save") op = "steps.save.one";

    if (op === "health") {
      return ok(res, { service: "email_api_railway", time: nowIso(), redis: hasRedis ? "on" : "off" });
    }

    // public unsubscribe
    if (op === "email.unsub") {
      const t = cleanStr(req.query.t ?? req.body.t);
      const parsed = t ? parseUnsubToken(process.env.EMAIL_MASTER_KEY, t) : null;
      if (!parsed) {
        res.status(400).type("text/html").send("<html><body>Bad token</body></html>");
        return;
      }
      const { tenantId: tId, email } = parsed;

      await sb().from("em_suppressions").insert([{ tenant_id: tId, email, reason: "unsub" }]).catch(() => {});
      await stopEnrollmentsByEmail(tId, email, "UNSUB").catch(() => {});

      res.type("text/html").send(
        `<html><body style="font-family:system-ui;padding:24px"><h2>✅ Atsisakyta</h2><p>${email}</p></body></html>`
      );
      return;
    }

    if (!tenantId) return bad(res, "Missing k/tenant_id");

    /* =========================
       SUPPRESSIONS
    ========================= */
    if (op === "suppressions.list") {
      const limit = Math.min(toInt(req.query.limit ?? req.body.limit, 200), 1000);
      const q = cleanStr(req.query.q ?? req.body.q);

      let query = sb().from("em_suppressions").select("*").eq("tenant_id", tenantId).order("created_at", { ascending: false }).limit(limit);
      if (q) query = query.ilike("email", `%${q}%`);

      const { data, error } = await query;
      if (error) return bad(res, "List failed", { supabase: error.message });
      return ok(res, { suppressions: data || [] });
    }

    if (op === "suppressions.add") {
      const email = normEmail(req.query.email ?? req.body.email);
      const reason = cleanStr(req.query.reason ?? req.body.reason) || "manual";
      if (!email) return bad(res, "Bad email");

      await sb().from("em_suppressions").insert([{ tenant_id: tenantId, email, reason }]).catch(() => {});
      await stopEnrollmentsByEmail(tenantId, email, "SUPPRESSED").catch(() => {});
      return ok(res, { inserted: true });
    }

    if (op === "suppressions.delete") {
      const id = cleanStr(req.query.id ?? req.body.id);
      if (!id) return bad(res, "Missing id");
      const { error } = await sb().from("em_suppressions").delete().eq("tenant_id", tenantId).eq("id", id);
      if (error) return bad(res, "Delete failed", { supabase: error.message });
      return ok(res, { deleted: true });
    }

    /* =========================
       MAILBOXES
    ========================= */
    if (op === "mailboxes.list") {
      const { data, error } = await sb().from("em_mailboxes").select("*").eq("tenant_id", tenantId).order("created_at", { ascending: false }).limit(500);
      if (error) return bad(res, "List failed", { supabase: error.message });
      return ok(res, { mailboxes: data || [] });
    }

    if (op === "mailboxes.add") {
      const email = normEmail(req.query.email ?? req.body.email);
      const from_name = cleanStr(req.query.from_name ?? req.body.from_name) || null;

      const imap_host = cleanStr(req.query.imap_host ?? req.body.imap_host);
      const imap_port = toInt(req.query.imap_port ?? req.body.imap_port, 993);
      const imap_tls = toBool(req.query.imap_tls ?? req.body.imap_tls, true);

      const smtp_host = cleanStr(req.query.smtp_host ?? req.body.smtp_host);
      const smtp_port = toInt(req.query.smtp_port ?? req.body.smtp_port, 587);
      const smtp_tls = toBool(req.query.smtp_tls ?? req.body.smtp_tls, true);

      const username = cleanStr(req.query.username ?? req.body.username) || email;
      const password = cleanStr(req.query.password ?? req.body.password);

      const daily_limit = toInt(req.query.daily_limit ?? req.body.daily_limit, 30);
      const per_minute_limit = toInt(req.query.per_minute_limit ?? req.body.per_minute_limit, 3);

      if (!email || !imap_host || !smtp_host || !username || !password) {
        return bad(res, "Missing fields (email/imap_host/smtp_host/username/password)");
      }

      const pass_enc = encryptJson(process.env.EMAIL_MASTER_KEY, { password });

      const row = {
        tenant_id: tenantId,
        email, from_name,
        imap_host, imap_port, imap_tls,
        smtp_host, smtp_port, smtp_tls,
        username,
        pass_enc,
        is_enabled: true,
        daily_limit,
        per_minute_limit
      };

      const { data, error } = await sb().from("em_mailboxes").insert([row]).select("*").limit(1);
      if (error) return bad(res, "Insert failed", { supabase: error.message });
      return ok(res, { mailbox: data && data[0] ? data[0] : null });
    }

    if (op === "mailboxes.toggle") {
      const id = cleanStr(req.query.id ?? req.body.id);
      const enabled = toBool(req.query.is_enabled ?? req.body.is_enabled, true);
      if (!id) return bad(res, "Missing id");

      const { data, error } = await sb().from("em_mailboxes")
        .update({ is_enabled: enabled })
        .eq("tenant_id", tenantId)
        .eq("id", id)
        .select("*")
        .limit(1);

      if (error) return bad(res, "Toggle failed", { supabase: error.message });
      return ok(res, { mailbox: data && data[0] ? data[0] : null });
    }

    if (op === "mailboxes.delete") {
      const id = cleanStr(req.query.id ?? req.body.id);
      if (!id) return bad(res, "Missing id");
      const { error } = await sb().from("em_mailboxes").delete().eq("tenant_id", tenantId).eq("id", id);
      if (error) return bad(res, "Delete failed", { supabase: error.message });
      return ok(res, { deleted: true });
    }

    if (op === "mailboxes.test") {
      const id = cleanStr(req.query.id ?? req.body.id);
      if (!id) return bad(res, "Missing id");

      const { data, error } = await sb().from("em_mailboxes").select("*").eq("tenant_id", tenantId).eq("id", id).limit(1);
      if (error || !data || !data[0]) return bad(res, "Mailbox not found");

      const resT = await mailboxConnectTest(data[0]);
      return ok(res, { ...resT });
    }

    /* =========================
       SEQUENCES
    ========================= */
    if (op === "sequences.list") {
      const { data, error } = await sb().from("em_sequences").select("*").eq("tenant_id", tenantId).order("created_at", { ascending: false }).limit(200);
      if (error) return bad(res, "List failed", { supabase: error.message });
      return ok(res, { sequences: data || [] });
    }

    if (op === "sequences.create") {
      const name = cleanStr(req.query.name ?? req.body.name) || "New sequence";
      const mailbox_id = cleanStr(req.query.mailbox_id ?? req.body.mailbox_id) || null;
      const { data, error } = await sb().from("em_sequences").insert([{ tenant_id: tenantId, name, mailbox_id, is_enabled: true }]).select("*").limit(1);
      if (error) return bad(res, "Create failed", { supabase: error.message });
      return ok(res, { sequence: data && data[0] ? data[0] : null });
    }

    if (op === "sequences.delete") {
      const id = cleanStr(req.query.id ?? req.body.id);
      if (!id) return bad(res, "Missing id");
      const { error } = await sb().from("em_sequences").delete().eq("tenant_id", tenantId).eq("id", id);
      if (error) return bad(res, "Delete failed", { supabase: error.message });
      return ok(res, { deleted: true });
    }

    /* =========================
       STEPS
    ========================= */
    if (op === "steps.get") {
      const sequence_id = cleanStr(req.query.sequence_id ?? req.body.sequence_id);
      if (!sequence_id) return bad(res, "Missing sequence_id");
      const { data, error } = await sb().from("em_sequence_steps")
        .select("*")
        .eq("tenant_id", tenantId)
        .eq("sequence_id", sequence_id)
        .order("step_index", { ascending: true });
      if (error) return bad(res, "Get failed", { supabase: error.message });
      return ok(res, { steps: data || [] });
    }

    if (op === "steps.save.one") {
      const sequence_id = cleanStr(req.query.sequence_id ?? req.body.sequence_id);
      const step_index = toInt(req.query.step_index ?? req.body.step_index, 1);
      if (!sequence_id) return bad(res, "Missing sequence_id");

      const payload = {
        tenant_id: tenantId,
        sequence_id,
        step_index,
        delay_days: Math.max(0, toInt(req.query.delay_days ?? req.body.delay_days, 0)),
        is_enabled: true,
        subject_a: String(req.query.subject_a ?? req.body.subject_a ?? "").trim() || "Subject",
        body_html_a: String(req.query.body_html_a ?? req.body.body_html_a ?? "").trim() || "<p>Body</p>",
        subject_b: cleanStr(req.query.subject_b ?? req.body.subject_b) || null,
        body_html_b: cleanStr(req.query.body_html_b ?? req.body.body_html_b) || null,
        ab_percent: Math.max(0, Math.min(100, toInt(req.query.ab_percent ?? req.body.ab_percent, 50))),
      };

      const { data: ex } = await sb().from("em_sequence_steps")
        .select("id")
        .eq("tenant_id", tenantId)
        .eq("sequence_id", sequence_id)
        .eq("step_index", step_index)
        .limit(1);

      if (ex && ex[0]) {
        const { data, error } = await sb().from("em_sequence_steps")
          .update(payload)
          .eq("tenant_id", tenantId)
          .eq("id", ex[0].id)
          .select("*")
          .limit(1);
        if (error) return bad(res, "Update failed", { supabase: error.message });
        return ok(res, { step: data && data[0] ? data[0] : null });
      } else {
        const { data, error } = await sb().from("em_sequence_steps").insert([payload]).select("*").limit(1);
        if (error) return bad(res, "Insert failed", { supabase: error.message });
        return ok(res, { step: data && data[0] ? data[0] : null });
      }
    }

    /* =========================
       INBOX / OUTBOX
    ========================= */
    if (op === "inbox.list") {
      const limit = Math.min(toInt(req.query.limit ?? req.body.limit, 200), 500);
      const { data, error } = await sb().from("em_inbox").select("*").eq("tenant_id", tenantId).order("received_at", { ascending: false }).limit(limit);
      if (error) return bad(res, "List failed", { supabase: error.message });
      return ok(res, { inbox: data || [] });
    }

    if (op === "outbox.list") {
      const limit = Math.min(toInt(req.query.limit ?? req.body.limit, 200), 500);
      const { data, error } = await sb().from("em_outbox").select("*").eq("tenant_id", tenantId).order("created_at", { ascending: false }).limit(limit);
      if (error) return bad(res, "List failed", { supabase: error.message });
      return ok(res, { outbox: data || [] });
    }

    if (op === "outbox.cancel") {
      const id = cleanStr(req.query.id ?? req.body.id);
      if (!id) return bad(res, "Missing id");
      const { data, error } = await sb().from("em_outbox")
        .update({ status: "CANCELLED" })
        .eq("tenant_id", tenantId)
        .eq("id", id)
        .select("*")
        .limit(1);
      if (error) return bad(res, "Cancel failed", { supabase: error.message });
      return ok(res, { row: data && data[0] ? data[0] : null });
    }

    if (op === "outbox.retry") {
      const id = cleanStr(req.query.id ?? req.body.id);
      if (!id) return bad(res, "Missing id");
      const { data, error } = await sb().from("em_outbox")
        .update({ status: "QUEUED", error: null, due_at: nowIso() })
        .eq("tenant_id", tenantId)
        .eq("id", id)
        .eq("status", "FAILED")
        .select("*")
        .limit(1);
      if (error) return bad(res, "Retry failed", { supabase: error.message });
      return ok(res, { row: data && data[0] ? data[0] : null });
    }

    /* =========================
       QUICK SEND -> OUTBOX QUEUE
    ========================= */
    if (op === "email.enqueue") {
      const mailbox_id = cleanStr(req.query.mailbox_id ?? req.body.mailbox_id);
      const to_email = normEmail(req.query.to_email ?? req.body.to_email);
      const subject = cleanStr(req.query.subject ?? req.body.subject);
      const body_html = String(req.query.body_html ?? req.body.body_html ?? "");

      if (!mailbox_id || !to_email || !subject || !body_html) return bad(res, "Missing mailbox_id/to_email/subject/body_html");
      if (await isSuppressed(tenantId, to_email)) return bad(res, "Suppressed");

      const { data, error } = await sb().from("em_outbox").insert([{
        tenant_id: tenantId,
        mailbox_id,
        to_email,
        subject,
        body_html,
        status: "QUEUED",
        due_at: nowIso()
      }]).select("*").limit(1);

      if (error) return bad(res, "Enqueue failed", { supabase: error.message });
      return ok(res, { queued: data && data[0] ? data[0] : null });
    }

    /* =========================
       ENROLL BULK (emails only)
    ========================= */
    if (op === "enroll.bulk") {
      const sequence_id = cleanStr(req.query.sequence_id ?? req.body.sequence_id);
      const mailbox_id = cleanStr(req.query.mailbox_id ?? req.body.mailbox_id);
      const raw = cleanStr(req.query.to_emails ?? req.body.to_emails);
      if (!sequence_id || !mailbox_id || !raw) return bad(res, "Missing sequence_id/mailbox_id/to_emails");

      const { data: step1Arr } = await sb().from("em_sequence_steps")
        .select("delay_days")
        .eq("tenant_id", tenantId)
        .eq("sequence_id", sequence_id)
        .eq("step_index", 1)
        .eq("is_enabled", true)
        .limit(1);

      const step1 = (step1Arr && step1Arr[0]) ? step1Arr[0] : { delay_days: 0 };
      const startDue = addDaysIso(nowIso(), step1.delay_days || 0);

      const list = raw.split(/[\n,;\t ]+/).map(normEmail).filter(Boolean);
      const unique = Array.from(new Set(list));
      if (!unique.length) return bad(res, "No valid emails");

      let rows = [];
      let suppressed = 0;

      for (const to of unique) {
        if (await isSuppressed(tenantId, to)) { suppressed++; continue; }
        const leadId = await findLeadIdByEmail(tenantId, to).catch(() => null);
        rows.push({
          tenant_id: tenantId,
          sequence_id,
          mailbox_id,
          lead_id: leadId,
          to_email: to,
          status: "ACTIVE",
          step_pos: 1,
          next_due_at: startDue
        });
      }

      const { error } = await sb().from("em_enrollments").insert(rows);
      if (error) return bad(res, "Enroll failed", { supabase: error.message });
      return ok(res, { enrolled: rows.length, suppressed });
    }

    /* =========================
       CAMPAIGN ENROLL (leads_json)
    ========================= */
    if (op === "campaign.enroll") {
      const sequence_id = cleanStr(req.query.sequence_id ?? req.body.sequence_id);
      const mailbox_id = cleanStr(req.query.mailbox_id ?? req.body.mailbox_id);
      const leads_json = cleanStr(req.query.leads_json ?? req.body.leads_json);
      if (!sequence_id || !mailbox_id || !leads_json) return bad(res, "Missing sequence_id/mailbox_id/leads_json");

      const leads = safeJsonParse(leads_json);
      if (!Array.isArray(leads) || !leads.length) return bad(res, "Bad leads_json");

      const { data: step1Arr } = await sb().from("em_sequence_steps")
        .select("delay_days")
        .eq("tenant_id", tenantId)
        .eq("sequence_id", sequence_id)
        .eq("step_index", 1)
        .eq("is_enabled", true)
        .limit(1);

      const step1 = (step1Arr && step1Arr[0]) ? step1Arr[0] : { delay_days: 0 };
      const startDue = addDaysIso(nowIso(), step1.delay_days || 0);

      let rows = [];
      let suppressed = 0;

      for (const L of leads) {
        const email = normEmail(L.email);
        if (!email) continue;
        if (await isSuppressed(tenantId, email)) { suppressed++; continue; }

        const vars = {
          first_name: cleanStr(L.first_name) || cleanStr(L.firstname) || "",
          last_name: cleanStr(L.last_name) || cleanStr(L.lastname) || "",
          company: cleanStr(L.company) || cleanStr(L.company_name) || "",
          website: cleanStr(L.website) || cleanStr(L.web) || "",
          phone: cleanStr(L.phone) || cleanStr(L.phone_number) || "",
          domain: cleanStr(L.domain) || "",
          title: cleanStr(L.title) || "",
          industry: cleanStr(L.industry) || "",
          linkedin: cleanStr(L.linkedin) || cleanStr(L.linkedin_url) || ""
        };
        if (!vars.domain && vars.website) vars.domain = String(vars.website).replace(/^https?:\/\//,"").split("/")[0];

        const leadId = cleanStr(L.lead_id) || (await findLeadIdByEmail(tenantId, email).catch(()=>null));

        rows.push({
          tenant_id: tenantId,
          sequence_id,
          mailbox_id,
          lead_id: leadId,
          to_email: email,
          status: "ACTIVE",
          step_pos: 1,
          next_due_at: startDue,
          vars
        });
      }

      const { error } = await sb().from("em_enrollments").insert(rows);
      if (error) return bad(res, "Enroll failed", { supabase: error.message });
      return ok(res, { enrolled: rows.length, suppressed });
    }

    /* =========================
       STATS
    ========================= */
    if (op === "stats") {
      const today = nowIso().slice(0, 10);

      const { data: outRows } = await sb().from("em_outbox").select("status,created_at,sent_at").eq("tenant_id", tenantId).limit(5000);
      const { data: inRows } = await sb().from("em_inbox").select("received_at").eq("tenant_id", tenantId).limit(5000);
      const { data: enRows } = await sb().from("em_enrollments").select("status").eq("tenant_id", tenantId).limit(5000);

      const out = outRows || [];
      const inbox = inRows || [];
      const enr = enRows || [];

      const sentToday = out.filter(x => x.status === "SENT" && String(x.sent_at || x.created_at || "").slice(0,10) === today).length;
      const queued = out.filter(x => x.status === "QUEUED").length;
      const failed = out.filter(x => x.status === "FAILED").length;
      const replied = out.filter(x => x.status === "REPLIED").length;
      const receivedToday = inbox.filter(x => String(x.received_at || "").slice(0,10) === today).length;
      const activeEnroll = enr.filter(x => x.status === "ACTIVE").length;

      return ok(res, { sentToday, queued, failed, replied, receivedToday, activeEnroll });
    }

    /* =========================
       EMAIL.TICK (ENQUEUE)
    ========================= */
    if (op === "email.tick") {
      if (!requireTickKey(req)) return res.status(403).json({ ok:false, error:"Forbidden" });
      if (!hasRedis) return bad(res, "REDIS_URL missing. Add Redis in Railway.");

      const onlyTenant = cleanStr(req.query.tenant_id ?? req.body.tenant_id) || null;

      if (onlyTenant) {
        const one = await tickTenant(onlyTenant);
        return ok(res, { mode: "enqueue", one, time: nowIso() });
      } else {
        const all = await tickAllTenants();
        return ok(res, { mode: "enqueue", all, time: nowIso() });
      }
    }

    return bad(res, "Unknown op");
  } catch (e) {
    return res.status(500).json({ ok:false, error:"Server error", message: String(e.message || e) });
  }
});

app.listen(PORT, () => {
  console.log("email-api listening on", PORT, "redis:", hasRedis ? "on" : "off");
});
