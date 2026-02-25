"use strict";

const crypto = require("crypto");
const dns = require("dns");
const net = require("net");
const tls = require("tls");
const nodemailer = require("nodemailer");
const { ImapFlow } = require("imapflow");
const { simpleParser } = require("mailparser");
const { createClient } = require("@supabase/supabase-js");

/* ---------- utils ---------- */
function cleanStr(v) { v = String(v ?? "").trim(); return v ? v : null; }
function toInt(v, def) { const n = parseInt(String(v ?? ""), 10); return Number.isFinite(n) ? n : def; }
function toBool(v, def = false) {
  if (v === undefined || v === null || v === "") return def;
  const s = String(v).toLowerCase().trim();
  return ["1","true","yes","y","on"].includes(s);
}
function normEmail(e) {
  e = String(e ?? "").trim().toLowerCase();
  return e.includes("@") ? e : null;
}
function nowIso() { return new Date().toISOString(); }
function addDaysIso(iso, days) {
  const dt = new Date(iso);
  dt.setUTCDate(dt.getUTCDate() + (parseInt(days, 10) || 0));
  return dt.toISOString();
}
function b64url(buf) {
  return Buffer.from(buf).toString("base64").replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/,"");
}
function safeJsonParse(s) { try { return JSON.parse(String(s)); } catch { return null; } }

function withTimeout(promise, ms, label = "timeout") {
  return new Promise((resolve, reject) => {
    const t = setTimeout(() => reject(new Error(label)), ms);
    promise.then(
      (v) => { clearTimeout(t); resolve(v); },
      (e) => { clearTimeout(t); reject(e); }
    );
  });
}

/* ---------- Supabase ---------- */
let _sb = null;
function sb() {
  if (_sb) return _sb;
  const url = process.env.SUPABASE_URL;
  const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
  if (!url || !key) throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
  _sb = createClient(url, key, {
    auth: { persistSession: false },
    global: { headers: { "X-Client-Info": "paule-email-center" } }
  });
  return _sb;
}

/* ---------- AES-256-GCM for pass_enc ---------- */
function getKeyBytes(masterKey) {
  return crypto.createHash("sha256").update(String(masterKey)).digest();
}
function encryptJson(masterKey, obj) {
  const key = getKeyBytes(masterKey);
  const iv = crypto.randomBytes(12);
  const cipher = crypto.createCipheriv("aes-256-gcm", key, iv);
  const pt = Buffer.from(JSON.stringify(obj), "utf8");
  const enc = Buffer.concat([cipher.update(pt), cipher.final()]);
  const tag = cipher.getAuthTag();
  return JSON.stringify({ v: 1, iv: b64url(iv), tag: b64url(tag), data: b64url(enc) });
}
function decryptJson(masterKey, encStr) {
  const pack = JSON.parse(String(encStr));
  const iv = Buffer.from(String(pack.iv).replace(/-/g,"+").replace(/_/g,"/"), "base64");
  const tag = Buffer.from(String(pack.tag).replace(/-/g,"+").replace(/_/g,"/"), "base64");
  const data = Buffer.from(String(pack.data).replace(/-/g,"+").replace(/_/g,"/"), "base64");
  const decipher = crypto.createDecipheriv("aes-256-gcm", getKeyBytes(masterKey), iv);
  decipher.setAuthTag(tag);
  const dec = Buffer.concat([decipher.update(data), decipher.final()]);
  return JSON.parse(dec.toString("utf8"));
}

/* ---------- Unsub token ---------- */
function makeUnsubToken(masterKey, tenantId, email) {
  const base = `${tenantId}|${email}`;
  const sig = crypto.createHmac("sha256", String(masterKey)).update(base).digest("base64url");
  return Buffer.from(`${base}|${sig}`, "utf8").toString("base64url");
}
function parseUnsubToken(masterKey, t) {
  const raw = Buffer.from(String(t || ""), "base64url").toString("utf8");
  const parts = raw.split("|");
  if (parts.length !== 3) return null;
  const [tenantId, email, sig] = parts;
  const base = `${tenantId}|${email}`;
  const check = crypto.createHmac("sha256", String(masterKey)).update(base).digest("base64url");
  if (check !== sig) return null;
  return { tenantId, email };
}
function ensureUnsubFooter(publicBaseUrl, masterKey, tenantId, toEmail, html) {
  const s = String(html || "");
  if (s.includes("op=email.unsub")) return s;
  const token = makeUnsubToken(masterKey, tenantId, toEmail);
  const base = String(publicBaseUrl || "").replace(/\/+$/, "");
  const url = `${base}/email_api?op=email.unsub&t=${encodeURIComponent(token)}`;
  return `${s}
<hr style="border:none;border-top:1px solid #eee;margin:24px 0" />
<div style="font-size:12px;color:#666;line-height:1.4">
  Jei nebenorite gauti laiškų, atsisakykite: <a href="${url}">${url}</a>
</div>`;
}

/* ---------- template + spin ---------- */
function hash01(seed) {
  const h = crypto.createHash("sha256").update(String(seed)).digest();
  return h[0] / 255;
}
function spinSyntax(text, seed) {
  return String(text || "").replace(/\{([^{}]+)\}/g, (m, inner) => {
    const parts = inner.split("|").map(s => s.trim()).filter(Boolean);
    if (!parts.length) return "";
    const r = hash01(seed + "|" + inner);
    const idx = Math.floor(r * parts.length);
    return parts[Math.min(idx, parts.length - 1)];
  });
}
function renderTemplate(text, vars) {
  return String(text || "").replace(/\{\{\s*([a-zA-Z0-9_]+)(\|([^}]*))?\s*\}\}/g, (m, key, _fallbackGroup, fallback) => {
    const v = vars && vars[key] !== undefined && vars[key] !== null ? String(vars[key]).trim() : "";
    if (v) return v;
    return fallback !== undefined ? String(fallback) : "";
  });
}
function buildVars({ mailbox, lead = {}, enrollmentVars = {} }) {
  const email = String(lead.email || enrollmentVars.email || "").toLowerCase();
  const website = lead.website || lead.web || enrollmentVars.website || "";
  const domain =
    (website ? String(website).replace(/^https?:\/\//, "").split("/")[0] : "") ||
    (email.includes("@") ? email.split("@")[1] : "");

  return {
    first_name: lead.first_name || lead.firstname || enrollmentVars.first_name || "",
    last_name: lead.last_name || lead.lastname || enrollmentVars.last_name || "",
    company: lead.company || lead.company_name || enrollmentVars.company || "",
    title: lead.title || enrollmentVars.title || "",
    industry: lead.industry || enrollmentVars.industry || "",
    website: website || "",
    domain: domain || "",
    phone: lead.phone || lead.phone_number || enrollmentVars.phone || "",
    linkedin: lead.linkedin || lead.linkedin_url || enrollmentVars.linkedin || "",
    sender_name: mailbox.from_name || "",
    sender_email: mailbox.email || "",
  };
}

/* ---------- leads optional ---------- */
async function fetchLeadByEmail(tenantId, email) {
  const table = process.env.LEADS_TABLE || "leads";
  const colEmail = process.env.LEADS_EMAIL_COL || "email";
  const colTenant = process.env.LEADS_TENANT_COL || "tenant_id";

  const { data, error } = await sb()
    .from(table)
    .select("*")
    .eq(colTenant, tenantId)
    .eq(colEmail, email)
    .limit(1);

  if (error) return null;
  return (data && data[0]) ? data[0] : null;
}
async function findLeadIdByEmail(tenantId, email) {
  const lead = await fetchLeadByEmail(tenantId, email).catch(() => null);
  return lead && lead.id ? String(lead.id) : null;
}

/* ---------- suppressions ---------- */
async function isSuppressed(tenantId, email) {
  const { data, error } = await sb()
    .from("em_suppressions")
    .select("id")
    .eq("tenant_id", tenantId)
    .eq("email", email)
    .limit(1);
  if (error) return false;
  return !!(data && data[0]);
}
async function stopEnrollmentsByEmail(tenantId, email, reason) {
  await sb()
    .from("em_enrollments")
    .update({ status: "STOPPED", stopped_at: nowIso(), stop_reason: reason })
    .eq("tenant_id", tenantId)
    .eq("to_email", email)
    .eq("status", "ACTIVE");

  await sb()
    .from("em_outbox")
    .update({ status: "CANCELLED", error: `Cancelled: ${reason}` })
    .eq("tenant_id", tenantId)
    .eq("to_email", email)
    .eq("status", "QUEUED");
}

/* ---------- daily counters ---------- */
async function refreshMailboxCountersIfNeeded(mailboxRow) {
  const today = nowIso().slice(0, 10);
  const mbDate = mailboxRow.sent_today_date ? String(mailboxRow.sent_today_date).slice(0, 10) : null;
  if (mbDate !== today) {
    await sb()
      .from("em_mailboxes")
      .update({ sent_today: 0, sent_today_date: today })
      .eq("id", mailboxRow.id);
    mailboxRow.sent_today = 0;
    mailboxRow.sent_today_date = today;
  }
}

/* ---------- connect test (IPv4-first) ---------- */
async function resolveIPv4(host) {
  const r = await withTimeout(dns.promises.lookup(host, { family: 4 }), 2500, "dns timeout");
  return r.address;
}
function tcpConnectFast({ host, port, useTls, timeoutMs }) {
  return new Promise((resolve) => {
    const start = Date.now();
    let sock = null;
    let done = false;

    const finish = (ok, err) => {
      if (done) return;
      done = true;
      try { sock && sock.destroy(); } catch {}
      resolve({ ok: !!ok, err: err ? String(err) : null, ms: Date.now() - start });
    };

    try {
      if (useTls) {
        sock = tls.connect({ host, port, servername: host, rejectUnauthorized: false });
        sock.setTimeout(timeoutMs, () => finish(false, "timeout"));
        sock.on("secureConnect", () => finish(true, null));
        sock.on("error", (e) => finish(false, e.message || e));
      } else {
        sock = net.connect({ host, port });
        sock.setTimeout(timeoutMs, () => finish(false, "timeout"));
        sock.on("connect", () => finish(true, null));
        sock.on("error", (e) => finish(false, e.message || e));
      }
    } catch (e) {
      finish(false, e.message || e);
    }
  });
}
async function mailboxConnectTest(mailboxRow) {
  const timeoutMs = toInt(process.env.CONNECT_TEST_TIMEOUT_MS, 8000);

  let smtp_ok = false, smtp_err = null, smtp_ms = null;
  let imap_ok = false, imap_err = null, imap_ms = null;

  try { await resolveIPv4(mailboxRow.smtp_host); } catch (e) { smtp_err = e.message || String(e); }
  try { await resolveIPv4(mailboxRow.imap_host); } catch (e) { imap_err = e.message || String(e); }

  try {
    const useTls = !!mailboxRow.smtp_tls && Number(mailboxRow.smtp_port) === 465;
    const rS = await tcpConnectFast({ host: mailboxRow.smtp_host, port: Number(mailboxRow.smtp_port), useTls, timeoutMs });
    smtp_ok = rS.ok; smtp_err = rS.err; smtp_ms = rS.ms;
  } catch (e) {
    smtp_ok = false; smtp_err = String(e.message || e);
  }

  try {
    const useTls = !!mailboxRow.imap_tls && Number(mailboxRow.imap_port) === 993;
    const rI = await tcpConnectFast({ host: mailboxRow.imap_host, port: Number(mailboxRow.imap_port), useTls, timeoutMs });
    imap_ok = rI.ok; imap_err = rI.err; imap_ms = rI.ms;
  } catch (e) {
    imap_ok = false; imap_err = String(e.message || e);
  }

  return { smtp_ok, smtp_err, smtp_ms, imap_ok, imap_err, imap_ms };
}

/* ---------- SMTP send ---------- */
async function smtpSend({ mailboxRow, password }, to, subject, html) {
  const secure = mailboxRow.smtp_tls ? (Number(mailboxRow.smtp_port) === 465) : false;

  const transporter = nodemailer.createTransport({
    host: mailboxRow.smtp_host,
    port: Number(mailboxRow.smtp_port),
    secure,
    auth: { user: mailboxRow.username, pass: password },
    tls: mailboxRow.smtp_tls ? { rejectUnauthorized: false } : undefined,
    connectionTimeout: toInt(process.env.SMTP_TIMEOUT_MS, 12000),
    greetingTimeout: toInt(process.env.SMTP_TIMEOUT_MS, 12000),
    socketTimeout: toInt(process.env.SMTP_TIMEOUT_MS, 12000),
  });

  try {
    const info = await transporter.sendMail({
      from: mailboxRow.from_name ? `"${mailboxRow.from_name}" <${mailboxRow.email}>` : mailboxRow.email,
      to,
      subject,
      html,
    });
    return info?.messageId || null;
  } finally {
    try { transporter.close(); } catch {}
  }
}

/* ---------- IMAP poll ---------- */
async function imapPollAndStore({ tenantId, mailboxRow, password }, maxPerTick = 25) {
  const client = new ImapFlow({
    host: mailboxRow.imap_host,
    port: Number(mailboxRow.imap_port),
    secure: !!mailboxRow.imap_tls,
    auth: { user: mailboxRow.username, pass: password },
    logger: false,
    socketTimeout: toInt(process.env.IMAP_TIMEOUT_MS, 12000),
  });

  let maxUid = mailboxRow.last_imap_uid || 0;
  let stored = 0;
  const fromSet = new Set();

  try {
    await client.connect();
    await client.mailboxOpen("INBOX");

    let uids = [];
    if (!maxUid) {
      uids = await client.search({ seen: false });
      uids = (uids || []).slice(-maxPerTick);
    } else {
      const start = maxUid + 1;
      for await (const msg of client.fetch({ uid: `${start}:*` }, { uid: true, source: true })) {
        if (msg?.uid) uids.push(msg.uid);
        if (uids.length >= maxPerTick) break;
      }
    }

    for (const uid of uids) {
      for await (const msg of client.fetch({ uid: `${uid}:${uid}` }, { uid: true, source: true })) {
        const parsed = await simpleParser(msg.source);

        const fromEmail = parsed?.from?.value?.[0]?.address
          ? String(parsed.from.value[0].address).toLowerCase()
          : null;

        const toEmail = parsed?.to?.value?.[0]?.address
          ? String(parsed.to.value[0].address).toLowerCase()
          : mailboxRow.email;

        if (!fromEmail) continue;
        fromSet.add(fromEmail);

        const row = {
          tenant_id: tenantId,
          mailbox_id: mailboxRow.id,
          from_email: fromEmail,
          to_email: toEmail || mailboxRow.email,
          subject: parsed.subject || null,
          body_text: parsed.text || null,
          body_html: typeof parsed.html === "string" ? parsed.html : null,
          received_at: parsed.date ? new Date(parsed.date).toISOString() : nowIso(),
          imap_uid: uid,
          message_id: parsed.messageId || null,
          in_reply_to: parsed.inReplyTo || null,
        };

        const { error } = await sb().from("em_inbox").insert([row]);
        if (!error) stored += 1;

        if (uid > maxUid) maxUid = uid;
        try { await client.messageFlagsAdd({ uid: `${uid}:${uid}` }, ["\\Seen"]); } catch {}
      }
    }
  } finally {
    try { await client.logout(); } catch {}
  }

  if (maxUid && maxUid !== (mailboxRow.last_imap_uid || 0)) {
    await sb().from("em_mailboxes").update({ last_imap_uid: maxUid }).eq("id", mailboxRow.id);
  }

  return { stored, last_uid: maxUid, fromEmails: Array.from(fromSet) };
}

/* ---------- A/B pick ---------- */
function pickVariant(enrollId, stepIndex, abPercent) {
  const p = Math.max(0, Math.min(100, parseInt(abPercent ?? 50, 10) || 50));
  const h = crypto.createHash("sha256").update(`${enrollId}:${stepIndex}`).digest();
  const n = h[0];
  const pct = Math.floor((n / 255) * 100);
  return pct < p ? "B" : "A";
}

/* ---------- render outbound ---------- */
async function renderOutbound({ tenantId, mailboxRow, password, outRow }) {
  let lead = null;
  if (outRow.to_email) {
    lead = await fetchLeadByEmail(tenantId, String(outRow.to_email).toLowerCase()).catch(() => null);
  }

  let enrollmentVars = {};
  if (outRow.enrollment_id) {
    const { data } = await sb()
      .from("em_enrollments")
      .select("vars")
      .eq("id", outRow.enrollment_id)
      .limit(1);
    if (data && data[0] && data[0].vars) enrollmentVars = data[0].vars;
  }

  const vars = buildVars({
    mailbox: { ...mailboxRow },
    lead: lead || { email: outRow.to_email },
    enrollmentVars
  });

  const seed = `${outRow.id}:${outRow.to_email}:${outRow.step_index || ""}:${outRow.variant || ""}`;

  let subject = spinSyntax(outRow.subject, seed);
  subject = renderTemplate(subject, vars);

  let html = spinSyntax(outRow.body_html, seed);
  html = renderTemplate(html, vars);

  html = ensureUnsubFooter(process.env.PUBLIC_BASE_URL, process.env.EMAIL_MASTER_KEY, tenantId, outRow.to_email, html);

  return { subject, html };
}

/* ---------- generate outbox from enrollments ---------- */
async function ensureSequenceOutboxForMailbox(mailboxRow) {
  const tenantId = mailboxRow.tenant_id;
  const now = nowIso();

  const { data: sequences } = await sb()
    .from("em_sequences")
    .select("id")
    .eq("tenant_id", tenantId)
    .eq("mailbox_id", mailboxRow.id)
    .eq("is_enabled", true);

  if (!sequences || !sequences.length) return { queued: 0, checked: 0 };

  const { data: enrollments } = await sb()
    .from("em_enrollments")
    .select("*")
    .eq("tenant_id", tenantId)
    .eq("mailbox_id", mailboxRow.id)
    .eq("status", "ACTIVE")
    .lte("next_due_at", now);

  if (!enrollments || !enrollments.length) return { queued: 0, checked: 0 };

  let queued = 0;

  const seqIds = new Set(sequences.map(s => s.id));
  const bySeq = {};
  for (const e of enrollments) {
    if (!seqIds.has(e.sequence_id)) continue;
    bySeq[e.sequence_id] = bySeq[e.sequence_id] || [];
    bySeq[e.sequence_id].push(e);
  }

  for (const seqId of Object.keys(bySeq)) {
    const { data: steps } = await sb()
      .from("em_sequence_steps")
      .select("*")
      .eq("tenant_id", tenantId)
      .eq("sequence_id", seqId)
      .eq("is_enabled", true)
      .order("step_index", { ascending: true });

    const stepMap = new Map((steps || []).map(s => [Number(s.step_index), s]));

    for (const en of bySeq[seqId]) {
      const to = normEmail(en.to_email);
      if (!to) continue;

      if (await isSuppressed(tenantId, to)) {
        await sb().from("em_enrollments")
          .update({ status: "STOPPED", stopped_at: now, stop_reason: "SUPPRESSED" })
          .eq("id", en.id);
        continue;
      }

      const step = stepMap.get(Number(en.step_pos));
      if (!step) {
        await sb().from("em_enrollments")
          .update({ status: "DONE", stopped_at: now, stop_reason: "COMPLETED" })
          .eq("id", en.id);
        continue;
      }

      const hasB = !!(step.subject_b && step.body_html_b);
      const variant = hasB ? pickVariant(en.id, en.step_pos, step.ab_percent) : "A";
      const subjectRaw = (variant === "B" ? step.subject_b : step.subject_a) || step.subject_a;
      const bodyRaw = (variant === "B" ? step.body_html_b : step.body_html_a) || step.body_html_a;

      const row = {
        tenant_id: tenantId,
        mailbox_id: en.mailbox_id,
        lead_id: en.lead_id || null,
        to_email: to,
        subject: subjectRaw,
        body_html: bodyRaw,
        status: "QUEUED",
        due_at: en.next_due_at || now,
        enrollment_id: en.id,
        step_index: en.step_pos,
        variant
      };

      const { error } = await sb().from("em_outbox").insert([row]);
      if (!error) queued += 1;
    }
  }

  return { queued, checked: enrollments.length };
}

/* ---------- advance enrollment after sent ---------- */
async function advanceEnrollmentAfterSent(tenantId, enrollmentId, stepIndex, sentAtIso) {
  const { data: enr } = await sb().from("em_enrollments").select("*").eq("id", enrollmentId).limit(1);
  const en = (enr && enr[0]) ? enr[0] : null;
  if (!en) return;
  if (String(en.status) !== "ACTIVE") return;
  if (Number(en.step_pos) !== Number(stepIndex)) return;

  const nextStepIndex = Number(stepIndex) + 1;

  const { data: nextStepArr } = await sb()
    .from("em_sequence_steps")
    .select("delay_days")
    .eq("tenant_id", tenantId)
    .eq("sequence_id", en.sequence_id)
    .eq("step_index", nextStepIndex)
    .eq("is_enabled", true)
    .limit(1);

  const nextStep = (nextStepArr && nextStepArr[0]) ? nextStepArr[0] : null;

  if (!nextStep) {
    await sb().from("em_enrollments").update({
      status: "DONE",
      last_sent_at: sentAtIso,
      stopped_at: sentAtIso,
      stop_reason: "COMPLETED"
    }).eq("id", enrollmentId);
    return;
  }

  const nextDue = addDaysIso(sentAtIso, nextStep.delay_days || 0);
  await sb().from("em_enrollments").update({
    step_pos: nextStepIndex,
    next_due_at: nextDue,
    last_sent_at: sentAtIso
  }).eq("id", enrollmentId);
}

/* ---------- claim outbox ---------- */
async function claimOutbox(outboxId) {
  const token = crypto.randomBytes(12).toString("hex");

  const { data, error } = await sb()
    .from("em_outbox")
    .update({ status: "SENDING", claim_token: token, claimed_at: nowIso() })
    .eq("id", outboxId)
    .eq("status", "QUEUED")
    .select("*")
    .limit(1);

  if (error || !data || !data[0]) return { ok: false, token: null, row: null };
  return { ok: true, token, row: data[0] };
}

module.exports = {
  cleanStr, toInt, toBool, normEmail, nowIso, addDaysIso, safeJsonParse, withTimeout,
  sb,
  encryptJson, decryptJson,
  makeUnsubToken, parseUnsubToken,
  mailboxConnectTest,
  smtpSend,
  imapPollAndStore,
  isSuppressed,
  stopEnrollmentsByEmail,
  refreshMailboxCountersIfNeeded,
  ensureSequenceOutboxForMailbox,
  renderOutbound,
  claimOutbox,
  advanceEnrollmentAfterSent,
  findLeadIdByEmail
};
