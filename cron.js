"use strict";

/**
 * cron.js — Railway cron runner
 * Purpose: call /email_api?op=email.tick and exit
 *
 * ENV:
 * - PUBLIC_BASE_URL (your WEB service URL, no trailing slash)
 * - EMAIL_TICK_KEY
 * - AUTO_TICK_TENANT_ID (optional)
 */

const BASE = (process.env.PUBLIC_BASE_URL || "").replace(/\/+$/, "");
const KEY = process.env.EMAIL_TICK_KEY || "";
const TENANT = process.env.AUTO_TICK_TENANT_ID || "";

async function main() {
  if (!BASE || !KEY) {
    console.error("Missing PUBLIC_BASE_URL or EMAIL_TICK_KEY");
    process.exit(1);
  }

  const url = new URL(BASE + "/email_api");
  url.searchParams.set("op", "email.tick");
  url.searchParams.set("key", KEY);
  if (TENANT) url.searchParams.set("tenant_id", TENANT);

  const r = await fetch(url.toString());
  const t = await r.text();
  console.log("tick status:", r.status);
  console.log(t);
  process.exit(r.ok ? 0 : 2);
}

main().catch((e) => {
  console.error(e);
  process.exit(3);
});
