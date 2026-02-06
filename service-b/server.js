const http = require("http");
const os = require("os");
const { URL } = require("url");
const { execFile } = require("child_process");
const dns = require("dns").promises;

const PORT = Number(process.env.PORT || 8080);
const SERVICE_NAME = process.env.RAILWAY_SERVICE_NAME || "service-b";
const SERVICE_A_URL = (process.env.SERVICE_A_URL || "http://service-a.railway.internal:8080").replace(/\/+$/, "");
const SELF_URL = (
  process.env.SELF_URL ||
  `http://${process.env.RAILWAY_PRIVATE_DOMAIN || `${SERVICE_NAME}.railway.internal`}:${PORT}`
).replace(/\/+$/, "");
const SERVICE_A_EXPECTED_NAME = process.env.SERVICE_A_EXPECTED_NAME || "service-a";
const TARGET_REGION_COUNT_DEFAULT = Number(process.env.TARGET_REGION_COUNT || 3);
const MAX_ROUNDS_DEFAULT = Number(process.env.MAX_ROUNDS || 8);
const SAMPLES_PER_ROUND_DEFAULT = Number(process.env.SAMPLES_PER_ROUND || 20);
const SAMPLE_CONCURRENCY_DEFAULT = Number(process.env.SAMPLE_CONCURRENCY || 10);
const REQUEST_TIMEOUT_MS_DEFAULT = Number(process.env.REQUEST_TIMEOUT_MS || 2000);

function json(res, status, payload) {
  res.writeHead(status, { "content-type": "application/json; charset=utf-8" });
  res.end(JSON.stringify(payload, null, 2));
}

function parseIntOrDefault(raw, fallback, min = 1, max = 1000) {
  const parsed = Number(raw);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(min, Math.min(max, Math.floor(parsed)));
}

function safeUrl(url) {
  try {
    return new URL(url);
  } catch {
    return null;
  }
}

function getServiceAHost() {
  const parsed = safeUrl(SERVICE_A_URL);
  return parsed ? parsed.hostname : process.env.SERVICE_A_HOST || "service-a.railway.internal";
}

function getSelfHost() {
  if (process.env.RAILWAY_PRIVATE_DOMAIN) return process.env.RAILWAY_PRIVATE_DOMAIN;
  const parsed = safeUrl(SELF_URL);
  return parsed ? parsed.hostname : `${SERVICE_NAME}.railway.internal`;
}

function identity() {
  return {
    service: SERVICE_NAME,
    hostname: os.hostname(),
    pid: process.pid,
    uptimeSec: Math.round(process.uptime()),
    railway: {
      projectName: process.env.RAILWAY_PROJECT_NAME || null,
      environmentName: process.env.RAILWAY_ENVIRONMENT_NAME || null,
      serviceName: process.env.RAILWAY_SERVICE_NAME || null,
      serviceId: process.env.RAILWAY_SERVICE_ID || null,
      replicaId: process.env.RAILWAY_REPLICA_ID || null,
      replicaRegion: process.env.RAILWAY_REPLICA_REGION || null,
      privateDomain: process.env.RAILWAY_PRIVATE_DOMAIN || null,
      publicDomain: process.env.RAILWAY_PUBLIC_DOMAIN || null,
      deploymentId: process.env.RAILWAY_DEPLOYMENT_ID || null
    },
    now: new Date().toISOString()
  };
}

function dig(host, recordType) {
  return new Promise((resolve) => {
    execFile(
      "dig",
      ["+short", host, recordType],
      { timeout: 5000 },
      (error, stdout, stderr) => {
        if (error) {
          resolve({
            ok: false,
            error: error.message,
            stderr: (stderr || "").trim()
          });
          return;
        }
        const lines = (stdout || "")
          .split("\n")
          .map((v) => v.trim())
          .filter(Boolean);
        resolve({ ok: true, lines });
      }
    );
  });
}

async function dnsSnapshot(host) {
  let lookup = null;
  try {
    lookup = await dns.lookup(host, { all: true, verbatim: true });
  } catch (error) {
    lookup = { error: error.message };
  }
  const [a, aaaa, cname] = await Promise.all([
    dig(host, "A"),
    dig(host, "AAAA"),
    dig(host, "CNAME")
  ]);
  return {
    host,
    lookup,
    dig: { A: a, AAAA: aaaa, CNAME: cname }
  };
}

async function fetchJson(url, timeoutMs) {
  const startedAt = Date.now();
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, {
      method: "GET",
      headers: { accept: "application/json" },
      signal: controller.signal
    });
    const text = await response.text();
    let data = null;
    try {
      data = JSON.parse(text);
    } catch {
      data = { raw: text };
    }
    return {
      ok: response.ok,
      status: response.status,
      elapsedMs: Date.now() - startedAt,
      url,
      data
    };
  } catch (error) {
    return {
      ok: false,
      status: null,
      elapsedMs: Date.now() - startedAt,
      url,
      error: error.message
    };
  } finally {
    clearTimeout(timer);
  }
}

async function sampleUrl(url, count, concurrency, timeoutMs) {
  const safeCount = Math.max(0, count);
  const safeConcurrency = Math.max(1, Math.min(safeCount || 1, concurrency));
  const results = new Array(safeCount);
  let next = 0;

  async function worker() {
    while (true) {
      const current = next++;
      if (current >= safeCount) return;
      results[current] = await fetchJson(url, timeoutMs);
    }
  }

  await Promise.all(Array.from({ length: safeConcurrency }, () => worker()));
  return results;
}

function extractIdentity(payload) {
  if (!payload || typeof payload !== "object") return null;
  return {
    service: payload.service || payload.railway?.serviceName || "unknown",
    region: payload.railway?.replicaRegion || payload.region || "unknown",
    replicaId: payload.railway?.replicaId || payload.replicaId || payload.hostname || "unknown",
    hostname: payload.hostname || null
  };
}

function summarizeResults(results) {
  const identities = [];
  const errors = [];
  let okResponses = 0;

  for (const result of results) {
    if (result && result.ok && result.status >= 200 && result.status < 300) {
      okResponses += 1;
      const id = extractIdentity(result.data);
      if (id) {
        identities.push(id);
      } else {
        errors.push("missing_identity_payload");
      }
      continue;
    }
    if (result && result.error) {
      errors.push(result.error);
    } else if (result && result.status !== null) {
      errors.push(`http_${result.status}`);
    } else {
      errors.push("unknown_error");
    }
  }

  const uniqueErrorCounts = {};
  for (const error of errors) {
    uniqueErrorCounts[error] = (uniqueErrorCounts[error] || 0) + 1;
  }

  return {
    attempted: results.length,
    okResponses,
    failedResponses: results.length - okResponses,
    identities,
    errorCounts: uniqueErrorCounts
  };
}

function aggregateIdentities(identities) {
  const byServiceRegion = new Map();
  const byService = new Map();
  const seenRegions = new Set();

  for (const id of identities) {
    const service = id.service || "unknown";
    const region = id.region || "unknown";
    const replicaId = id.replicaId || "unknown";
    seenRegions.add(region);

    const srKey = `${service}::${region}`;
    if (!byServiceRegion.has(srKey)) {
      byServiceRegion.set(srKey, { service, region, hits: 0, replicas: new Set() });
    }
    const sr = byServiceRegion.get(srKey);
    sr.hits += 1;
    sr.replicas.add(replicaId);

    if (!byService.has(service)) {
      byService.set(service, { service, hits: 0, regions: new Set(), replicas: new Set() });
    }
    const s = byService.get(service);
    s.hits += 1;
    s.regions.add(region);
    s.replicas.add(replicaId);
  }

  return {
    byServiceRegion: Array.from(byServiceRegion.values())
      .map((v) => ({
        service: v.service,
        region: v.region,
        hits: v.hits,
        replicas: Array.from(v.replicas).sort(),
        replicaCount: v.replicas.size
      }))
      .sort((a, b) => `${a.service}:${a.region}`.localeCompare(`${b.service}:${b.region}`)),
    byService: Array.from(byService.values())
      .map((v) => ({
        service: v.service,
        hits: v.hits,
        regions: Array.from(v.regions).sort(),
        regionCount: v.regions.size,
        replicaCount: v.replicas.size
      }))
      .sort((a, b) => a.service.localeCompare(b.service)),
    seenRegions: Array.from(seenRegions).sort()
  };
}

function parseExpectedRegions(searchParams) {
  const fromQuery = (searchParams.get("expectedRegions") || "")
    .split(",")
    .map((v) => v.trim())
    .filter(Boolean);
  if (fromQuery.length > 0) return fromQuery;
  return (process.env.EXPECTED_REGIONS || "")
    .split(",")
    .map((v) => v.trim())
    .filter(Boolean);
}

function buildDigQuery(targets) {
  return targets.map((host) => `host=${encodeURIComponent(host)}`).join("&");
}

async function buildCombinedReport(searchParams) {
  const targetRegionCount = parseIntOrDefault(
    searchParams.get("targetRegions"),
    TARGET_REGION_COUNT_DEFAULT,
    1,
    16
  );
  const maxRounds = parseIntOrDefault(searchParams.get("rounds"), MAX_ROUNDS_DEFAULT, 1, 50);
  const samplesPerRound = parseIntOrDefault(
    searchParams.get("samplesPerRound"),
    SAMPLES_PER_ROUND_DEFAULT,
    1,
    1000
  );
  const sampleConcurrency = parseIntOrDefault(
    searchParams.get("concurrency"),
    SAMPLE_CONCURRENCY_DEFAULT,
    1,
    200
  );
  const timeoutMs = parseIntOrDefault(
    searchParams.get("timeoutMs"),
    REQUEST_TIMEOUT_MS_DEFAULT,
    200,
    20000
  );
  const includeRaw = searchParams.get("includeRaw") === "1";

  const serviceAWhoamiUrl = `${SERVICE_A_URL}/whoami`;
  const selfWhoamiUrl = `${SELF_URL}/whoami`;
  const serviceAHost = getServiceAHost();
  const selfHost = getSelfHost();

  const [localDig, serviceADigFromA] = await Promise.all([
    Promise.all([dnsSnapshot(serviceAHost), dnsSnapshot(selfHost)]),
    fetchJson(`${SERVICE_A_URL}/dig?${buildDigQuery([serviceAHost, selfHost])}`, timeoutMs)
  ]);

  const allSelfResults = [];
  const allServiceAResults = [];
  const roundSummaries = [];

  for (let round = 1; round <= maxRounds; round += 1) {
    const [selfRound, serviceARound] = await Promise.all([
      sampleUrl(selfWhoamiUrl, samplesPerRound, sampleConcurrency, timeoutMs),
      sampleUrl(serviceAWhoamiUrl, samplesPerRound, sampleConcurrency, timeoutMs)
    ]);
    allSelfResults.push(...selfRound);
    allServiceAResults.push(...serviceARound);

    const selfSummary = summarizeResults(allSelfResults);
    const serviceASummary = summarizeResults(allServiceAResults);
    const aggregate = aggregateIdentities([...selfSummary.identities, ...serviceASummary.identities]);

    roundSummaries.push({
      round,
      self: {
        attempted: selfSummary.attempted,
        okResponses: selfSummary.okResponses,
        uniqueReplicas: new Set(selfSummary.identities.map((v) => `${v.region}:${v.replicaId}`)).size,
        seenRegions: Array.from(new Set(selfSummary.identities.map((v) => v.region))).sort()
      },
      serviceA: {
        attempted: serviceASummary.attempted,
        okResponses: serviceASummary.okResponses,
        uniqueReplicas: new Set(serviceASummary.identities.map((v) => `${v.region}:${v.replicaId}`)).size,
        seenRegions: Array.from(new Set(serviceASummary.identities.map((v) => v.region))).sort()
      }
    });

    const selfEntry = aggregate.byService.find((v) => v.service === SERVICE_NAME);
    const serviceAEntry = aggregate.byService.find((v) => v.service === SERVICE_A_EXPECTED_NAME);
    const reachedTargets =
      (selfEntry?.regionCount || 0) >= targetRegionCount &&
      (serviceAEntry?.regionCount || 0) >= targetRegionCount;
    if (reachedTargets) break;
  }

  const selfSummary = summarizeResults(allSelfResults);
  const serviceASummary = summarizeResults(allServiceAResults);
  const aggregate = aggregateIdentities([...selfSummary.identities, ...serviceASummary.identities]);
  const expectedRegions = parseExpectedRegions(searchParams);
  const missingExpectedRegions =
    expectedRegions.length > 0
      ? expectedRegions.filter((region) => !aggregate.seenRegions.includes(region))
      : [];

  const base = {
    generatedAt: new Date().toISOString(),
    local: identity(),
    config: {
      serviceAUrl: SERVICE_A_URL,
      selfUrl: SELF_URL,
      serviceAExpectedName: SERVICE_A_EXPECTED_NAME,
      targetRegionCount,
      maxRounds,
      samplesPerRound,
      sampleConcurrency,
      timeoutMs,
      expectedRegions
    },
    dig: {
      fromServiceB: localDig,
      fromServiceA: serviceADigFromA
    },
    rounds: roundSummaries,
    summaries: {
      self: {
        attempted: selfSummary.attempted,
        okResponses: selfSummary.okResponses,
        failedResponses: selfSummary.failedResponses,
        errorCounts: selfSummary.errorCounts
      },
      serviceA: {
        attempted: serviceASummary.attempted,
        okResponses: serviceASummary.okResponses,
        failedResponses: serviceASummary.failedResponses,
        errorCounts: serviceASummary.errorCounts
      }
    },
    aggregate: {
      byServiceRegion: aggregate.byServiceRegion,
      byService: aggregate.byService,
      seenRegions: aggregate.seenRegions,
      missingExpectedRegions
    },
    discoveredReplicas: {
      self: Array.from(
        new Set(selfSummary.identities.map((v) => `${v.region}:${v.replicaId}`))
      ).sort(),
      serviceA: Array.from(
        new Set(serviceASummary.identities.map((v) => `${v.region}:${v.replicaId}`))
      ).sort()
    },
    note:
      "This endpoint reports regions/replicas observed by internal probes. If a region is missing, increase rounds/samples, verify replica distribution, and verify private DNS targets."
  };

  if (includeRaw) {
    return {
      ...base,
      raw: {
        self: allSelfResults,
        serviceA: allServiceAResults
      }
    };
  }

  return base;
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host || "localhost"}`);

  if (url.pathname === "/health") {
    json(res, 200, { ok: true, service: SERVICE_NAME, now: new Date().toISOString() });
    return;
  }

  if (url.pathname === "/" || url.pathname === "/whoami") {
    json(res, 200, identity());
    return;
  }

  if (url.pathname === "/probe-once") {
    const remote = await fetchJson(`${SERVICE_A_URL}/whoami`, REQUEST_TIMEOUT_MS_DEFAULT);
    json(res, 200, {
      local: identity(),
      remote
    });
    return;
  }

  if (url.pathname === "/dig") {
    const explicitTargets = url.searchParams
      .getAll("host")
      .flatMap((v) => v.split(","))
      .map((v) => v.trim())
      .filter(Boolean);
    const targets = explicitTargets.length > 0 ? explicitTargets : [getServiceAHost(), getSelfHost()];
    const snapshots = [];
    for (const host of targets) {
      snapshots.push(await dnsSnapshot(host));
    }
    json(res, 200, {
      observer: identity(),
      targets: snapshots
    });
    return;
  }

  if (url.pathname === "/combined") {
    const report = await buildCombinedReport(url.searchParams);
    json(res, 200, report);
    return;
  }

  json(res, 404, { error: "not_found", path: url.pathname });
});

server.listen(PORT, () => {
  console.log(
    JSON.stringify({
      message: "service-b listening",
      port: PORT,
      service: SERVICE_NAME,
      replicaId: process.env.RAILWAY_REPLICA_ID || null,
      replicaRegion: process.env.RAILWAY_REPLICA_REGION || null,
      serviceAUrl: SERVICE_A_URL,
      selfUrl: SELF_URL
    })
  );
});
