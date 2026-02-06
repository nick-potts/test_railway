const http = require("http");
const os = require("os");
const { URL, URLSearchParams } = require("url");
const { execFile } = require("child_process");
const dns = require("dns").promises;

const PORT = Number(process.env.PORT || 8080);
const SERVICE_NAME = process.env.RAILWAY_SERVICE_NAME || "service-b";
const SERVICE_A_URL = (process.env.SERVICE_A_URL || "http://service-a.railway.internal:8080").replace(/\/+$/, "");
const SELF_URL = (
  process.env.SELF_URL ||
  `http://${process.env.RAILWAY_PRIVATE_DOMAIN || `${SERVICE_NAME}.railway.internal`}:${PORT}`
).replace(/\/+$/, "");

const REQUEST_TIMEOUT_MS_DEFAULT = Number(process.env.REQUEST_TIMEOUT_MS || 3000);
const HOSTNAME_SAMPLES_DEFAULT = Number(process.env.HOSTNAME_SAMPLES || 12);
const SOURCE_CONCURRENCY_DEFAULT = Number(process.env.SOURCE_CONCURRENCY || 10);
const FANOUT_CONCURRENCY_DEFAULT = Number(process.env.FANOUT_CONCURRENCY || 10);

function json(res, status, payload) {
  res.writeHead(status, { "content-type": "application/json; charset=utf-8" });
  res.end(JSON.stringify(payload, null, 2));
}

function parseIntOrDefault(raw, fallback, min = 1, max = 1000) {
  if (raw === null || raw === undefined) return fallback;
  if (typeof raw === "string" && raw.trim() === "") return fallback;
  const parsed = Number(raw);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(min, Math.min(max, Math.floor(parsed)));
}

function parseRegionList(raw) {
  return (raw || "")
    .split(",")
    .map((v) => v.trim())
    .filter(Boolean);
}

function safeUrl(url) {
  try {
    return new URL(url);
  } catch {
    return null;
  }
}

function getHost(url, fallback) {
  const parsed = safeUrl(url);
  return parsed ? parsed.hostname : fallback;
}

function getPort(url, fallbackPort) {
  const parsed = safeUrl(url);
  if (!parsed) return fallbackPort;
  if (parsed.port) return Number(parsed.port);
  return parsed.protocol === "https:" ? 443 : 80;
}

function getServiceAHost() {
  return getHost(SERVICE_A_URL, "service-a.railway.internal");
}

function getServiceAPort() {
  return getPort(SERVICE_A_URL, 8080);
}

function getSelfHost() {
  if (process.env.RAILWAY_PRIVATE_DOMAIN) return process.env.RAILWAY_PRIVATE_DOMAIN;
  return getHost(SELF_URL, `${SERVICE_NAME}.railway.internal`);
}

function getSelfPort() {
  return getPort(SELF_URL, PORT);
}

function parseExpectedSourceRegions(searchParams) {
  const fromQuery = parseRegionList(searchParams.get("expectedSourceRegions"));
  if (fromQuery.length > 0) return fromQuery;
  return parseRegionList(process.env.EXPECTED_SOURCE_REGIONS);
}

function parseExpectedDestRegions(searchParams) {
  const fromQuerySpecific = parseRegionList(searchParams.get("expectedDestRegions"));
  if (fromQuerySpecific.length > 0) return fromQuerySpecific;
  const fromQueryLegacy = parseRegionList(searchParams.get("expectedRegions"));
  if (fromQueryLegacy.length > 0) return fromQueryLegacy;
  return parseRegionList(process.env.EXPECTED_DEST_REGIONS || process.env.EXPECTED_REGIONS);
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

function extractIdentity(payload) {
  if (!payload || typeof payload !== "object") return null;
  return {
    service: payload.service || payload.railway?.serviceName || "unknown",
    region: payload.railway?.replicaRegion || payload.region || "unknown",
    replicaId: payload.railway?.replicaId || payload.replicaId || payload.hostname || "unknown",
    hostname: payload.hostname || null
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

function isIpv4(value) {
  return /^\d{1,3}(\.\d{1,3}){3}$/.test(value);
}

async function resolveARecords(host) {
  try {
    const records = await dns.resolve4(host);
    return Array.from(new Set(records.filter(isIpv4))).sort();
  } catch {
    const a = await dig(host, "A");
    if (!a.ok) return [];
    return Array.from(new Set(a.lines.filter(isIpv4))).sort();
  }
}

async function fetchJson(url, timeoutMs, extraHeaders = {}) {
  const startedAt = Date.now();
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, {
      method: "GET",
      headers: {
        accept: "application/json",
        connection: "close",
        ...extraHeaders
      },
      signal: controller.signal
    });
    const text = await response.text();
    let data;
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

async function mapWithConcurrency(items, concurrency, handler) {
  const safeConcurrency = Math.max(1, Math.min(items.length || 1, concurrency));
  const output = new Array(items.length);
  let next = 0;

  async function worker() {
    while (true) {
      const index = next++;
      if (index >= items.length) return;
      output[index] = await handler(items[index], index);
    }
  }

  await Promise.all(Array.from({ length: safeConcurrency }, () => worker()));
  return output;
}

async function sampleUrl(url, count, concurrency, timeoutMs, headers = {}) {
  const safeCount = Math.max(0, count);
  const indexes = Array.from({ length: safeCount }, (_, i) => i);
  return mapWithConcurrency(indexes, concurrency, async () => fetchJson(url, timeoutMs, headers));
}

function summarizeIdentities(identities) {
  const byRegion = new Map();
  const byReplica = new Map();
  const regions = new Set();
  const replicas = new Set();

  for (const item of identities) {
    const region = item.region || "unknown";
    const replicaId = item.replicaId || "unknown";
    const service = item.service || "unknown";
    regions.add(region);
    replicas.add(`${region}:${replicaId}`);

    if (!byRegion.has(region)) byRegion.set(region, 0);
    byRegion.set(region, byRegion.get(region) + 1);

    const replicaKey = `${service}:${region}:${replicaId}`;
    if (!byReplica.has(replicaKey)) {
      byReplica.set(replicaKey, { service, region, replicaId, hits: 0 });
    }
    byReplica.get(replicaKey).hits += 1;
  }

  return {
    regions: Array.from(regions).sort(),
    replicas: Array.from(replicas).sort(),
    byRegion: Array.from(byRegion.entries())
      .map(([region, hits]) => ({ region, hits }))
      .sort((a, b) => a.region.localeCompare(b.region)),
    byReplica: Array.from(byReplica.values()).sort((a, b) =>
      `${a.service}:${a.region}:${a.replicaId}`.localeCompare(`${b.service}:${b.region}:${b.replicaId}`)
    )
  };
}

function summarizeHttpResults(results) {
  const identities = [];
  const errorCounts = {};

  for (const result of results) {
    if (result && result.ok && result.status >= 200 && result.status < 300) {
      const id = extractIdentity(result.data);
      if (id) {
        identities.push(id);
      } else {
        errorCounts.missing_identity_payload = (errorCounts.missing_identity_payload || 0) + 1;
      }
      continue;
    }
    const key = result?.error || (result?.status !== null ? `http_${result.status}` : "unknown_error");
    errorCounts[key] = (errorCounts[key] || 0) + 1;
  }

  return {
    attempted: results.length,
    okResponses: identities.length,
    failedResponses: results.length - identities.length,
    errorCounts,
    identities,
    identitySummary: summarizeIdentities(identities)
  };
}

function buildStickiness(identitySummary, attempted) {
  const rows = Array.isArray(identitySummary?.byReplica) ? identitySummary.byReplica : [];
  const uniqueDestReplicas = rows.length;
  const uniqueDestRegions = Array.isArray(identitySummary?.regions) ? identitySummary.regions.length : 0;
  const totalHits = rows.reduce((sum, row) => sum + (row.hits || 0), 0);
  const top = rows.slice().sort((a, b) => (b.hits || 0) - (a.hits || 0))[0] || null;
  const topReplicaHitPct = totalHits > 0 && top ? Number(((top.hits / totalHits) * 100).toFixed(2)) : 0;

  return {
    attempted,
    observedHits: totalHits,
    uniqueDestReplicas,
    uniqueDestRegions,
    topReplica: top
      ? { replicaId: top.replicaId, region: top.region, hits: top.hits }
      : null,
    topReplicaHitPct,
    allHitsToSingleReplica: uniqueDestReplicas === 1 && totalHits > 0
  };
}

async function buildLocalProbeReport(searchParams) {
  const timeoutMs = parseIntOrDefault(
    searchParams.get("timeoutMs"),
    REQUEST_TIMEOUT_MS_DEFAULT,
    200,
    60000
  );
  const dnsSamples = parseIntOrDefault(
    searchParams.get("dnsSamples") || searchParams.get("hostnameSamples"),
    HOSTNAME_SAMPLES_DEFAULT,
    1,
    500
  );
  const sourceConcurrency = parseIntOrDefault(
    searchParams.get("sourceConcurrency"),
    SOURCE_CONCURRENCY_DEFAULT,
    1,
    100
  );

  const serviceAHost = getServiceAHost();
  const serviceAPort = getServiceAPort();
  const selfHost = getSelfHost();
  const selfPort = getSelfPort();

  const [serviceAIps, selfIps, serviceADns, selfDns, serviceADigFromServiceA] = await Promise.all([
    resolveARecords(serviceAHost),
    resolveARecords(selfHost),
    dnsSnapshot(serviceAHost),
    dnsSnapshot(selfHost),
    fetchJson(
      `${SERVICE_A_URL}/dig?host=${encodeURIComponent(serviceAHost)}&host=${encodeURIComponent(selfHost)}`,
      timeoutMs
    )
  ]);

  const dnsResults = await sampleUrl(
    `${SERVICE_A_URL}/whoami`,
    dnsSamples,
    Math.min(sourceConcurrency, dnsSamples),
    timeoutMs
  );
  const dnsSummary = summarizeHttpResults(dnsResults);

  return {
    generatedAt: new Date().toISOString(),
    local: identity(),
    config: {
      serviceAUrl: SERVICE_A_URL,
      selfUrl: SELF_URL,
      timeoutMs,
      dnsSamples,
      sourceConcurrency
    },
    discovery: {
      serviceA: {
        host: serviceAHost,
        port: serviceAPort,
        ips: serviceAIps
      },
      self: {
        host: selfHost,
        port: selfPort,
        ips: selfIps
      }
    },
    dig: {
      serviceA: serviceADns,
      self: selfDns,
      fromServiceA: serviceADigFromServiceA
    },
    dnsProbe: {
      attempted: dnsSummary.attempted,
      okResponses: dnsSummary.okResponses,
      failedResponses: dnsSummary.failedResponses,
      errorCounts: dnsSummary.errorCounts,
      identitySummary: dnsSummary.identitySummary,
      stickiness: buildStickiness(dnsSummary.identitySummary, dnsSummary.attempted)
    }
  };
}

function aggregateSourceReports(sourceResults) {
  const sourceRegions = new Set();
  const sourceReplicas = new Set();
  const dnsDestRegions = new Set();
  const dnsDestReplicas = new Set();
  const dnsMatrix = new Map();
  const expectedDestIps = new Set();
  const stickinessBySource = [];
  const errors = {};

  for (const source of sourceResults) {
    if (!(source.response && source.response.ok && source.response.status >= 200 && source.response.status < 300)) {
      const key =
        source.response?.error ||
        (source.response?.status !== null ? `http_${source.response?.status}` : "unknown_error");
      errors[key] = (errors[key] || 0) + 1;
      continue;
    }

    const report = source.response.data;
    const sourceId = extractIdentity(report?.local);
    if (!sourceId) {
      errors.missing_source_identity = (errors.missing_source_identity || 0) + 1;
      continue;
    }

    sourceRegions.add(sourceId.region);
    sourceReplicas.add(`${sourceId.region}:${sourceId.replicaId}`);

    const sourceExpectedIps = Array.isArray(report?.discovery?.serviceA?.ips)
      ? report.discovery.serviceA.ips
      : [];
    for (const ip of sourceExpectedIps) expectedDestIps.add(ip);

    const dnsByReplica = Array.isArray(report?.dnsProbe?.identitySummary?.byReplica)
      ? report.dnsProbe.identitySummary.byReplica
      : [];

    for (const item of dnsByReplica) {
      const edge = `${sourceId.region}=>${item.region}`;
      dnsDestRegions.add(item.region);
      dnsDestReplicas.add(`${item.region}:${item.replicaId}`);

      if (!dnsMatrix.has(edge)) {
        dnsMatrix.set(edge, {
          sourceRegion: sourceId.region,
          destRegion: item.region,
          hits: 0,
          sourceReplicas: new Set(),
          destReplicas: new Set()
        });
      }
      const edgeItem = dnsMatrix.get(edge);
      edgeItem.hits += item.hits;
      edgeItem.sourceReplicas.add(sourceId.replicaId);
      edgeItem.destReplicas.add(item.replicaId);
    }

    stickinessBySource.push({
      sourceReplica: `${sourceId.region}:${sourceId.replicaId}`,
      sourceRegion: sourceId.region,
      ...report?.dnsProbe?.stickiness
    });
  }

  return {
    sourceRegions: Array.from(sourceRegions).sort(),
    sourceReplicas: Array.from(sourceReplicas).sort(),
    dnsDestRegions: Array.from(dnsDestRegions).sort(),
    dnsDestReplicas: Array.from(dnsDestReplicas).sort(),
    dnsMatrix: Array.from(dnsMatrix.values())
      .map((v) => ({
        sourceRegion: v.sourceRegion,
        destRegion: v.destRegion,
        hits: v.hits,
        sourceReplicaCount: v.sourceReplicas.size,
        destReplicaCount: v.destReplicas.size,
        sourceReplicas: Array.from(v.sourceReplicas).sort(),
        destReplicas: Array.from(v.destReplicas).sort()
      }))
      .sort((a, b) => `${a.sourceRegion}:${a.destRegion}`.localeCompare(`${b.sourceRegion}:${b.destRegion}`)),
    expectedDestIps: Array.from(expectedDestIps).sort(),
    stickinessBySource: stickinessBySource.sort((a, b) =>
      (a.sourceReplica || "").localeCompare(b.sourceReplica || "")
    ),
    errorCounts: errors
  };
}

function compactSourceReport(item) {
  const report = item.response?.data;
  return {
    sourceIp: item.sourceIp,
    ok: Boolean(item.response?.ok),
    status: item.response?.status ?? null,
    elapsedMs: item.response?.elapsedMs ?? null,
    local: report?.local || null,
    serviceA: {
      resolvedIps: report?.discovery?.serviceA?.ips || [],
      hostnameRegions: report?.hostnameProbe?.identitySummary?.regions || [],
      hostnameReplicas: report?.hostnameProbe?.identitySummary?.replicas || [],
      directRegions: report?.directIpProbe?.identitySummary?.regions || [],
      directReplicas: report?.directIpProbe?.identitySummary?.replicas || []
    },
    errors: item.response?.ok ? null : item.response?.error || item.response?.status || "unknown_error"
  };
}

async function buildCombinedReport(searchParams) {
  const timeoutMs = parseIntOrDefault(
    searchParams.get("timeoutMs"),
    REQUEST_TIMEOUT_MS_DEFAULT,
    200,
    60000
  );
  const samplesPerIp = parseIntOrDefault(
    searchParams.get("samplesPerIp"),
    SAMPLES_PER_IP_DEFAULT,
    1,
    50
  );
  const hostnameSamples = parseIntOrDefault(
    searchParams.get("hostnameSamples"),
    HOSTNAME_SAMPLES_DEFAULT,
    1,
    500
  );
  const sourceConcurrency = parseIntOrDefault(
    searchParams.get("sourceConcurrency"),
    SOURCE_CONCURRENCY_DEFAULT,
    1,
    100
  );
  const fanoutConcurrency = parseIntOrDefault(
    searchParams.get("fanoutConcurrency"),
    FANOUT_CONCURRENCY_DEFAULT,
    1,
    100
  );
  const perSourceTimeoutMs = parseIntOrDefault(
    searchParams.get("perSourceTimeoutMs"),
    Math.max(8000, timeoutMs * (samplesPerIp + hostnameSamples + 4)),
    1000,
    120000
  );
  const includeRaw = searchParams.get("includeRaw") === "1";

  const expectedSourceRegions = parseExpectedSourceRegions(searchParams);
  const expectedDestRegions = parseExpectedDestRegions(searchParams);

  const serviceAHost = getServiceAHost();
  const serviceAPort = getServiceAPort();
  const selfHost = getSelfHost();
  const selfPort = getSelfPort();

  const [sourceIps, localDig] = await Promise.all([
    resolveARecords(selfHost),
    Promise.all([dnsSnapshot(serviceAHost), dnsSnapshot(selfHost)])
  ]);

  const params = new URLSearchParams({
    timeoutMs: String(timeoutMs),
    samplesPerIp: String(samplesPerIp),
    hostnameSamples: String(hostnameSamples),
    sourceConcurrency: String(sourceConcurrency)
  }).toString();

  const sourceResults = await mapWithConcurrency(sourceIps, fanoutConcurrency, async (sourceIp) => {
    const url = `http://${sourceIp}:${selfPort}/probe-direct?${params}`;
    const response = await fetchJson(url, perSourceTimeoutMs, { host: selfHost });
    return { sourceIp, response };
  });

  const aggregate = aggregateSourceReports(sourceResults);

  const missingExpectedSourceRegions =
    expectedSourceRegions.length > 0
      ? expectedSourceRegions.filter((v) => !aggregate.sourceRegions.includes(v))
      : [];

  const missingExpectedDestRegionsDirect =
    expectedDestRegions.length > 0
      ? expectedDestRegions.filter((v) => !aggregate.directDestRegions.includes(v))
      : [];

  const missingExpectedDestRegionsHostname =
    expectedDestRegions.length > 0
      ? expectedDestRegions.filter((v) => !aggregate.hostnameDestRegions.includes(v))
      : [];

  const base = {
    generatedAt: new Date().toISOString(),
    local: identity(),
    config: {
      serviceAUrl: SERVICE_A_URL,
      selfUrl: SELF_URL,
      timeoutMs,
      samplesPerIp,
      hostnameSamples,
      sourceConcurrency,
      fanoutConcurrency,
      perSourceTimeoutMs,
      expectedSourceRegions,
      expectedDestRegions
    },
    discovery: {
      sourceService: {
        host: selfHost,
        port: selfPort,
        resolvedIps: sourceIps
      },
      destService: {
        host: serviceAHost,
        port: serviceAPort
      }
    },
    dig: {
      fromServiceB: localDig
    },
    fanout: {
      attemptedSourceIps: sourceIps.length,
      sourceIpsWithResponse: sourceResults.filter((v) => v.response?.ok).length,
      sourceIpsWithError: sourceResults.filter((v) => !v.response?.ok).length
    },
    bridge: {
      sourceRegions: aggregate.sourceRegions,
      sourceReplicas: aggregate.sourceReplicas,
      directIpDestRegions: aggregate.directDestRegions,
      directIpDestReplicas: aggregate.directDestReplicas,
      hostnameDestRegions: aggregate.hostnameDestRegions,
      hostnameDestReplicas: aggregate.hostnameDestReplicas,
      directIpMatrix: aggregate.directMatrix,
      hostnameMatrix: aggregate.hostnameMatrix,
      errorCounts: aggregate.errorCounts
    },
    expectations: {
      expectedSourceRegions,
      expectedDestRegions,
      missingExpectedSourceRegions,
      missingExpectedDestRegionsDirect,
      missingExpectedDestRegionsHostname
    },
    note:
      "directIpMatrix shows source replica region -> destination replica region using explicit destination IPs. hostnameMatrix shows routing when calling service-a by railway.internal hostname."
  };

  if (includeRaw) {
    return {
      ...base,
      raw: sourceResults
    };
  }

  return {
    ...base,
    sourceReports: sourceResults.map(compactSourceReport)
  };
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

  if (url.pathname === "/probe-direct") {
    const report = await buildLocalProbeReport(url.searchParams);
    json(res, 200, report);
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
