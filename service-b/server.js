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

function html(res, status, body) {
  res.writeHead(status, { "content-type": "text/html; charset=utf-8" });
  res.end(body);
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

function percentile(sortedValues, pct) {
  if (!Array.isArray(sortedValues) || sortedValues.length === 0) return null;
  if (sortedValues.length === 1) return sortedValues[0];
  const clamped = Math.max(0, Math.min(100, pct));
  const position = (clamped / 100) * (sortedValues.length - 1);
  const lower = Math.floor(position);
  const upper = Math.ceil(position);
  if (lower === upper) return sortedValues[lower];
  const weight = position - lower;
  return sortedValues[lower] * (1 - weight) + sortedValues[upper] * weight;
}

function buildLatencyStats(samples) {
  const values = samples
    .map((v) => Number(v))
    .filter((v) => Number.isFinite(v) && v >= 0)
    .sort((a, b) => a - b);

  if (values.length === 0) {
    return {
      count: 0,
      minMs: null,
      maxMs: null,
      avgMs: null,
      p50Ms: null,
      p95Ms: null
    };
  }

  const sum = values.reduce((acc, v) => acc + v, 0);
  return {
    count: values.length,
    minMs: Number(values[0].toFixed(2)),
    maxMs: Number(values[values.length - 1].toFixed(2)),
    avgMs: Number((sum / values.length).toFixed(2)),
    p50Ms: Number(percentile(values, 50).toFixed(2)),
    p95Ms: Number(percentile(values, 95).toFixed(2))
  };
}

function summarizeHttpResults(results) {
  const identities = [];
  const errorCounts = {};
  const successLatencies = [];
  const latenciesByReplica = new Map();
  const latenciesByRegion = new Map();

  for (const result of results) {
    if (result && result.ok && result.status >= 200 && result.status < 300) {
      const id = extractIdentity(result.data);
      if (id) {
        identities.push(id);
        const elapsed = Number(result.elapsedMs);
        if (Number.isFinite(elapsed) && elapsed >= 0) {
          successLatencies.push(elapsed);
          const replicaKey = `${id.service}:${id.region}:${id.replicaId}`;
          if (!latenciesByReplica.has(replicaKey)) {
            latenciesByReplica.set(replicaKey, {
              service: id.service,
              region: id.region,
              replicaId: id.replicaId,
              samples: []
            });
          }
          latenciesByReplica.get(replicaKey).samples.push(elapsed);

          const regionKey = `${id.service}:${id.region}`;
          if (!latenciesByRegion.has(regionKey)) {
            latenciesByRegion.set(regionKey, {
              service: id.service,
              region: id.region,
              samples: []
            });
          }
          latenciesByRegion.get(regionKey).samples.push(elapsed);
        }
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
    identitySummary: summarizeIdentities(identities),
    latencySummary: {
      overall: buildLatencyStats(successLatencies),
      byReplica: Array.from(latenciesByReplica.values())
        .map((entry) => ({
          service: entry.service,
          region: entry.region,
          replicaId: entry.replicaId,
          ...buildLatencyStats(entry.samples)
        }))
        .sort((a, b) =>
          `${a.service}:${a.region}:${a.replicaId}`.localeCompare(
            `${b.service}:${b.region}:${b.replicaId}`
          )
        ),
      byRegion: Array.from(latenciesByRegion.values())
        .map((entry) => ({
          service: entry.service,
          region: entry.region,
          ...buildLatencyStats(entry.samples)
        }))
        .sort((a, b) => `${a.service}:${a.region}`.localeCompare(`${b.service}:${b.region}`))
    }
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
      latencySummary: dnsSummary.latencySummary,
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
  const regionLatencyMatrix = new Map();
  const replicaLatencyLinks = [];
  const expectedDestIps = new Set();
  const stickinessBySource = [];
  const errors = {};
  let totalLatencyCount = 0;
  let totalLatencyWeightedSum = 0;
  let minLatencyOverall = Number.POSITIVE_INFINITY;
  let maxLatencyOverall = 0;

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
    const dnsLatencyByReplica = Array.isArray(report?.dnsProbe?.latencySummary?.byReplica)
      ? report.dnsProbe.latencySummary.byReplica
      : [];
    const latencyByReplicaKey = new Map(
      dnsLatencyByReplica.map((entry) => [`${entry.service}:${entry.region}:${entry.replicaId}`, entry])
    );

    for (const item of dnsByReplica) {
      const edge = `${sourceId.region}=>${item.region}`;
      dnsDestRegions.add(item.region);
      dnsDestReplicas.add(`${item.region}:${item.replicaId}`);
      const latency = latencyByReplicaKey.get(`${item.service}:${item.region}:${item.replicaId}`) || null;

      if (!dnsMatrix.has(edge)) {
        dnsMatrix.set(edge, {
          sourceRegion: sourceId.region,
          destRegion: item.region,
          hits: 0,
          latencyWeightedSum: 0,
          latencyCount: 0,
          minLatencyMs: Number.POSITIVE_INFINITY,
          maxLatencyMs: 0,
          sourceReplicas: new Set(),
          destReplicas: new Set()
        });
      }
      const edgeItem = dnsMatrix.get(edge);
      edgeItem.hits += item.hits;
      edgeItem.sourceReplicas.add(sourceId.replicaId);
      edgeItem.destReplicas.add(item.replicaId);
      if (latency && Number.isFinite(latency.avgMs) && Number.isFinite(latency.count) && latency.count > 0) {
        edgeItem.latencyWeightedSum += latency.avgMs * latency.count;
        edgeItem.latencyCount += latency.count;
        if (Number.isFinite(latency.minMs)) {
          edgeItem.minLatencyMs = Math.min(edgeItem.minLatencyMs, latency.minMs);
          minLatencyOverall = Math.min(minLatencyOverall, latency.minMs);
        }
        if (Number.isFinite(latency.maxMs)) {
          edgeItem.maxLatencyMs = Math.max(edgeItem.maxLatencyMs, latency.maxMs);
          maxLatencyOverall = Math.max(maxLatencyOverall, latency.maxMs);
        }
        totalLatencyWeightedSum += latency.avgMs * latency.count;
        totalLatencyCount += latency.count;
      }

      replicaLatencyLinks.push({
        sourceService: sourceId.service,
        sourceRegion: sourceId.region,
        sourceReplicaId: sourceId.replicaId,
        destService: item.service,
        destRegion: item.region,
        destReplicaId: item.replicaId,
        hits: item.hits,
        latency: latency
          ? {
              count: latency.count,
              minMs: latency.minMs,
              maxMs: latency.maxMs,
              avgMs: latency.avgMs,
              p50Ms: latency.p50Ms,
              p95Ms: latency.p95Ms
            }
          : null
      });

      const regionLatencyKey = `${sourceId.region}=>${item.region}`;
      if (!regionLatencyMatrix.has(regionLatencyKey)) {
        regionLatencyMatrix.set(regionLatencyKey, {
          sourceRegion: sourceId.region,
          destRegion: item.region,
          hits: 0,
          latencyWeightedSum: 0,
          latencyCount: 0,
          minLatencyMs: Number.POSITIVE_INFINITY,
          maxLatencyMs: 0,
          sourceReplicas: new Set(),
          destReplicas: new Set()
        });
      }
      const regionLatency = regionLatencyMatrix.get(regionLatencyKey);
      regionLatency.hits += item.hits;
      regionLatency.sourceReplicas.add(sourceId.replicaId);
      regionLatency.destReplicas.add(item.replicaId);
      if (latency && Number.isFinite(latency.avgMs) && Number.isFinite(latency.count) && latency.count > 0) {
        regionLatency.latencyWeightedSum += latency.avgMs * latency.count;
        regionLatency.latencyCount += latency.count;
        if (Number.isFinite(latency.minMs)) {
          regionLatency.minLatencyMs = Math.min(regionLatency.minLatencyMs, latency.minMs);
        }
        if (Number.isFinite(latency.maxMs)) {
          regionLatency.maxLatencyMs = Math.max(regionLatency.maxLatencyMs, latency.maxMs);
        }
      }
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
        avgLatencyMs:
          v.latencyCount > 0 ? Number((v.latencyWeightedSum / v.latencyCount).toFixed(2)) : null,
        minLatencyMs:
          Number.isFinite(v.minLatencyMs) && v.minLatencyMs !== Number.POSITIVE_INFINITY
            ? Number(v.minLatencyMs.toFixed(2))
            : null,
        maxLatencyMs: Number.isFinite(v.maxLatencyMs) && v.maxLatencyMs > 0 ? Number(v.maxLatencyMs.toFixed(2)) : null,
        sourceReplicaCount: v.sourceReplicas.size,
        destReplicaCount: v.destReplicas.size,
        sourceReplicas: Array.from(v.sourceReplicas).sort(),
        destReplicas: Array.from(v.destReplicas).sort()
      }))
      .sort((a, b) => `${a.sourceRegion}:${a.destRegion}`.localeCompare(`${b.sourceRegion}:${b.destRegion}`)),
    regionLatencyMatrix: Array.from(regionLatencyMatrix.values())
      .map((v) => ({
        sourceRegion: v.sourceRegion,
        destRegion: v.destRegion,
        hits: v.hits,
        avgLatencyMs:
          v.latencyCount > 0 ? Number((v.latencyWeightedSum / v.latencyCount).toFixed(2)) : null,
        minLatencyMs:
          Number.isFinite(v.minLatencyMs) && v.minLatencyMs !== Number.POSITIVE_INFINITY
            ? Number(v.minLatencyMs.toFixed(2))
            : null,
        maxLatencyMs: Number.isFinite(v.maxLatencyMs) && v.maxLatencyMs > 0 ? Number(v.maxLatencyMs.toFixed(2)) : null,
        sourceReplicaCount: v.sourceReplicas.size,
        destReplicaCount: v.destReplicas.size,
        sourceReplicas: Array.from(v.sourceReplicas).sort(),
        destReplicas: Array.from(v.destReplicas).sort()
      }))
      .sort((a, b) => `${a.sourceRegion}:${a.destRegion}`.localeCompare(`${b.sourceRegion}:${b.destRegion}`)),
    replicaLatencyLinks: replicaLatencyLinks.sort((a, b) =>
      `${a.sourceRegion}:${a.sourceReplicaId}:${a.destRegion}:${a.destReplicaId}`.localeCompare(
        `${b.sourceRegion}:${b.sourceReplicaId}:${b.destRegion}:${b.destReplicaId}`
      )
    ),
    latencyOverall: {
      count: totalLatencyCount,
      avgLatencyMs:
        totalLatencyCount > 0 ? Number((totalLatencyWeightedSum / totalLatencyCount).toFixed(2)) : null,
      minLatencyMs:
        Number.isFinite(minLatencyOverall) && minLatencyOverall !== Number.POSITIVE_INFINITY
          ? Number(minLatencyOverall.toFixed(2))
          : null,
      maxLatencyMs:
        Number.isFinite(maxLatencyOverall) && maxLatencyOverall > 0 ? Number(maxLatencyOverall.toFixed(2)) : null
    },
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
      dnsRegions: report?.dnsProbe?.identitySummary?.regions || [],
      dnsReplicas: report?.dnsProbe?.identitySummary?.replicas || [],
      stickiness: report?.dnsProbe?.stickiness || null
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
  const fanoutConcurrency = parseIntOrDefault(
    searchParams.get("fanoutConcurrency"),
    FANOUT_CONCURRENCY_DEFAULT,
    1,
    100
  );
  const perSourceTimeoutMs = parseIntOrDefault(
    searchParams.get("perSourceTimeoutMs"),
    Math.max(8000, timeoutMs * (dnsSamples + 4)),
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
    dnsSamples: String(dnsSamples),
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

  const missingExpectedDestRegionsDns =
    expectedDestRegions.length > 0
      ? expectedDestRegions.filter((v) => !aggregate.dnsDestRegions.includes(v))
      : [];

  const base = {
    generatedAt: new Date().toISOString(),
    local: identity(),
    config: {
      serviceAUrl: SERVICE_A_URL,
      selfUrl: SELF_URL,
      timeoutMs,
      dnsSamples,
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
      dnsDestRegions: aggregate.dnsDestRegions,
      dnsDestReplicas: aggregate.dnsDestReplicas,
      dnsMatrix: aggregate.dnsMatrix,
      regionLatencyMatrix: aggregate.regionLatencyMatrix,
      replicaLatencyLinks: aggregate.replicaLatencyLinks,
      latencyOverall: aggregate.latencyOverall,
      expectedDestIps: aggregate.expectedDestIps,
      stickinessBySource: aggregate.stickinessBySource,
      errorCounts: aggregate.errorCounts
    },
    expectations: {
      expectedSourceRegions,
      expectedDestRegions,
      missingExpectedSourceRegions,
      missingExpectedDestRegionsDns
    },
    note:
      "Source fanout uses service-b replica IPs. Destination routing is measured via DNS hostname calls to service-a.railway.internal only."
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

function buildMapHtml() {
  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Railway Replica Latency Map</title>
  <style>
    :root {
      --bg: #08111f;
      --bg2: #10243b;
      --surface: rgba(9, 19, 33, 0.72);
      --surface-border: rgba(142, 175, 204, 0.2);
      --text: #d9ebff;
      --muted: #8eacc9;
      --source: #4dd2ff;
      --dest: #ffd166;
      --good: #20c997;
      --warn: #ffb347;
      --bad: #ff6b57;
    }

    * { box-sizing: border-box; }

    body {
      margin: 0;
      color: var(--text);
      background:
        radial-gradient(circle at 12% 8%, rgba(32, 201, 151, 0.16), transparent 34%),
        radial-gradient(circle at 85% 5%, rgba(77, 210, 255, 0.12), transparent 36%),
        linear-gradient(145deg, var(--bg), var(--bg2) 54%, #07101b);
      font-family: "Avenir Next", "Trebuchet MS", "Segoe UI", sans-serif;
      min-height: 100vh;
    }

    .page {
      width: min(1400px, 96vw);
      margin: 20px auto;
      display: grid;
      gap: 14px;
    }

    .hero {
      background: var(--surface);
      border: 1px solid var(--surface-border);
      border-radius: 16px;
      padding: 14px 18px;
      backdrop-filter: blur(8px);
    }

    .hero h1 {
      margin: 0 0 6px 0;
      font-size: clamp(1.05rem, 1.8vw, 1.5rem);
      letter-spacing: 0.04em;
      text-transform: uppercase;
    }

    .hero p {
      margin: 0;
      color: var(--muted);
      font-size: 0.9rem;
      line-height: 1.35;
    }

    .layout {
      display: grid;
      grid-template-columns: 1.7fr 1fr;
      gap: 14px;
    }

    .panel {
      background: var(--surface);
      border: 1px solid var(--surface-border);
      border-radius: 16px;
      overflow: hidden;
      backdrop-filter: blur(10px);
    }

    .map-wrap {
      padding: 0;
      position: relative;
      min-height: 640px;
    }

    #map {
      width: 100%;
      height: 100%;
      display: block;
      background:
        radial-gradient(circle at 35% 28%, rgba(89, 178, 255, 0.08), transparent 45%),
        radial-gradient(circle at 74% 78%, rgba(32, 201, 151, 0.08), transparent 50%),
        linear-gradient(180deg, rgba(4, 9, 17, 0.92), rgba(10, 20, 35, 0.94));
    }

    .status {
      position: absolute;
      top: 10px;
      left: 12px;
      padding: 6px 10px;
      border-radius: 999px;
      font-size: 0.78rem;
      color: #eaf5ff;
      border: 1px solid rgba(183, 210, 233, 0.25);
      background: rgba(5, 14, 26, 0.75);
      z-index: 10;
    }

    .legend {
      position: absolute;
      right: 12px;
      bottom: 12px;
      background: rgba(5, 14, 26, 0.82);
      border: 1px solid rgba(183, 210, 233, 0.25);
      border-radius: 10px;
      padding: 8px 10px;
      font-size: 0.78rem;
      color: var(--muted);
      display: grid;
      gap: 6px;
      z-index: 10;
    }

    .legend-row {
      display: flex;
      align-items: center;
      gap: 6px;
      white-space: nowrap;
    }

    .swatch {
      width: 22px;
      height: 3px;
      border-radius: 999px;
      flex: 0 0 auto;
    }

    .info {
      padding: 14px;
      display: grid;
      gap: 12px;
    }

    .card {
      border: 1px solid rgba(152, 184, 210, 0.16);
      border-radius: 12px;
      padding: 10px 12px;
      background: rgba(8, 18, 32, 0.72);
    }

    .card h2 {
      margin: 0 0 8px 0;
      font-size: 0.9rem;
      text-transform: uppercase;
      letter-spacing: 0.06em;
      color: #e8f4ff;
    }

    .kv {
      display: grid;
      grid-template-columns: 1fr auto;
      gap: 6px 10px;
      font-size: 0.84rem;
      color: var(--muted);
    }

    .kv b {
      color: #ecf7ff;
      font-weight: 600;
    }

    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 0.78rem;
      color: var(--muted);
    }

    th, td {
      padding: 6px 4px;
      border-bottom: 1px solid rgba(157, 190, 217, 0.12);
      text-align: left;
      vertical-align: top;
    }

    th {
      color: #e8f4ff;
      font-size: 0.73rem;
      text-transform: uppercase;
      letter-spacing: 0.06em;
      font-weight: 600;
      position: sticky;
      top: 0;
      background: rgba(8, 18, 32, 0.92);
      z-index: 2;
    }

    .table-scroll {
      max-height: 360px;
      overflow: auto;
      border-radius: 8px;
    }

    .small {
      color: var(--muted);
      font-size: 0.72rem;
      line-height: 1.35;
    }

    .mono {
      font-family: "Menlo", "Consolas", "SFMono-Regular", monospace;
    }

    .node-source { fill: var(--source); }
    .node-dest { fill: var(--dest); }
    .node-ring {
      fill: none;
      stroke: rgba(207, 230, 250, 0.3);
      stroke-width: 1.1;
    }
    .region-label {
      fill: #d6e8fb;
      font-size: 10px;
      letter-spacing: 0.04em;
      text-transform: uppercase;
      paint-order: stroke;
      stroke: rgba(5, 12, 22, 0.9);
      stroke-width: 3;
      stroke-linejoin: round;
      font-weight: 700;
    }
    .replica-label {
      fill: rgba(207, 230, 250, 0.9);
      font-size: 8px;
      letter-spacing: 0.03em;
      paint-order: stroke;
      stroke: rgba(7, 15, 26, 0.85);
      stroke-width: 2;
    }
    .link {
      fill: none;
      stroke-linecap: round;
      animation: pulse 7s linear infinite;
    }
    @keyframes pulse {
      from { stroke-dashoffset: 0; }
      to { stroke-dashoffset: -90; }
    }

    .land {
      fill: rgba(88, 146, 194, 0.12);
      stroke: rgba(161, 195, 223, 0.12);
      stroke-width: 1;
    }
    .grid-line {
      stroke: rgba(159, 191, 218, 0.12);
      stroke-width: 1;
      fill: none;
    }

    @media (max-width: 1080px) {
      .layout {
        grid-template-columns: 1fr;
      }
      .map-wrap {
        min-height: 500px;
      }
    }
  </style>
</head>
<body>
  <div class="page">
    <section class="hero">
      <h1>Railway Cross-Service Replica Latency Map</h1>
      <p>Source fanout: <span class="mono">service-b</span> replica IPs. Destination routing: DNS only via <span class="mono">service-a.railway.internal</span>. Links show latency and hit distribution per source replica.</p>
    </section>

    <section class="layout">
      <article class="panel map-wrap">
        <div class="status" id="status">Loading</div>
        <svg id="map" viewBox="0 0 1200 660" preserveAspectRatio="xMidYMid meet" aria-label="Replica latency map">
          <g id="gridLayer"></g>
          <g id="landLayer"></g>
          <g id="linkLayer"></g>
          <g id="nodeLayer"></g>
          <g id="labelLayer"></g>
        </svg>
        <div class="legend">
          <div class="legend-row"><span class="swatch" style="background: var(--good)"></span><span>Low latency (&lt;80ms)</span></div>
          <div class="legend-row"><span class="swatch" style="background: var(--warn)"></span><span>Medium latency (80-180ms)</span></div>
          <div class="legend-row"><span class="swatch" style="background: var(--bad)"></span><span>High latency (&gt;180ms)</span></div>
          <div class="legend-row"><span style="width:10px;height:10px;border-radius:50%;background:var(--source);display:inline-block"></span><span>Source replicas (service-b)</span></div>
          <div class="legend-row"><span style="width:10px;height:10px;border-radius:50%;background:var(--dest);display:inline-block"></span><span>Destination replicas (service-a)</span></div>
        </div>
      </article>

      <aside class="panel info">
        <div class="card">
          <h2>Summary</h2>
          <div class="kv">
            <span>Endpoint</span><b id="endpoint" class="mono">/combined</b>
            <span>Generated At</span><b id="generatedAt">-</b>
            <span>Source Regions</span><b id="srcRegions">-</b>
            <span>Source Replicas</span><b id="srcReplicas">-</b>
            <span>Destination Regions</span><b id="dstRegions">-</b>
            <span>Destination Replicas</span><b id="dstReplicas">-</b>
            <span>Total Link Samples</span><b id="totalSamples">-</b>
            <span>Overall Avg Latency</span><b id="avgLatency">-</b>
            <span>Observed Min/Max</span><b id="minMaxLatency">-</b>
          </div>
        </div>

        <div class="card">
          <h2>Region Matrix (avg ms)</h2>
          <div class="table-scroll">
            <table>
              <thead>
                <tr>
                  <th>Source Region</th>
                  <th>Dest Region</th>
                  <th>Hits</th>
                  <th>Avg</th>
                  <th>Min</th>
                  <th>Max</th>
                </tr>
              </thead>
              <tbody id="regionMatrixRows"></tbody>
            </table>
          </div>
        </div>

        <div class="card">
          <h2>Replica Links</h2>
          <div class="table-scroll">
            <table>
              <thead>
                <tr>
                  <th>Source</th>
                  <th>Dest</th>
                  <th>Hits</th>
                  <th>Avg</th>
                  <th>P95</th>
                </tr>
              </thead>
              <tbody id="replicaRows"></tbody>
            </table>
          </div>
          <p class="small">Rows are source-replica to destination-replica DNS routes. High single-link concentration indicates sticky routing from that source replica.</p>
        </div>
      </aside>
    </section>
  </div>

  <script>
    (function () {
      var svgNs = 'http://www.w3.org/2000/svg';
      var mapEl = document.getElementById('map');
      var gridLayer = document.getElementById('gridLayer');
      var landLayer = document.getElementById('landLayer');
      var linkLayer = document.getElementById('linkLayer');
      var nodeLayer = document.getElementById('nodeLayer');
      var labelLayer = document.getElementById('labelLayer');
      var width = 1200;
      var height = 660;

      var regionCoords = {
        'us-west1': { lat: 37.39, lon: -122.08 },
        'us-west2': { lat: 34.05, lon: -118.24 },
        'us-west3': { lat: 40.76, lon: -111.89 },
        'us-west4': { lat: 36.17, lon: -115.14 },
        'us-central1': { lat: 41.26, lon: -95.86 },
        'us-east1': { lat: 33.75, lon: -84.39 },
        'us-east4': { lat: 37.43, lon: -78.65 },
        'europe-west1': { lat: 50.11, lon: 8.68 },
        'europe-west2': { lat: 51.5, lon: -0.12 },
        'europe-west3': { lat: 50.93, lon: 6.95 },
        'europe-west4': { lat: 52.37, lon: 4.9 },
        'europe-north1': { lat: 60.17, lon: 24.94 },
        'asia-southeast1': { lat: 1.35, lon: 103.82 },
        'asia-southeast2': { lat: -6.21, lon: 106.85 },
        'asia-east1': { lat: 25.03, lon: 121.56 },
        'asia-east2': { lat: 22.3, lon: 114.16 },
        'asia-northeast1': { lat: 35.67, lon: 139.65 },
        'asia-northeast2': { lat: 37.56, lon: 126.98 },
        'asia-south1': { lat: 19.07, lon: 72.88 },
        'asia-south2': { lat: 17.38, lon: 78.49 },
        'australia-southeast1': { lat: -33.87, lon: 151.21 },
        'southamerica-east1': { lat: -23.55, lon: -46.63 }
      };

      function create(tag, attrs) {
        var el = document.createElementNS(svgNs, tag);
        Object.keys(attrs || {}).forEach(function (key) {
          if (attrs[key] !== null && attrs[key] !== undefined) {
            el.setAttribute(key, String(attrs[key]));
          }
        });
        return el;
      }

      function clear(el) {
        while (el.firstChild) el.removeChild(el.firstChild);
      }

      function setText(id, value) {
        var el = document.getElementById(id);
        if (el) el.textContent = value;
      }

      function fmtMs(value) {
        return Number.isFinite(value) ? value.toFixed(1) + ' ms' : '-';
      }

      function normalizeRegion(region) {
        var raw = String(region || 'unknown').toLowerCase();
        if (regionCoords[raw]) return raw;
        var stripped = raw.replace(/-[a-z0-9]{4,}$/i, '');
        if (regionCoords[stripped]) return stripped;
        return stripped;
      }

      function prettyRegion(region) {
        return String(region || 'unknown').replace(/-[a-z0-9]{4,}$/i, '');
      }

      function project(lon, lat) {
        return {
          x: ((lon + 180) / 360) * width,
          y: ((90 - lat) / 180) * height
        };
      }

      function latencyColor(avgMs) {
        if (!Number.isFinite(avgMs)) return 'rgba(173, 205, 233, 0.55)';
        if (avgMs < 80) return '#20c997';
        if (avgMs < 180) return '#ffb347';
        return '#ff6b57';
      }

      function strokeWidth(hits) {
        var h = Number(hits || 0);
        return Math.max(1.2, Math.min(7, 1 + h / 6));
      }

      function parseReplica(raw) {
        var text = String(raw || '');
        var idx = text.indexOf(':');
        if (idx < 0) return { region: 'unknown', replicaId: text || 'unknown' };
        return {
          region: text.slice(0, idx),
          replicaId: text.slice(idx + 1)
        };
      }

      function nodeId(kind, region, replicaId) {
        return kind + '|' + region + '|' + replicaId;
      }

      function toShortReplica(id) {
        if (!id) return 'unknown';
        return id.slice(0, 8);
      }

      function curvedPath(x1, y1, x2, y2) {
        var mx = (x1 + x2) / 2;
        var my = (y1 + y2) / 2;
        var dx = x2 - x1;
        var dy = y2 - y1;
        var distance = Math.sqrt(dx * dx + dy * dy) || 1;
        var nx = -dy / distance;
        var ny = dx / distance;
        var curve = Math.min(120, 40 + distance * 0.15);
        var cx = mx + nx * curve;
        var cy = my + ny * curve;
        return 'M ' + x1 + ' ' + y1 + ' Q ' + cx + ' ' + cy + ' ' + x2 + ' ' + y2;
      }

      function drawBackground() {
        clear(gridLayer);
        clear(landLayer);

        for (var lat = -60; lat <= 60; lat += 30) {
          var y = project(0, lat).y;
          gridLayer.appendChild(create('path', {
            d: 'M 0 ' + y + ' L ' + width + ' ' + y,
            class: 'grid-line'
          }));
        }

        for (var lon = -150; lon <= 150; lon += 30) {
          var x = project(lon, 0).x;
          gridLayer.appendChild(create('path', {
            d: 'M ' + x + ' 0 L ' + x + ' ' + height,
            class: 'grid-line'
          }));
        }

        var landShapes = [
          'M 85 170 C 170 90, 315 80, 390 135 C 460 185, 465 290, 395 350 C 335 400, 255 410, 190 375 C 115 330, 72 245, 85 170 Z',
          'M 475 125 C 555 80, 645 95, 710 140 C 760 176, 765 248, 715 293 C 660 344, 560 355, 500 320 C 430 280, 415 190, 475 125 Z',
          'M 660 355 C 745 332, 845 360, 900 410 C 948 454, 943 528, 886 565 C 827 603, 730 600, 678 552 C 620 500, 612 414, 660 355 Z'
        ];
        landShapes.forEach(function (shape) {
          landLayer.appendChild(create('path', { d: shape, class: 'land' }));
        });
      }

      function buildNodePositions(report) {
        var bridge = report.bridge || {};
        var sourceReplicas = Array.isArray(bridge.sourceReplicas) ? bridge.sourceReplicas : [];
        var destReplicas = Array.isArray(bridge.dnsDestReplicas) ? bridge.dnsDestReplicas : [];
        var regions = new Set();
        var nodes = [];

        sourceReplicas.forEach(function (raw) {
          var parsed = parseReplica(raw);
          nodes.push({ kind: 'source', region: parsed.region, replicaId: parsed.replicaId });
          regions.add(parsed.region);
        });
        destReplicas.forEach(function (raw) {
          var parsed = parseReplica(raw);
          nodes.push({ kind: 'dest', region: parsed.region, replicaId: parsed.replicaId });
          regions.add(parsed.region);
        });

        var uniqueRegions = Array.from(regions).sort();
        var unknownRegions = [];
        var regionAnchors = {};
        uniqueRegions.forEach(function (region) {
          var key = normalizeRegion(region);
          var coords = regionCoords[key];
          if (coords) {
            regionAnchors[region] = project(coords.lon, coords.lat);
          } else {
            unknownRegions.push(region);
          }
        });

        if (unknownRegions.length > 0) {
          unknownRegions.forEach(function (region, idx) {
            var columns = Math.min(4, unknownRegions.length);
            var col = idx % columns;
            var row = Math.floor(idx / columns);
            var x = ((col + 1) / (columns + 1)) * width;
            var y = height - 80 - row * 55;
            regionAnchors[region] = { x: x, y: y };
          });
        }

        var grouped = {};
        nodes.forEach(function (node) {
          var key = node.kind + '|' + node.region;
          if (!grouped[key]) grouped[key] = [];
          grouped[key].push(node);
        });

        var positioned = [];
        Object.keys(grouped).forEach(function (groupKey) {
          var list = grouped[groupKey];
          var first = list[0];
          var anchor = regionAnchors[first.region] || { x: width / 2, y: height / 2 };
          var sideOffset = first.kind === 'source' ? -14 : 14;
          var base = {
            x: Math.max(20, Math.min(width - 20, anchor.x)),
            y: Math.max(20, Math.min(height - 20, anchor.y + sideOffset))
          };
          var total = list.length;
          list.forEach(function (node, index) {
            var angle = ((Math.PI * 2) * index) / Math.max(total, 1) + (node.kind === 'source' ? -0.45 : 0.45);
            var radius = 11 + Math.floor(index / 8) * 10;
            var x = base.x + Math.cos(angle) * radius;
            var y = base.y + Math.sin(angle) * radius;
            positioned.push({
              id: nodeId(node.kind, node.region, node.replicaId),
              kind: node.kind,
              region: node.region,
              replicaId: node.replicaId,
              x: Math.max(14, Math.min(width - 14, x)),
              y: Math.max(14, Math.min(height - 14, y))
            });
          });
        });

        return { nodes: positioned, regionAnchors: regionAnchors };
      }

      function renderMap(report) {
        drawBackground();
        clear(linkLayer);
        clear(nodeLayer);
        clear(labelLayer);

        var bridge = report.bridge || {};
        var links = Array.isArray(bridge.replicaLatencyLinks) ? bridge.replicaLatencyLinks : [];
        var positioned = buildNodePositions(report);
        var nodeMap = {};
        positioned.nodes.forEach(function (node) {
          nodeMap[node.id] = node;
        });

        var linkEntries = links
          .filter(function (link) {
            return link && link.latency && Number.isFinite(link.latency.avgMs);
          })
          .map(function (link) {
            var sourceKey = nodeId('source', link.sourceRegion, link.sourceReplicaId);
            var destKey = nodeId('dest', link.destRegion, link.destReplicaId);
            return {
              link: link,
              source: nodeMap[sourceKey],
              dest: nodeMap[destKey]
            };
          })
          .filter(function (item) {
            return item.source && item.dest;
          });

        linkEntries.sort(function (a, b) {
          return (a.link.latency.avgMs || 0) - (b.link.latency.avgMs || 0);
        });

        linkEntries.forEach(function (entry) {
          var link = entry.link;
          var avg = Number(link.latency.avgMs);
          var path = create('path', {
            d: curvedPath(entry.source.x, entry.source.y, entry.dest.x, entry.dest.y),
            class: 'link',
            stroke: latencyColor(avg),
            'stroke-width': strokeWidth(link.hits),
            'stroke-opacity': 0.72,
            'stroke-dasharray': '8 6'
          });
          var title = create('title', {});
          title.textContent =
            entry.source.region + ':' + toShortReplica(entry.source.replicaId) + ' -> ' +
            entry.dest.region + ':' + toShortReplica(entry.dest.replicaId) +
            ' | avg=' + fmtMs(avg) + ' p95=' + fmtMs(link.latency.p95Ms) +
            ' hits=' + String(link.hits || 0);
          path.appendChild(title);
          linkLayer.appendChild(path);
        });

        positioned.nodes.forEach(function (node) {
          var ring = create('circle', {
            cx: node.x,
            cy: node.y,
            r: 7.5,
            class: 'node-ring'
          });
          nodeLayer.appendChild(ring);

          var dot = create('circle', {
            cx: node.x,
            cy: node.y,
            r: 4.2,
            class: node.kind === 'source' ? 'node-source' : 'node-dest'
          });
          var dotTitle = create('title', {});
          dotTitle.textContent = (node.kind === 'source' ? 'source ' : 'dest ') + node.region + ':' + node.replicaId;
          dot.appendChild(dotTitle);
          nodeLayer.appendChild(dot);
        });

        Object.keys(positioned.regionAnchors).forEach(function (region) {
          var anchor = positioned.regionAnchors[region];
          var label = create('text', {
            x: anchor.x,
            y: anchor.y - 18,
            'text-anchor': 'middle',
            class: 'region-label'
          });
          label.textContent = prettyRegion(region);
          labelLayer.appendChild(label);
        });
      }

      function renderTables(report) {
        var bridge = report.bridge || {};
        var regionRows = Array.isArray(bridge.regionLatencyMatrix) ? bridge.regionLatencyMatrix : [];
        var replicaRows = Array.isArray(bridge.replicaLatencyLinks) ? bridge.replicaLatencyLinks : [];

        var regionBody = document.getElementById('regionMatrixRows');
        regionBody.innerHTML = '';
        if (regionRows.length === 0) {
          var empty = document.createElement('tr');
          empty.innerHTML = '<td colspan="6">No region latency rows</td>';
          regionBody.appendChild(empty);
        } else {
          regionRows.forEach(function (row) {
            var tr = document.createElement('tr');
            tr.innerHTML =
              '<td>' + row.sourceRegion + '</td>' +
              '<td>' + row.destRegion + '</td>' +
              '<td>' + String(row.hits || 0) + '</td>' +
              '<td>' + fmtMs(row.avgLatencyMs) + '</td>' +
              '<td>' + fmtMs(row.minLatencyMs) + '</td>' +
              '<td>' + fmtMs(row.maxLatencyMs) + '</td>';
            regionBody.appendChild(tr);
          });
        }

        var replicaBody = document.getElementById('replicaRows');
        replicaBody.innerHTML = '';
        var sortedReplica = replicaRows.slice().sort(function (a, b) {
          var av = Number.isFinite(a && a.latency && a.latency.avgMs) ? a.latency.avgMs : Number.POSITIVE_INFINITY;
          var bv = Number.isFinite(b && b.latency && b.latency.avgMs) ? b.latency.avgMs : Number.POSITIVE_INFINITY;
          if (av === bv) return (b.hits || 0) - (a.hits || 0);
          return av - bv;
        });

        if (sortedReplica.length === 0) {
          var emptyReplica = document.createElement('tr');
          emptyReplica.innerHTML = '<td colspan="5">No replica links</td>';
          replicaBody.appendChild(emptyReplica);
        } else {
          sortedReplica.forEach(function (row) {
            var tr = document.createElement('tr');
            tr.innerHTML =
              '<td class="mono">' + row.sourceRegion + ':' + toShortReplica(row.sourceReplicaId) + '</td>' +
              '<td class="mono">' + row.destRegion + ':' + toShortReplica(row.destReplicaId) + '</td>' +
              '<td>' + String(row.hits || 0) + '</td>' +
              '<td>' + fmtMs(row.latency ? row.latency.avgMs : null) + '</td>' +
              '<td>' + fmtMs(row.latency ? row.latency.p95Ms : null) + '</td>';
            replicaBody.appendChild(tr);
          });
        }
      }

      function renderSummary(report, endpoint) {
        var bridge = report.bridge || {};
        var overall = bridge.latencyOverall || {};
        setText('endpoint', endpoint);
        setText('generatedAt', report.generatedAt || '-');
        setText('srcRegions', String((bridge.sourceRegions || []).length));
        setText('srcReplicas', String((bridge.sourceReplicas || []).length));
        setText('dstRegions', String((bridge.dnsDestRegions || []).length));
        setText('dstReplicas', String((bridge.dnsDestReplicas || []).length));
        setText('totalSamples', String(overall.count || 0));
        setText('avgLatency', fmtMs(overall.avgLatencyMs));
        setText('minMaxLatency', fmtMs(overall.minLatencyMs) + ' / ' + fmtMs(overall.maxLatencyMs));
      }

      async function load() {
        var query = window.location.search || '';
        var endpoint = '/combined' + query;
        setText('endpoint', endpoint);
        setText('status', 'Loading ' + endpoint);
        try {
          var response = await fetch(endpoint, { headers: { accept: 'application/json' } });
          if (!response.ok) {
            throw new Error('HTTP ' + response.status);
          }
          var report = await response.json();
          renderSummary(report, endpoint);
          renderMap(report);
          renderTables(report);
          setText('status', 'Updated ' + new Date().toLocaleTimeString());
        } catch (error) {
          setText('status', 'Load failed: ' + (error && error.message ? error.message : 'unknown'));
        }
      }

      load();
    })();
  </script>
</body>
</html>`;
}

function buildMap2Html() {
  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Railway DNS Routing &amp; Latency</title>
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"><\/script>
  <style>
    :root {
      --bg: #0a0e1a;
      --surface: rgba(12, 18, 35, 0.92);
      --surface-border: rgba(100, 140, 180, 0.18);
      --text: #d4e4f7;
      --muted: #7a99b8;
      --accent: #4dc9f6;
      --good: #22c993;
      --warn: #f5a623;
      --bad: #f25757;
      --source-color: #4dc9f6;
      --dest-color: #ffd166;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      background: var(--bg);
      color: var(--text);
      height: 100vh;
      display: flex;
      overflow: hidden;
    }
    #map-container { flex: 1; position: relative; }
    #map { width: 100%; height: 100%; }
    .leaflet-container { background: #0a0e1a; }
    .leaflet-tile-pane { opacity: 0.85; }

    #sidebar {
      width: 420px;
      min-width: 340px;
      background: var(--surface);
      border-left: 1px solid var(--surface-border);
      overflow-y: auto;
      display: flex;
      flex-direction: column;
    }

    .sidebar-header {
      padding: 16px;
      border-bottom: 1px solid var(--surface-border);
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
    }
    .sidebar-header h1 {
      font-size: 0.95rem;
      letter-spacing: 0.04em;
      text-transform: uppercase;
      white-space: nowrap;
    }
    #refresh-btn {
      background: rgba(77, 201, 246, 0.12);
      border: 1px solid rgba(77, 201, 246, 0.3);
      color: var(--accent);
      padding: 6px 14px;
      border-radius: 6px;
      cursor: pointer;
      font-size: 0.8rem;
      font-weight: 600;
      transition: background 0.15s;
    }
    #refresh-btn:hover { background: rgba(77, 201, 246, 0.22); }
    #refresh-btn:disabled { opacity: 0.4; cursor: default; }

    .section {
      padding: 14px 16px;
      border-bottom: 1px solid var(--surface-border);
    }
    .section h2 {
      font-size: 0.78rem;
      text-transform: uppercase;
      letter-spacing: 0.06em;
      color: var(--muted);
      margin-bottom: 10px;
    }

    .stats-grid {
      display: grid;
      grid-template-columns: 1fr 1fr 1fr;
      gap: 8px;
    }
    .stat-card {
      background: rgba(255,255,255,0.03);
      border: 1px solid rgba(100,140,180,0.1);
      border-radius: 8px;
      padding: 8px 10px;
    }
    .stat-card .label {
      font-size: 0.7rem;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.04em;
    }
    .stat-card .value {
      font-size: 1.1rem;
      font-weight: 700;
      margin-top: 2px;
    }

    /* NxN grid tables */
    .nxn-wrap { overflow-x: auto; }
    .nxn-table {
      border-collapse: collapse;
      font-size: 0.72rem;
      width: auto;
      min-width: 100%;
    }
    .nxn-table th, .nxn-table td {
      padding: 5px 7px;
      text-align: center;
      white-space: nowrap;
      font-variant-numeric: tabular-nums;
    }
    .nxn-table thead th {
      color: var(--muted);
      font-size: 0.65rem;
      text-transform: uppercase;
      letter-spacing: 0.04em;
      font-weight: 600;
      border-bottom: 1px solid var(--surface-border);
      position: sticky;
      top: 0;
      background: var(--surface);
    }
    .nxn-table .row-header {
      text-align: right;
      color: var(--accent);
      font-weight: 600;
      font-family: "SF Mono", "Menlo", "Consolas", monospace;
      font-size: 0.68rem;
      padding-right: 10px;
      border-right: 1px solid var(--surface-border);
    }
    .nxn-table .col-header {
      color: var(--dest-color);
      font-family: "SF Mono", "Menlo", "Consolas", monospace;
    }
    .nxn-table .row-total {
      color: var(--muted);
      font-size: 0.65rem;
      border-left: 1px solid var(--surface-border);
      padding-left: 10px;
    }
    .nxn-cell {
      border-radius: 4px;
      min-width: 48px;
    }
    .nxn-cell .hits { font-weight: 700; }
    .nxn-cell .pct { font-size: 0.6rem; color: var(--muted); display: block; }
    .nxn-cell.empty { color: rgba(122,153,184,0.3); }
    .nxn-cell.self-route {
      border: 1px dashed rgba(77,201,246,0.3);
    }

    .legend-bar {
      display: flex;
      align-items: center;
      gap: 14px;
      padding: 10px 16px;
      border-bottom: 1px solid var(--surface-border);
      font-size: 0.74rem;
      color: var(--muted);
      flex-wrap: wrap;
    }
    .legend-item {
      display: flex;
      align-items: center;
      gap: 5px;
      white-space: nowrap;
    }
    .legend-dot {
      width: 10px;
      height: 10px;
      border-radius: 50%;
      flex-shrink: 0;
    }
    .legend-line {
      width: 20px;
      height: 3px;
      border-radius: 2px;
      flex-shrink: 0;
    }
    .legend-line-dashed {
      width: 20px;
      height: 0;
      border-top: 2px dashed rgba(122,153,184,0.35);
      flex-shrink: 0;
    }

    #status-bar {
      padding: 8px 16px;
      font-size: 0.74rem;
      color: var(--muted);
      border-bottom: 1px solid var(--surface-border);
    }

    .mono { font-family: "SF Mono", "Menlo", "Consolas", monospace; }
    .ms-good { color: var(--good); }
    .ms-warn { color: var(--warn); }
    .ms-bad { color: var(--bad); }

    .arc-tooltip {
      background: rgba(8, 14, 28, 0.94) !important;
      border: 1px solid rgba(100, 140, 180, 0.3) !important;
      border-radius: 8px !important;
      padding: 8px 12px !important;
      color: var(--text) !important;
      font-size: 0.78rem !important;
      line-height: 1.5 !important;
      box-shadow: 0 4px 20px rgba(0,0,0,0.5) !important;
    }
    .arc-tooltip .tip-header { font-weight: 700; margin-bottom: 4px; color: #fff; }
    .arc-tooltip .tip-row { display: flex; justify-content: space-between; gap: 16px; }
    .arc-tooltip .tip-label { color: var(--muted); }
    .arc-tooltip .tip-value { font-weight: 600; font-variant-numeric: tabular-nums; }

    .region-tooltip {
      background: rgba(8, 14, 28, 0.94) !important;
      border: 1px solid rgba(100, 140, 180, 0.3) !important;
      border-radius: 8px !important;
      padding: 8px 12px !important;
      color: var(--text) !important;
      font-size: 0.78rem !important;
      box-shadow: 0 4px 20px rgba(0,0,0,0.5) !important;
    }

    @keyframes pulse-ring {
      0%   { transform: scale(1);   opacity: 0.7; }
      50%  { transform: scale(1.6); opacity: 0; }
      100% { transform: scale(1);   opacity: 0; }
    }
    .self-ring {
      border-radius: 50%;
      animation: pulse-ring 2s ease-out infinite;
      pointer-events: none;
    }

    @media (max-width: 860px) {
      body { flex-direction: column; }
      #sidebar { width: 100%; min-width: 0; max-height: 45vh; border-left: none; border-top: 1px solid var(--surface-border); }
      #map-container { min-height: 55vh; }
    }
  </style>
</head>
<body>
  <div id="map-container">
    <div id="map"></div>
  </div>
  <div id="sidebar">
    <div class="sidebar-header">
      <h1>DNS Routing &amp; Latency</h1>
      <button id="refresh-btn">Refresh</button>
    </div>
    <div id="status-bar">Loading...</div>
    <div class="legend-bar">
      <span class="legend-item"><span class="legend-dot" style="background:var(--source-color)"></span>Source</span>
      <span class="legend-item"><span class="legend-dot" style="background:var(--dest-color)"></span>Dest</span>
      <span class="legend-item"><span class="legend-line" style="background:var(--good)"></span>&lt;80ms</span>
      <span class="legend-item"><span class="legend-line" style="background:var(--warn)"></span>80-180ms</span>
      <span class="legend-item"><span class="legend-line" style="background:var(--bad)"></span>&gt;180ms</span>
      <span class="legend-item"><span class="legend-line-dashed"></span>Unrouted</span>
    </div>
    <div class="section">
      <h2>DNS Routing Pattern</h2>
      <div class="nxn-wrap" id="routing-grid"></div>
    </div>
    <div class="section">
      <h2>Latency Matrix</h2>
      <div class="nxn-wrap" id="latency-grid"></div>
    </div>
    <div class="section">
      <h2>Summary</h2>
      <div class="stats-grid" id="summary-stats">
        <div class="stat-card"><div class="label">Src Regions</div><div class="value" id="s-src-regions">-</div></div>
        <div class="stat-card"><div class="label">Dst Regions</div><div class="value" id="s-dst-regions">-</div></div>
        <div class="stat-card"><div class="label">Src Replicas</div><div class="value" id="s-src-replicas">-</div></div>
        <div class="stat-card"><div class="label">Dst Replicas</div><div class="value" id="s-dst-replicas">-</div></div>
        <div class="stat-card"><div class="label">Avg Latency</div><div class="value" id="s-avg-latency">-</div></div>
        <div class="stat-card"><div class="label">Samples</div><div class="value" id="s-samples">-</div></div>
      </div>
    </div>
  </div>

  <script>
  (function() {
    var REGION_COORDS = {
      'us-west1':           [37.39, -122.08],
      'us-west2':           [34.05, -118.24],
      'us-west3':           [40.76, -111.89],
      'us-west4':           [36.17, -115.14],
      'us-central1':        [41.26, -95.86],
      'us-east1':           [33.75, -84.39],
      'us-east4':           [37.43, -78.65],
      'europe-west1':       [50.11, 8.68],
      'europe-west2':       [51.50, -0.12],
      'europe-west3':       [50.93, 6.95],
      'europe-west4':       [52.37, 4.90],
      'europe-north1':      [60.17, 24.94],
      'asia-southeast1':    [1.35, 103.82],
      'asia-southeast2':    [-6.21, 106.85],
      'asia-east1':         [25.03, 121.56],
      'asia-east2':         [22.30, 114.16],
      'asia-northeast1':    [35.67, 139.65],
      'asia-northeast2':    [37.56, 126.98],
      'asia-south1':        [19.07, 72.88],
      'asia-south2':        [17.38, 78.49],
      'australia-southeast1': [-33.87, 151.21],
      'southamerica-east1': [-23.55, -46.63]
    };

    var map = L.map('map', {
      center: [25, 10],
      zoom: 2,
      minZoom: 2,
      maxZoom: 7,
      zoomControl: true,
      attributionControl: false
    });

    L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_nolabels/{z}/{x}/{y}{r}.png', {
      subdomains: 'abcd',
      maxZoom: 19
    }).addTo(map);

    L.control.attribution({ position: 'bottomleft', prefix: false })
      .addAttribution('&copy; <a href="https://carto.com/">CARTO</a>')
      .addTo(map);

    var arcGroup = L.layerGroup().addTo(map);
    var nodeGroup = L.layerGroup().addTo(map);

    function normalizeRegion(r) {
      var raw = String(r || 'unknown').toLowerCase();
      if (REGION_COORDS[raw]) return raw;
      var stripped = raw.replace(/-[a-z0-9]{4,}$/i, '');
      if (REGION_COORDS[stripped]) return stripped;
      return raw;
    }

    function regionLatLng(region) {
      var key = normalizeRegion(region);
      var c = REGION_COORDS[key];
      if (c) return c;
      var hash = 0;
      for (var i = 0; i < region.length; i++) hash = ((hash << 5) - hash + region.charCodeAt(i)) | 0;
      return [(hash % 60) - 10, ((hash >> 8) % 300) - 150];
    }

    function latencyColor(ms) {
      if (ms == null || !isFinite(ms)) return 'rgba(150,180,210,0.5)';
      if (ms < 80) return '#22c993';
      if (ms < 180) return '#f5a623';
      return '#f25757';
    }

    function msClass(ms) {
      if (ms == null || !isFinite(ms)) return '';
      if (ms < 80) return 'ms-good';
      if (ms < 180) return 'ms-warn';
      return 'ms-bad';
    }

    function fmtMs(v) {
      return (v != null && isFinite(v)) ? v.toFixed(1) : '-';
    }

    function shortRegion(r) {
      return r.replace(/^(us|europe|asia|australia|southamerica)-/, function(m, p) {
        var abbr = { us:'us', europe:'eu', asia:'as', australia:'au', southamerica:'sa' };
        return (abbr[p] || p) + '-';
      });
    }

    function arcPoints(from, to, numPoints) {
      numPoints = numPoints || 40;
      var pts = [];
      var lat1 = from[0], lng1 = from[1], lat2 = to[0], lng2 = to[1];
      var dLng = lng2 - lng1;
      if (Math.abs(dLng) > 180) {
        if (dLng > 0) lng1 += 360;
        else lng2 += 360;
      }
      var midLat = (lat1 + lat2) / 2;
      var midLng = (lng1 + lng2) / 2;
      var dist = Math.sqrt((lat2 - lat1) * (lat2 - lat1) + (dLng) * (dLng));
      var bulge = Math.min(25, Math.max(5, dist * 0.2));
      var perpLat = -(lng2 - lng1);
      var perpLng = (lat2 - lat1);
      var perpLen = Math.sqrt(perpLat * perpLat + perpLng * perpLng) || 1;
      perpLat = perpLat / perpLen * bulge;
      perpLng = perpLng / perpLen * bulge;
      var ctrlLat = midLat + perpLat;
      var ctrlLng = midLng + perpLng;
      for (var i = 0; i <= numPoints; i++) {
        var t = i / numPoints;
        var u = 1 - t;
        var lat = u * u * lat1 + 2 * u * t * ctrlLat + t * t * lat2;
        var lng = u * u * lng1 + 2 * u * t * ctrlLng + t * t * lng2;
        if (lng > 180) lng -= 360;
        if (lng < -180) lng += 360;
        pts.push([lat, lng]);
      }
      return pts;
    }

    function strokeW(hits) {
      return Math.max(1.5, Math.min(6, 1.5 + (hits || 0) / 5));
    }

    function buildLookup(matrix) {
      var m = {};
      (matrix || []).forEach(function(e) {
        var key = e.sourceRegion + '|' + e.destRegion;
        m[key] = e;
      });
      return m;
    }

    function renderMap(data) {
      arcGroup.clearLayers();
      nodeGroup.clearLayers();

      var bridge = data.bridge || {};
      var matrix = bridge.dnsMatrix || [];
      var srcRegions = bridge.sourceRegions || [];
      var dstRegions = bridge.dnsDestRegions || [];
      var sourceReplicas = bridge.sourceReplicas || [];
      var destReplicas = bridge.dnsDestReplicas || [];

      var lookup = buildLookup(matrix);

      var regionInfo = {};
      function ensureRegion(region, kind) {
        if (!regionInfo[region]) {
          regionInfo[region] = { sources: new Set(), dests: new Set(), latLng: regionLatLng(region) };
        }
        if (kind === 'source') regionInfo[region].sources.add(region);
        if (kind === 'dest') regionInfo[region].dests.add(region);
      }

      sourceReplicas.forEach(function(r) {
        var parts = r.split(':');
        ensureRegion(parts[0], 'source');
        regionInfo[parts[0]].sources.add(parts[1] || parts[0]);
      });
      destReplicas.forEach(function(r) {
        var parts = r.split(':');
        ensureRegion(parts[0], 'dest');
        regionInfo[parts[0]].dests.add(parts[1] || parts[0]);
      });

      var allSrc = srcRegions.length ? srcRegions : Object.keys(regionInfo).filter(function(k) { return regionInfo[k].sources.size > 0; });
      var allDst = dstRegions.length ? dstRegions : Object.keys(regionInfo).filter(function(k) { return regionInfo[k].dests.size > 0; });

      // Draw unrouted pairs as faint dashed gray lines
      allSrc.forEach(function(src) {
        allDst.forEach(function(dst) {
          if (src === dst) return; // skip same-region for arcs
          var key = src + '|' + dst;
          if (lookup[key]) return; // has data, will draw solid arc
          var srcLL = regionLatLng(src);
          var dstLL = regionLatLng(dst);
          var pts = arcPoints(srcLL, dstLL);
          var line = L.polyline(pts, {
            color: 'rgba(122,153,184,0.15)',
            weight: 1,
            opacity: 1,
            dashArray: '4 6',
            interactive: false
          });
          arcGroup.addLayer(line);
        });
      });

      // Draw observed routes as solid arcs
      var selfRouteRegions = {};
      matrix.forEach(function(edge) {
        if (edge.sourceRegion === edge.destRegion) {
          selfRouteRegions[edge.sourceRegion] = edge;
          return;
        }
        var srcLL = regionLatLng(edge.sourceRegion);
        var dstLL = regionLatLng(edge.destRegion);
        var avg = edge.avgLatencyMs;
        var color = latencyColor(avg);
        var pts = arcPoints(srcLL, dstLL);
        var line = L.polyline(pts, {
          color: color,
          weight: strokeW(edge.hits),
          opacity: 0.8
        });

        var tip = '<div class="arc-tooltip">' +
          '<div class="tip-header">' + edge.sourceRegion + ' \\u2192 ' + edge.destRegion + '</div>' +
          '<div class="tip-row"><span class="tip-label">Avg</span><span class="tip-value ' + msClass(avg) + '">' + fmtMs(avg) + ' ms</span></div>' +
          '<div class="tip-row"><span class="tip-label">Min / Max</span><span class="tip-value">' + fmtMs(edge.minLatencyMs) + ' / ' + fmtMs(edge.maxLatencyMs) + ' ms</span></div>' +
          '<div class="tip-row"><span class="tip-label">Hits</span><span class="tip-value">' + (edge.hits || 0) + '</span></div>' +
          '</div>';
        line.bindTooltip(tip, { sticky: true, className: 'arc-tooltip', direction: 'top' });
        arcGroup.addLayer(line);
      });

      // Draw nodes + self-route pulsing rings
      Object.keys(regionInfo).forEach(function(region) {
        var info = regionInfo[region];
        var ll = info.latLng;
        var srcCount = info.sources.size;
        var dstCount = info.dests.size;
        var hasSrc = srcCount > 0;
        var hasDst = dstCount > 0;

        if (hasSrc) {
          var srcMarker = L.circleMarker(ll, {
            radius: Math.min(16, 7 + srcCount),
            fillColor: '#4dc9f6',
            fillOpacity: 0.85,
            color: 'rgba(77,201,246,0.4)',
            weight: 2
          });
          var srcTip = '<div class="region-tooltip"><strong>' + region + '</strong> (source)<br>' + srcCount + ' replica(s)</div>';
          srcMarker.bindTooltip(srcTip, { className: 'region-tooltip', direction: 'top', offset: [0, -10] });
          nodeGroup.addLayer(srcMarker);
        }

        if (hasDst) {
          var offset = hasSrc ? [0.8, 0.8] : [0, 0];
          var dstMarker = L.circleMarker([ll[0] - offset[0], ll[1] + offset[1]], {
            radius: Math.min(16, 7 + dstCount),
            fillColor: '#ffd166',
            fillOpacity: 0.85,
            color: 'rgba(255,209,102,0.4)',
            weight: 2
          });
          var dstTip = '<div class="region-tooltip"><strong>' + region + '</strong> (dest)<br>' + dstCount + ' replica(s)</div>';
          dstMarker.bindTooltip(dstTip, { className: 'region-tooltip', direction: 'top', offset: [0, -10] });
          nodeGroup.addLayer(dstMarker);
        }

        // Pulsing ring for same-region routes
        if (selfRouteRegions[region]) {
          var selfEdge = selfRouteRegions[region];
          var ringColor = latencyColor(selfEdge.avgLatencyMs);
          var ringIcon = L.divIcon({
            html: '<div class="self-ring" style="width:32px;height:32px;border:3px solid ' + ringColor + ';"></div>',
            className: '',
            iconSize: [32, 32],
            iconAnchor: [16, 16]
          });
          var ringMarker = L.marker(ll, { icon: ringIcon, interactive: true });
          var ringTip = '<div class="arc-tooltip">' +
            '<div class="tip-header">' + region + ' \\u2192 ' + region + ' (same-region)</div>' +
            '<div class="tip-row"><span class="tip-label">Avg</span><span class="tip-value ' + msClass(selfEdge.avgLatencyMs) + '">' + fmtMs(selfEdge.avgLatencyMs) + ' ms</span></div>' +
            '<div class="tip-row"><span class="tip-label">Hits</span><span class="tip-value">' + (selfEdge.hits || 0) + '</span></div>' +
            '</div>';
          ringMarker.bindTooltip(ringTip, { className: 'arc-tooltip', direction: 'top', offset: [0, -20] });
          nodeGroup.addLayer(ringMarker);
        }

        var labelIcon = L.divIcon({
          html: '<div style="color:#d4e4f7;font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:0.04em;text-shadow:0 1px 4px rgba(0,0,0,0.9);white-space:nowrap;pointer-events:none">' + region + '</div>',
          className: '',
          iconAnchor: [-8, 20]
        });
        L.marker(ll, { icon: labelIcon, interactive: false }).addTo(nodeGroup);
      });
    }

    function renderSidebar(data) {
      var bridge = data.bridge || {};
      var overall = bridge.latencyOverall || {};
      var matrix = bridge.dnsMatrix || [];
      var srcRegions = (bridge.sourceRegions || []).slice().sort();
      var dstRegions = (bridge.dnsDestRegions || []).slice().sort();

      // Summary stats
      document.getElementById('s-src-regions').textContent = srcRegions.length;
      document.getElementById('s-dst-regions').textContent = dstRegions.length;
      document.getElementById('s-src-replicas').textContent = (bridge.sourceReplicas || []).length;
      document.getElementById('s-dst-replicas').textContent = (bridge.dnsDestReplicas || []).length;
      document.getElementById('s-avg-latency').innerHTML = '<span class="' + msClass(overall.avgLatencyMs) + '">' + fmtMs(overall.avgLatencyMs) + ' ms</span>';
      document.getElementById('s-samples').textContent = overall.count || 0;

      var lookup = buildLookup(matrix);

      // Compute row totals for routing %
      var rowTotals = {};
      srcRegions.forEach(function(src) { rowTotals[src] = 0; });
      matrix.forEach(function(e) { rowTotals[e.sourceRegion] = (rowTotals[e.sourceRegion] || 0) + (e.hits || 0); });

      // Build DNS Routing Pattern NxN grid
      var routeHtml = '<table class="nxn-table"><thead><tr><th></th>';
      dstRegions.forEach(function(d) {
        routeHtml += '<th class="col-header">' + shortRegion(d) + '</th>';
      });
      routeHtml += '<th class="row-total">Total</th></tr></thead><tbody>';

      srcRegions.forEach(function(src) {
        routeHtml += '<tr><td class="row-header">' + shortRegion(src) + '</td>';
        var total = rowTotals[src] || 0;
        dstRegions.forEach(function(dst) {
          var key = src + '|' + dst;
          var edge = lookup[key];
          var isSelf = src === dst;
          if (edge && edge.hits > 0) {
            var pct = total > 0 ? ((edge.hits / total) * 100) : 0;
            var intensity = Math.min(1, pct / 100);
            var bg = 'rgba(77,201,246,' + (0.08 + intensity * 0.4).toFixed(2) + ')';
            routeHtml += '<td class="nxn-cell' + (isSelf ? ' self-route' : '') + '" style="background:' + bg + '">' +
              '<span class="hits">' + edge.hits + '</span>' +
              '<span class="pct">' + pct.toFixed(0) + '%</span></td>';
          } else {
            routeHtml += '<td class="nxn-cell empty' + (isSelf ? ' self-route' : '') + '">\\u2014</td>';
          }
        });
        routeHtml += '<td class="row-total mono">' + total + '</td></tr>';
      });
      routeHtml += '</tbody></table>';
      document.getElementById('routing-grid').innerHTML = srcRegions.length === 0
        ? '<div style="color:var(--muted);font-size:0.78rem">No data</div>'
        : routeHtml;

      // Build Latency Matrix NxN grid
      var latHtml = '<table class="nxn-table"><thead><tr><th></th>';
      dstRegions.forEach(function(d) {
        latHtml += '<th class="col-header">' + shortRegion(d) + '</th>';
      });
      latHtml += '</tr></thead><tbody>';

      srcRegions.forEach(function(src) {
        latHtml += '<tr><td class="row-header">' + shortRegion(src) + '</td>';
        dstRegions.forEach(function(dst) {
          var key = src + '|' + dst;
          var edge = lookup[key];
          var isSelf = src === dst;
          if (edge && edge.avgLatencyMs != null && isFinite(edge.avgLatencyMs)) {
            var cls = msClass(edge.avgLatencyMs);
            latHtml += '<td class="nxn-cell ' + cls + (isSelf ? ' self-route' : '') + '">' + fmtMs(edge.avgLatencyMs) + '</td>';
          } else {
            latHtml += '<td class="nxn-cell empty' + (isSelf ? ' self-route' : '') + '">\\u2014</td>';
          }
        });
        latHtml += '</tr>';
      });
      latHtml += '</tbody></table>';
      document.getElementById('latency-grid').innerHTML = srcRegions.length === 0
        ? '<div style="color:var(--muted);font-size:0.78rem">No data</div>'
        : latHtml;
    }

    var loading = false;
    function loadData() {
      if (loading) return;
      loading = true;
      var btn = document.getElementById('refresh-btn');
      btn.disabled = true;
      btn.textContent = 'Loading...';
      document.getElementById('status-bar').textContent = 'Fetching /combined...';

      var query = window.location.search || '';
      fetch('/combined' + query, { headers: { accept: 'application/json' } })
        .then(function(r) {
          if (!r.ok) throw new Error('HTTP ' + r.status);
          return r.json();
        })
        .then(function(data) {
          renderMap(data);
          renderSidebar(data);
          document.getElementById('status-bar').textContent = 'Updated ' + new Date().toLocaleTimeString() + ' \\u2014 generated ' + (data.generatedAt || '?');
        })
        .catch(function(err) {
          document.getElementById('status-bar').textContent = 'Error: ' + (err.message || err);
        })
        .finally(function() {
          loading = false;
          btn.disabled = false;
          btn.textContent = 'Refresh';
        });
    }

    document.getElementById('refresh-btn').addEventListener('click', loadData);
    loadData();
  })();
  <\/script>
</body>
</html>`;
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

  if (url.pathname === "/map") {
    html(res, 200, buildMapHtml());
    return;
  }

  if (url.pathname === "/map2") {
    html(res, 200, buildMap2Html());
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
