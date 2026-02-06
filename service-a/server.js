const http = require("http");
const os = require("os");
const { URL } = require("url");
const { execFile } = require("child_process");
const dns = require("dns").promises;

const PORT = Number(process.env.PORT || 8080);
const SERVICE_NAME = process.env.RAILWAY_SERVICE_NAME || "service-a";

function json(res, status, payload) {
  res.writeHead(status, { "content-type": "application/json; charset=utf-8" });
  res.end(JSON.stringify(payload, null, 2));
}

function parseTargets(searchParams) {
  const explicit = searchParams
    .getAll("host")
    .flatMap((v) => v.split(","))
    .map((v) => v.trim())
    .filter(Boolean);

  if (explicit.length > 0) return explicit;

  const defaults = [];
  if (process.env.RAILWAY_PRIVATE_DOMAIN) defaults.push(process.env.RAILWAY_PRIVATE_DOMAIN);
  if (process.env.PEER_HOST) defaults.push(process.env.PEER_HOST);
  if (defaults.length === 0) defaults.push(`${SERVICE_NAME}.railway.internal`);
  return defaults;
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

  if (url.pathname === "/dig") {
    const targets = parseTargets(url.searchParams);
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

  if (url.pathname === "/report") {
    const targets = parseTargets(url.searchParams);
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

  json(res, 404, { error: "not_found", path: url.pathname });
});

server.listen(PORT, () => {
  console.log(
    JSON.stringify({
      message: "service-a listening",
      port: PORT,
      service: SERVICE_NAME,
      replicaId: process.env.RAILWAY_REPLICA_ID || null,
      replicaRegion: process.env.RAILWAY_REPLICA_REGION || null
    })
  );
});
