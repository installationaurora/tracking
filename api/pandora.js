const REGISTRY_KEY = process.env.PANDORA_REGISTRY_KEY;
const REDIS_URL = process.env.UPSTASH_REDIS_REST_URL;
const REDIS_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;

const HEARTBEAT_TTL = 15;
const MARKER_TTL = 24 * 60 * 60;

function json(value) {
  return JSON.stringify(value);
}

async function redisPipeline(commands) {
  if (!REDIS_URL || !REDIS_TOKEN) {
    throw new Error("Redis is not configured");
  }

  const response = await fetch(`${REDIS_URL}/pipeline`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${REDIS_TOKEN}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(commands),
  });

  if (!response.ok) {
    throw new Error(`Redis HTTP ${response.status}`);
  }

  const data = await response.json();
  return data.map((item) => item?.result);
}

async function redis(command, ...args) {
  const [result] = await redisPipeline([[command, ...args]]);
  return result;
}

function validRequest(req) {
  return (
    req.headers["x-pandora-key"] === REGISTRY_KEY &&
    !!REGISTRY_KEY
  );
}

function cleanMarkers(markers, jobId, placeId) {
  const now = Date.now();

  return (Array.isArray(markers) ? markers : []).filter((marker) => {
    return (
      marker &&
      String(marker.jobId) === String(jobId) &&
      Number(marker.placeId) === Number(placeId) &&
      Number(marker.createdAt) >
        now - MARKER_TTL * 1000
    );
  });
}

export default async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader(
    "Access-Control-Allow-Headers",
    "Content-Type, X-Pandora-Key"
  );
  res.setHeader(
    "Access-Control-Allow-Methods",
    "POST, OPTIONS"
  );

  if (req.method === "OPTIONS") {
    return res.status(204).end();
  }

  if (req.method !== "POST") {
    return res.status(405).json({
      error: "POST required",
    });
  }

  if (!validRequest(req)) {
    return res.status(401).json({
      error: "Unauthorized",
    });
  }

  try {
    const body =
      typeof req.body === "string"
        ? JSON.parse(req.body)
        : req.body || {};

    const action = body.action;

    // ==========================================
    // EXECUTOR HEARTBEAT
    // ==========================================

    if (action === "heartbeat") {
      const jobId = String(body.jobId || "");
      const placeId = Number(body.placeId || 0);
      const userId = String(body.userId || "");

      if (!jobId || !userId || !placeId) {
        return res.status(400).json({
          error: "Missing heartbeat fields",
        });
      }

      const key = `pandora:executors:${placeId}:${jobId}`;

      const record = json({
        userId,
        jobId,
        placeId,
        lastSeen: Date.now(),
      });

      await redisPipeline([
        ["HSET", key, userId, record],
        ["EXPIRE", key, HEARTBEAT_TTL],
      ]);

      return res.status(200).json({
        ok: true,
      });
    }

    // ==========================================
    // EXECUTOR LIST
    // ==========================================

    if (action === "list") {
      const jobId = String(body.jobId || "");
      const placeId = Number(body.placeId || 0);

      if (!jobId || !placeId) {
        return res.status(400).json({
          error: "Missing list fields",
        });
      }

      const key = `pandora:executors:${placeId}:${jobId}`;

      const values = await redis("HVALS", key);

      const now = Date.now();
      const users = [];

      for (const raw of Array.isArray(values) ? values : []) {
        try {
          const record = JSON.parse(raw);

          if (
            now - Number(record.lastSeen) <=
            HEARTBEAT_TTL * 1000
          ) {
            users.push(String(record.userId));
          }
        } catch (_) {}
      }

      return res.status(200).json({
        users,
      });
    }

    // ==========================================
    // ADD SHARED MARKER
    // ==========================================

    if (action === "marker_add") {
      const jobId = String(body.jobId || "");
      const placeId = Number(body.placeId || 0);
      const userId = String(body.userId || "");
      const markerName = String(body.markerName || "");
      const position = body.position;

      if (
        !jobId ||
        !placeId ||
        !userId ||
        !markerName ||
        !position ||
        typeof position.x !== "number" ||
        typeof position.y !== "number" ||
        typeof position.z !== "number"
      ) {
        return res.status(400).json({
          error: "Missing marker fields",
        });
      }

      const marker = {
        id: String(
          body.id || `${userId}-${Date.now()}`
        ),

        userId,

        jobId,

        placeId,

        markerName,

        position: {
          x: position.x,
          y: position.y,
          z: position.z,
        },

        createdAt: Date.now(),
      };

      const key =
        `pandora:markers:${placeId}:${jobId}`;

      await redisPipeline([
        ["LPUSH", key, json(marker)],
        ["EXPIRE", key, MARKER_TTL],
      ]);

      return res.status(200).json({
        ok: true,
        marker,
      });
    }

    // ==========================================
    // GET SHARED MARKERS
    // ==========================================

    if (action === "marker_list") {
      const jobId = String(body.jobId || "");
      const placeId = Number(body.placeId || 0);

      if (!jobId || !placeId) {
        return res.status(400).json({
          error: "Missing marker list fields",
        });
      }

      const key =
        `pandora:markers:${placeId}:${jobId}`;

      const rawMarkers = await redis(
        "LRANGE",
        key,
        "0",
        "200"
      );

      const markers = [];

      for (
        const raw of Array.isArray(rawMarkers)
          ? rawMarkers
          : []
      ) {
        try {
          const marker = JSON.parse(raw);

          if (marker) {
            markers.push(marker);
          }
        } catch (_) {}
      }

      return res.status(200).json({
        markers: cleanMarkers(
          markers,
          jobId,
          placeId
        ),
      });
    }

    // ==========================================
    // UNKNOWN ACTION
    // ==========================================

    return res.status(400).json({
      error: "Unknown action",
    });

  } catch (error) {
    return res.status(500).json({
      error: String(
        error?.message || error
      ),
    });
  }
}
