const REGISTRY_TTL_SECONDS = 15;

function json(status, body) {
  return {
    status,
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  };
}

async function redis(command, args = []) {
  const base = process.env.UPSTASH_REDIS_REST_URL;
  const token = process.env.UPSTASH_REDIS_REST_TOKEN;

  if (!base || !token) {
    throw new Error("Missing Upstash Redis environment variables");
  }

  const path = [command, ...args].map((part) => encodeURIComponent(String(part))).join("/");

  const response = await fetch(`${base}/${path}`, {
    headers: {
      Authorization: `Bearer ${token}`,
    },
  });

  if (!response.ok) {
    throw new Error(`Redis request failed: ${response.status}`);
  }

  return response.json();
}

function registryKey(placeId, jobId) {
  return `pandora:executors:${placeId}:${jobId}`;
}

function validId(value) {
  return typeof value === "string" || typeof value === "number";
}

export default async function handler(req, res) {
  res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate");

  if (req.method !== "POST") {
    return res.status(405).json({ error: "POST required" });
  }

  const suppliedKey = req.headers["x-pandora-key"];
  if (!suppliedKey || suppliedKey !== process.env.PANDORA_REGISTRY_KEY) {
    return res.status(401).json({ error: "Unauthorized" });
  }

  const body = req.body || {};
  const { action, userId, jobId, placeId } = body;

  if (!validId(jobId) || !validId(placeId)) {
    return res.status(400).json({ error: "Missing placeId or jobId" });
  }

  const key = registryKey(placeId, jobId);

  try {
    if (action === "heartbeat") {
      if (!validId(userId)) {
        return res.status(400).json({ error: "Missing userId" });
      }

      await redis("sadd", [key, String(userId)]);
      await redis("expire", [key, REGISTRY_TTL_SECONDS]);

      return res.status(200).json({ ok: true });
    }

    if (action === "list") {
      const result = await redis("smembers", [key]);
      return res.status(200).json({
        users: Array.isArray(result.result) ? result.result : [],
      });
    }

    return res.status(400).json({ error: "Unknown action" });
  } catch (error) {
    console.error("Pandora registry error:", error);
    return res.status(500).json({ error: "Registry unavailable" });
  }
}
