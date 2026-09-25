const REGISTRY_KEY = process.env.PANDORA_REGISTRY_KEY;
const REDIS_URL = process.env.UPSTASH_REDIS_REST_URL;
const REDIS_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;

const HEARTBEAT_TTL = 15;
const MARKER_TTL = 30;
const MAX_MARKERS_PER_USER = 5;
const PLAYER_TARGET_TTL = 5 * 60;

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
  const [result] = await redisPipeline([
    [command, ...args],
  ]);

  return result;
}

function validRequest(req) {
  return (
    !!REGISTRY_KEY &&
    req.headers["x-pandora-key"] === REGISTRY_KEY
  );
}

function cleanMarkers(markers, jobId, placeId) {
  const now = Date.now();

  return (Array.isArray(markers) ? markers : []).filter(
    (marker) => {
      if (!marker) return false;

      if (
        String(marker.jobId) !== String(jobId)
      ) {
        return false;
      }

      if (
        Number(marker.placeId) !== Number(placeId)
      ) {
        return false;
      }

      if (
        now - Number(marker.createdAt) >
        MARKER_TTL * 1000
      ) {
        return false;
      }

      return true;
    }
  );
}

function cleanPlayerTargets(targets, jobId, placeId) {
  const now = Date.now();

  return (
    Array.isArray(targets) ? targets : []
  ).filter((target) => {
    if (!target) return false;

    if (
      String(target.jobId) !== String(jobId)
    ) {
      return false;
    }

    if (
      Number(target.placeId) !== Number(placeId)
    ) {
      return false;
    }

    if (
      now - Number(target.createdAt) >
      PLAYER_TARGET_TTL * 1000
    ) {
      return false;
    }

    if (!target.sourceUserId) {
      return false;
    }

    if (!target.targetUserId) {
      return false;
    }

    return true;
  });
}

export default async function handler(req, res) {
  res.setHeader(
    "Access-Control-Allow-Origin",
    "*"
  );

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

    /*
     * ============================================================
     * HEARTBEAT
     * ============================================================
     */

    if (action === "heartbeat") {
      const jobId = String(
        body.jobId || ""
      );

      const placeId = Number(
        body.placeId || 0
      );

      const userId = String(
        body.userId || ""
      );

      if (
        !jobId ||
        !userId ||
        !placeId
      ) {
        return res.status(400).json({
          error:
            "Missing heartbeat fields",
        });
      }

      const key =
        `pandora:executors:${placeId}:${jobId}`;

      const record = json({
        userId,
        jobId,
        placeId,
        lastSeen: Date.now(),
      });

      await redisPipeline([
        [
          "HSET",
          key,
          userId,
          record,
        ],
        [
          "EXPIRE",
          key,
          HEARTBEAT_TTL,
        ],
      ]);

      return res.status(200).json({
        ok: true,
      });
    }

    /*
     * ============================================================
     * EXECUTOR LIST
     * ============================================================
     */

    if (action === "list") {
      const jobId = String(
        body.jobId || ""
      );

      const placeId = Number(
        body.placeId || 0
      );

      if (
        !jobId ||
        !placeId
      ) {
        return res.status(400).json({
          error:
            "Missing list fields",
        });
      }

      const key =
        `pandora:executors:${placeId}:${jobId}`;

      const values = await redis(
        "HVALS",
        key
      );

      const now = Date.now();
      const users = [];

      for (
        const raw of Array.isArray(values)
          ? values
          : []
      ) {
        try {
          const record =
            JSON.parse(raw);

          if (
            now -
              Number(record.lastSeen) <=
            HEARTBEAT_TTL * 1000
          ) {
            users.push(
              String(record.userId)
            );
          }
        } catch (_) {}
      }

      return res.status(200).json({
        users,
      });
    }

    /*
     * ============================================================
     * MARKER ADD
     * ============================================================
     */

    if (action === "marker_add") {
      const jobId = String(
        body.jobId || ""
      );

      const placeId = Number(
        body.placeId || 0
      );

      const userId = String(
        body.userId || ""
      );

      const markerName = String(
        body.markerName || ""
      );

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
          error:
            "Missing marker fields",
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

      const now = Date.now();
      const existingMarkers = [];

      for (
        const raw of Array.isArray(
          rawMarkers
        )
          ? rawMarkers
          : []
      ) {
        try {
          const marker =
            JSON.parse(raw);

          if (!marker) {
            continue;
          }

          if (
            now -
              Number(marker.createdAt) >
            MARKER_TTL * 1000
          ) {
            continue;
          }

          if (
            String(marker.jobId) !==
              String(jobId) ||
            Number(marker.placeId) !==
              Number(placeId)
          ) {
            continue;
          }

          existingMarkers.push(
            marker
          );
        } catch (_) {}
      }

      const userMarkers =
        existingMarkers.filter(
          (marker) =>
            String(
              marker.userId
            ) === String(userId)
        );

      /*
       * Each user may have a maximum
       * of five active markers.
       *
       * If they place a sixth,
       * remove their oldest marker.
       */

      if (
        userMarkers.length >=
        MAX_MARKERS_PER_USER
      ) {
        userMarkers.sort(
          (a, b) =>
            Number(a.createdAt) -
            Number(b.createdAt)
        );

        const oldest =
          userMarkers[0];

        await redis(
          "LREM",
          key,
          "1",
          json(oldest)
        );
      }

      const marker = {
        id: String(
          body.id ||
            `${userId}-${Date.now()}`
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

      await redisPipeline([
        [
          "LPUSH",
          key,
          json(marker),
        ],

        [
          "EXPIRE",
          key,
          MARKER_TTL,
        ],
      ]);

      return res.status(200).json({
        ok: true,
        marker,
      });
    }

    /*
     * ============================================================
     * MARKER LIST
     * ============================================================
     */

    if (action === "marker_list") {
      const jobId = String(
        body.jobId || ""
      );

      const placeId = Number(
        body.placeId || 0
      );

      if (
        !jobId ||
        !placeId
      ) {
        return res.status(400).json({
          error:
            "Missing marker list fields",
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
        const raw of Array.isArray(
          rawMarkers
        )
          ? rawMarkers
          : []
      ) {
        try {
          const marker =
            JSON.parse(raw);

          if (marker) {
            markers.push(marker);
          }
        } catch (_) {}
      }

      const cleaned =
        cleanMarkers(
          markers,
          jobId,
          placeId
        );

      return res.status(200).json({
        markers: cleaned,
      });
    }

    /*
     * ============================================================
     * PLAYER TARGET
     *
     * One target per Pandora user.
     *
     * Targets automatically expire after
     * five minutes.
     * ============================================================
     */

    if (action === "player_target") {
      const jobId = String(
        body.jobId || ""
      );

      const placeId = Number(
        body.placeId || 0
      );

      const sourceUserId = String(
        body.sourceUserId || ""
      );

      const targetUserId = String(
        body.targetUserId || ""
      );

      if (
        !jobId ||
        !placeId ||
        !sourceUserId ||
        !targetUserId
      ) {
        return res.status(400).json({
          error:
            "Missing player target fields",
        });
      }

      if (
        sourceUserId ===
        targetUserId
      ) {
        return res.status(400).json({
          error:
            "Cannot target yourself",
        });
      }

      const key =
        `pandora:player_targets:${placeId}:${jobId}`;

      const rawTargets = await redis(
        "LRANGE",
        key,
        "0",
        "200"
      );

      const now = Date.now();

      const existingTargets = [];

      for (
        const raw of Array.isArray(
          rawTargets
        )
          ? rawTargets
          : []
      ) {
        try {
          const target =
            JSON.parse(raw);

          if (!target) {
            continue;
          }

          if (
            now -
              Number(target.createdAt) >
            PLAYER_TARGET_TTL * 1000
          ) {
            continue;
          }

          if (
            String(target.jobId) !==
              String(jobId) ||
            Number(target.placeId) !==
              Number(placeId)
          ) {
            continue;
          }

          existingTargets.push(
            target
          );
        } catch (_) {}
      }

      /*
       * Remove any previous target
       * belonging to this Pandora user.
       */

      const oldTargets =
        existingTargets.filter(
          (target) =>
            String(
              target.sourceUserId
            ) ===
            String(sourceUserId)
        );

      const newTarget = {
        id: String(
          body.id ||
            `${sourceUserId}-${Date.now()}`
        ),

        sourceUserId,

        targetUserId,

        jobId,

        placeId,

        createdAt: Date.now(),
      };

      /*
       * Remove previous target(s)
       * for this source user.
       */

      const commands = [];

      for (
        const oldTarget of oldTargets
      ) {
        commands.push([
          "LREM",
          key,
          "1",
          json(oldTarget),
        ]);
      }

      /*
       * Add the new target.
       */

      commands.push([
        "LPUSH",
        key,
        json(newTarget),
      ]);

      commands.push([
        "EXPIRE",
        key,
        PLAYER_TARGET_TTL,
      ]);

      await redisPipeline(
        commands
      );

      return res.status(200).json({
        ok: true,
        target: newTarget,
      });
    }

    /*
     * ============================================================
     * PLAYER TARGET LIST
     * ============================================================
     */

    if (
      action ===
      "player_target_list"
    ) {
      const jobId = String(
        body.jobId || ""
      );

      const placeId = Number(
        body.placeId || 0
      );

      if (
        !jobId ||
        !placeId
      ) {
        return res.status(400).json({
          error:
            "Missing player target list fields",
        });
      }

      const key =
        `pandora:player_targets:${placeId}:${jobId}`;

      const rawTargets = await redis(
        "LRANGE",
        key,
        "0",
        "200"
      );

      const targets = [];

      for (
        const raw of Array.isArray(
          rawTargets
        )
          ? rawTargets
          : []
      ) {
        try {
          const target =
            JSON.parse(raw);

          if (target) {
            targets.push(
              target
            );
          }
        } catch (_) {}
      }

      const cleaned =
        cleanPlayerTargets(
          targets,
          jobId,
          placeId
        );

      /*
       * Remove expired targets
       * from Redis when possible.
       */

      const expiredTargets =
        targets.filter(
          (target) =>
            !cleaned.some(
              (active) =>
                String(
                  active.id
                ) ===
                String(target.id)
            )
        );

      if (
        expiredTargets.length > 0
      ) {
        const cleanupCommands =
          [];

        for (
          const expired of
            expiredTargets
        ) {
          cleanupCommands.push([
            "LREM",
            key,
            "1",
            json(expired),
          ]);
        }

        if (
          cleanupCommands.length
        ) {
          await redisPipeline(
            cleanupCommands
          );
        }
      }

      return res.status(200).json({
        targets: cleaned,
      });
    }

    /*
     * ============================================================
     * UNKNOWN ACTION
     * ============================================================
     */

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
