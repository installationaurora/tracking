# Pandora Executor Registry

This Vercel API keeps a short-lived list of Pandora users per Roblox server (`PlaceId + JobId`).
The Roblox executor remains responsible for rendering the indicator; the API only shares UserIds.

## 1. Deploy to Vercel

Create a Vercel project from this folder/repository.

## 2. Add an Upstash Redis database

Connect an Upstash Redis database to the Vercel project and make sure these environment variables are available:

- `UPSTASH_REDIS_REST_URL`
- `UPSTASH_REDIS_REST_TOKEN`

## 3. Add your registry key

Create a strong random value and add this Vercel environment variable:

- `PANDORA_REGISTRY_KEY`

For example, generate a long random string rather than using `CHANGE_ME`.

## 4. Put the deployed endpoint in Pandora

After deployment, the endpoint is:

`https://YOUR-PROJECT.vercel.app/api/pandora`

Set these constants in `PandoraResearchTeam_optimized.lua`:

```lua
local EXECUTOR_REGISTRY_URL = "https://YOUR-PROJECT.vercel.app/api/pandora"
local EXECUTOR_REGISTRY_KEY = "YOUR_PANDORA_REGISTRY_KEY"
```

The key is sent in `X-Pandora-Key`.

## How it works

Each executor sends a heartbeat every 5 seconds. The API stores that UserId in a Redis set for the current Roblox `PlaceId + JobId`. The set expires after 15 seconds without a heartbeat.

When an executor asks for the current server's users, the API returns only the UserIds registered for that exact Roblox server.
