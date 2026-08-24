"""Channel-to-run bindings for ARI, shared by the API and the manager process.

The ARI event listener runs as its own process, so the only thing it shares
with the API that originates calls is Redis. Both ends need the same key
format, and both need to write it: the manager binds a channel when it enters
Stasis, but a call that is never answered never gets there -- and a dead
channel nobody can trace back to a run is a run that stays open forever.
"""

from typing import Optional

import redis.asyncio as aioredis

from api.constants import REDIS_URL

CHANNEL_KEY_PREFIX = "ari:channel:"
EXT_CHANNEL_KEY_PREFIX = "ari:ext_channel:"
PENDING_BRIDGE_PREFIX = "ari:pending_bridge:"
CHANNEL_KEY_TTL = 3600  # 1 hour safety expiry


async def bind_channel_to_run(channel_id: str, workflow_run_id: str) -> None:
    """Bind a freshly originated channel to its run, before Stasis.

    Called at origination so an unanswered call can still be traced back to
    its run when the channel is destroyed. Never raises: failing to write the
    hint must not take down a call that is otherwise fine.
    """
    if not channel_id or not workflow_run_id:
        return

    client: Optional[aioredis.Redis] = None
    try:
        client = aioredis.from_url(REDIS_URL, decode_responses=True)
        await client.set(
            f"{CHANNEL_KEY_PREFIX}{channel_id}",
            str(workflow_run_id),
            ex=CHANNEL_KEY_TTL,
        )
    except Exception:  # noqa: BLE001 - best effort, see docstring
        pass
    finally:
        if client is not None:
            try:
                await client.aclose()
            except Exception:  # noqa: BLE001 - closing must not raise either
                pass
