import asyncio
import logging
import os

import httpx

from datetime import datetime, timezone

from utils.db_utils import A_DB


logger = logging.getLogger(__name__)


PRIMARY_URL = (
    f"http://{os.getenv('PRIMARY_BACKEND_HOST')}:"
    f"{os.getenv('PRIMARY_BACKEND_PORT')}/health"
)


# ==========================================================
# HEALTH CONFIGURATION
# ==========================================================

# How often SECONDARY checks PRIMARY /health
CHECK_INTERVAL = 5

# How often an active backend writes its DB heartbeat
SELF_HEALTH_INTERVAL = 30

# PRIMARY heartbeat is considered stale after this duration
PRIMARY_SELF_HEALTH_TIMEOUT = 120

# PRIMARY HTTP health must fail this many consecutive times
MAX_FAILURES = 3


# ==========================================================
# PRIMARY HEARTBEAT CHECK
# ==========================================================

async def is_primary_heartbeat_alive():
    """
    Returns True when PRIMARY has a recent healthy heartbeat.

    PRIMARY heartbeat is considered healthy when:

        status = 'UP'

    AND

        checked_at is within PRIMARY_SELF_HEALTH_TIMEOUT seconds.

    Returns False when:

        - no PRIMARY heartbeat exists
        - latest heartbeat status is not UP
        - latest heartbeat is stale
        - DB query fails
    """

    try:

        async with A_DB() as db:

            query = """
                SELECT
                    status,
                    checked_at
                FROM backend_self_health
                WHERE backend_name = 'PRIMARY'
                ORDER BY checked_at DESC
                LIMIT 1
            """

            row = await db.fetchrow(query)

            # --------------------------------------------------
            # No heartbeat
            # --------------------------------------------------

            if not row:

                logger.warning(
                    "No PRIMARY self-health record found"
                )

                return False

            # --------------------------------------------------
            # Check heartbeat status
            # --------------------------------------------------

            if row["status"] != "UP":

                logger.warning(
                    "PRIMARY heartbeat status is "
                    f"{row['status']}"
                )

                return False

            # --------------------------------------------------
            # Check heartbeat age
            # --------------------------------------------------

            query = """
                SELECT (
                    $1 >=
                    NOW() -
                    ($2 * INTERVAL '1 second')
                ) as alive
            """
            try:

                heartbeat_alive = await db.fetchrow(
                    query,
                    row["checked_at"],
                    PRIMARY_SELF_HEALTH_TIMEOUT
                )

                print(heartbeat_alive)

                if not heartbeat_alive["alive"]:

                    logger.warning(
                        "PRIMARY heartbeat is stale. "
                        f"Last heartbeat: {row['checked_at']}"
                    )

                    return False

                logger.debug(
                    "PRIMARY heartbeat is healthy. "
                    f"Last heartbeat: {row['checked_at']}"
                )

            except Exception as e:
                print(f"error: {e}")
            return True

    except Exception as e:

        logger.error(
            f"Failed to check PRIMARY heartbeat: {e}"
        )

        # --------------------------------------------------
        # IMPORTANT
        #
        # If DB check itself fails, we cannot confirm
        # PRIMARY heartbeat is healthy.
        #
        # But this function returns False only for the
        # heartbeat signal. PRIMARY will only be declared
        # DOWN when HTTP is also DOWN.
        # --------------------------------------------------

        return False


# ==========================================================
# SAVE PRIMARY DOWN
# ==========================================================

async def save_primary_down(down_at):

    try:

        async with A_DB() as db:

            query = """
                INSERT INTO primary_backend_health
                (down_at, status)
                SELECT $1, 'DOWN'
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM primary_backend_health
                    WHERE status = 'DOWN'
                      AND up_at IS NULL
                )
                RETURNING id
            """

            row = await db.fetchrow(
                query,
                down_at
            )

            if row:

                logger.error(
                    f"PRIMARY DOWN recorded at {down_at}"
                )

    except Exception as e:

        logger.error(
            f"Failed to save PRIMARY DOWN status: {e}"
        )


# ==========================================================
# SAVE PRIMARY UP
# ==========================================================

async def save_primary_up(up_at):

    try:

        async with A_DB() as db:

            query = """
                UPDATE primary_backend_health
                SET
                    up_at = $1,
                    duration_seconds =
                        EXTRACT(
                            EPOCH FROM ($1 - down_at)
                        ),
                    status = 'UP'
                WHERE id = (
                    SELECT id
                    FROM primary_backend_health
                    WHERE status = 'DOWN'
                      AND up_at IS NULL
                    ORDER BY down_at DESC
                    LIMIT 1
                )
            """

            result = await db.execute(
                query,
                up_at
            )

            logger.info(
                f"PRIMARY UP recorded at {up_at}. "
                f"Result={result}"
            )

    except Exception as e:

        logger.error(
            f"Failed to save PRIMARY UP status: {e}"
        )


# ==========================================================
# SAVE SELF HEALTH
# ==========================================================

async def save_self_health(status):

    try:

        async with A_DB() as db:

            backend_name = os.getenv(
                "SERVER_ROLE",
                "UNKNOWN"
            ).upper()

            query = """
                INSERT INTO backend_self_health
                (
                    backend_name,
                    status,
                    checked_at
                )
                VALUES ($1, $2, $3)
            """

            await db.execute(
                query,
                backend_name,
                status,
                datetime.now(timezone.utc)
            )

    except Exception as e:

        logger.error(
            f"Failed to save SELF health status: {e}"
        )


# ==========================================================
# SELF HEALTH MONITOR
# ==========================================================

async def self_health_monitor():

    try:

        while True:

            await save_self_health("UP")

            # --------------------------------------------------
            # Heartbeat frequency
            #
            # IMPORTANT:
            # This is 30 seconds, NOT 120 seconds.
            # 120 seconds is the timeout.
            # --------------------------------------------------

            await asyncio.sleep(
                SELF_HEALTH_INTERVAL
            )

    except asyncio.CancelledError:

        logger.info(
            "SELF health monitor stopped"
        )

        raise


# ==========================================================
# PRIMARY HEALTH MONITOR
# ==========================================================

async def start_health_monitor(
    primary_down_event
):

    fail_count = 0
    primary_down = False

    secondary_self_health_task = None

    while True:

        # ==================================================
        # 1. CHECK PRIMARY HTTP HEALTH
        # ==================================================

        server_alive = False

        try:

            async with httpx.AsyncClient(
                timeout=2
            ) as client:

                response = await client.get(
                    PRIMARY_URL
                )

            if response.status_code == 200:

                server_alive = True

                # PRIMARY HTTP is healthy
                fail_count = 0

                logger.debug(
                    "PRIMARY HTTP health: UP"
                )

            else:

                fail_count += 1

                logger.warning(
                    "PRIMARY HTTP health failed "
                    f"({fail_count}/{MAX_FAILURES}) "
                    f"status={response.status_code}"
                )

        except Exception as e:

            fail_count += 1

            logger.warning(
                "Cannot reach PRIMARY "
                f"({fail_count}/{MAX_FAILURES}): {e}"
            )

        # ==================================================
        # 2. DETERMINE HTTP HEALTH
        # ==================================================

        # HTTP is considered DOWN only after
        # MAX_FAILURES consecutive failures.

        http_health_down = (
            fail_count >= MAX_FAILURES
        )

        # If HTTP has not failed 3 consecutive times,
        # HTTP signal is considered healthy.

        http_health_up = not http_health_down

        # ==================================================
        # 3. CHECK PRIMARY DB HEARTBEAT
        # ==================================================

        heartbeat_alive = (
            await is_primary_heartbeat_alive()
        )

        # ==================================================
        # 4. PRIMARY HEALTH DECISION
        # ==================================================
        #
        # PRIMARY is UP if either signal says UP:
        #
        # HTTP UP       -> PRIMARY UP
        # Heartbeat UP  -> PRIMARY UP
        #
        # PRIMARY is DOWN ONLY when:
        #
        # HTTP DOWN
        #     AND
        # Heartbeat DOWN
        #
        # ==================================================

        primary_alive = (
            http_health_up
            or heartbeat_alive
        )

        logger.info(
            "PRIMARY health decision | "
            f"HTTP_UP={http_health_up} | "
            f"HTTP_FAILURES={fail_count} | "
            f"HEARTBEAT_UP={heartbeat_alive} | "
            f"PRIMARY_UP={primary_alive}"
        )

        # ==================================================
        # 5. PRIMARY DOWN
        # ==================================================

        if not primary_alive and not primary_down:

            down_at = datetime.now(timezone.utc)

            logger.error(
                "================================================"
            )

            logger.error(
                "PRIMARY DOWN"
            )

            logger.error(
                "HTTP health: DOWN"
            )

            logger.error(
                f"HTTP failures: {fail_count}"
            )

            logger.error(
                "DB heartbeat: STALE/DOWN"
            )

            logger.error(
                "Both PRIMARY health signals failed"
            )

            logger.error(
                f"PRIMARY DOWN at {down_at}"
            )

            logger.error(
                "================================================"
            )

            await save_primary_down(
                down_at
            )

            primary_down = True

            # --------------------------------------------------
            # Tell failover worker that PRIMARY is DOWN
            # --------------------------------------------------

            primary_down_event.set()

            # --------------------------------------------------
            # SECONDARY becomes active.
            #
            # Start SECONDARY heartbeat only now.
            # --------------------------------------------------

            if secondary_self_health_task is None:

                logger.warning(
                    "Starting SECONDARY self-health monitor"
                )

                secondary_self_health_task = (
                    asyncio.create_task(
                        self_health_monitor()
                    )
                )

        # ==================================================
        # 6. PRIMARY RECOVERED
        # ==================================================

        elif primary_alive and primary_down:

            up_at = datetime.now(timezone.utc)

            logger.warning(
                "================================================"
            )

            logger.warning(
                "PRIMARY BACKEND RECOVERED"
            )

            logger.warning(
                f"PRIMARY UP at {up_at}"
            )

            logger.warning(
                "================================================"
            )

            await save_primary_up(
                up_at
            )

            primary_down = False

            # --------------------------------------------------
            # Tell failover worker PRIMARY is back
            # --------------------------------------------------

            primary_down_event.clear()

            # --------------------------------------------------
            # Stop SECONDARY heartbeat
            # --------------------------------------------------

            if secondary_self_health_task:

                logger.info(
                    "Stopping SECONDARY self-health monitor"
                )

                secondary_self_health_task.cancel()

                try:

                    await secondary_self_health_task

                except asyncio.CancelledError:
                    pass

                secondary_self_health_task = None

            # --------------------------------------------------
            # Reset HTTP failure counter
            # --------------------------------------------------

            fail_count = 0

        # ==================================================
        # 7. WAIT BEFORE NEXT CHECK
        # ==================================================

        await asyncio.sleep(
            CHECK_INTERVAL
        )
