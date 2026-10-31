import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from collections import defaultdict

from plant_architecture_resolver import PlantArchitectureResolver
from utils.db_write_queue import DB_WRITE_QUEUE, VICTORIA_DB_WRITE_QUEUE

logger = logging.getLogger(__name__)


@dataclass
class AggState:
    sum: float = 0.0
    count: int = 0
    last: float = None
    aggregation: str = None
    processed: int = 0
    sample_count: int = 0


class ResolutionSampling:

    def __init__(self):
        # device -> resolution -> tag -> [(value, aggregation)]
        self.raw_buffers = defaultdict(
            lambda: defaultdict(
                lambda: defaultdict(list)
            )
        )

        # device -> resolution -> tag -> AggState
        self.agg_states = defaultdict(
            lambda: defaultdict(dict)
        )

        self.tag_lower_cache = {}

        self.flush_lock = asyncio.Lock()

        self.device_resolver = PlantArchitectureResolver()

        asyncio.create_task(self.rtm_clock())

    # -------------------------------------------------
    # CLOCK
    # 6,12,18,24,30,36,42,48,54 -> partial aggregation
    # 58 -> final flush
    # -------------------------------------------------
    async def rtm_clock(self):

        partial_ticks = {6, 12, 18, 24, 30, 36, 42, 48, 54}

        while True:

            try:

                now = datetime.now()

                sec = now.second

                final_run = True

                future = sorted([x for x in partial_ticks if x > sec] + [58])

                if future:
                    next_tick = future[0]
                    next_run = now.replace(second=next_tick, microsecond=0)
                else:
                    next_run = (
                        now.replace(second=6, microsecond=0)
                        + timedelta(minutes=1)
                    )

                await asyncio.sleep((next_run - now).total_seconds())

                if next_run.second == 58 and final_run:
                    final_run = False
                    await self.final_flush()
                    
                    await asyncio.sleep(1)
                if next_run.second == 0 or next_run.second == 59:
                    
                    await asyncio.sleep(1)
                else:
                    final_run = True
                    await self.partial_flush_all()
            except Exception as e:
                logger.exception(e)
            except asyncio.CancelledError:
                logger.info("Worker stopping...")
                raise

    # -------------------------------------------------
    # INGEST
    # -------------------------------------------------
    async def add_data(
        self,
        device_id,
        tag,
        value,
        timestamp,
        resolution,
        aggregation,
    ):

        if tag not in self.tag_lower_cache:
            self.tag_lower_cache[tag] = tag.lower()

        tag = self.tag_lower_cache[tag]

        self.raw_buffers[device_id][resolution][tag].append(
            (value, aggregation)
        )

    # -------------------------------------------------
    # PARTIAL AGGREGATION
    # -------------------------------------------------
    async def partial_flush_all(self):

        async with self.flush_lock:

            for device_id, resolution_dict in self.raw_buffers.items():

                agg_resolution = self.agg_states[device_id]

                for resolution, tags in resolution_dict.items():

                    agg_tags = agg_resolution[resolution]

                    for tag, points in tags.items():

                        if not points:
                            continue

                        state = agg_tags.get(tag)

                        if state is None:
                            state = AggState(
                                aggregation=points[-1][1]
                            )
                            agg_tags[tag] = state

                        start = state.processed

                        if start >= len(points):
                            continue

                        for value, agg in points[start:]:

                            if value is None:
                                continue

                            state.sample_count += 1

                            if agg == "avg":
                                state.sum += value
                                state.count += 1

                            elif agg == "last":
                                state.last = value

                        state.processed = len(points)

    # -------------------------------------------------
    # FINAL FLUSH
    # -------------------------------------------------
    async def final_flush(self):

        async with self.flush_lock:

            start_time = time.perf_counter()

            try:

                #
                # Process last 4 seconds
                #
                for device_id, resolution_dict in self.raw_buffers.items():

                    agg_resolution = self.agg_states[device_id]

                    for resolution, tags in resolution_dict.items():

                        agg_tags = agg_resolution[resolution]

                        for tag, points in tags.items():

                            if not points:
                                continue

                            state = agg_tags.get(tag)

                            if state is None:
                                state = AggState(
                                    aggregation=points[-1][1]
                                )
                                agg_tags[tag] = state

                            start = state.processed

                            for value, agg in points[start:]:

                                if value is None:
                                    continue

                                state.sample_count += 1

                                if agg == "avg":
                                    state.sum += value
                                    state.count += 1

                                elif agg == "last":
                                    state.last = value

                            state.processed = len(points)

                #
                # Build batch
                #
                aligned = (
                    datetime.now().replace(second=0, microsecond=0)
                    + timedelta(minutes=1)
                )

                ts = aligned.strftime("%Y-%m-%d %H:%M:%S")

                batch = []

                total_devices = 0
                total_tags = 0
                total_samples = 0

                for device_id, resolution_dict in self.agg_states.items():

                    row = {}

                    has_valid = False

                    tag_count = 0
                    sample_count = 0

                    for resolution, tags in resolution_dict.items():

                        for tag, state in tags.items():

                            tag_count += 1
                            sample_count += state.sample_count

                            if state.aggregation == "avg":

                                value = (
                                    round(
                                        state.sum / state.count,
                                        2,
                                    )
                                    if state.count
                                    else None
                                )

                            elif state.aggregation == "last":

                                value = state.last

                            else:

                                value = None

                            if value is not None:
                                has_valid = True

                            row[tag] = value

                    if not row:
                        continue

                    total_devices += 1
                    total_tags += tag_count
                    total_samples += sample_count

                    batch.append(
                        (
                            device_id,
                            {
                                "time_stamp": ts,
                                "app_status": 200 if has_valid else 500,
                                **row,
                            },
                        )
                    )

                if batch:
                    # await DB_WRITE_QUEUE.put(batch)
                    # Historical output: the Victoria worker consumes this queue.
                    # Queueing a batch is not confirmation of a database insert.
                    await VICTORIA_DB_WRITE_QUEUE.put(batch)

                    # SECOND CACHE ENTRY POINT (after sampling): store latest avg/last
                    # values via update_block_devices -> update_device_cache -> merge.
                    # JSON uses sampled_telemetry/<device_id>.json; Valkey uses
                    # <prefix>:sampled:<device_id>. History above is independent.
                    # The legacy name does not mean rewriting plant metadata JSON.
                    await self.device_resolver.update_block_devices(batch=batch)
                    # self.device_resolver.update_device_files(
                    #     batch=batch
                    # )

                #
                # Reset for next minute
                #
                self.raw_buffers.clear()
                self.agg_states.clear()

                duration = (
                    time.perf_counter() - start_time
                ) * 1000

                logger.info(
                    "RTM FINAL | devices=%d tags=%d samples=%d duration=%.2fms",
                    total_devices,
                    total_tags,
                    total_samples,
                    duration,
                )

            except Exception:
                logger.exception("RTM final flush failed")