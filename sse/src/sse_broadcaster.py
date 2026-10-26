import asyncio
import json
import logging
from collections import defaultdict

logger = logging.getLogger(__name__)


class SSEBroadcaster:
    def __init__(
        self,
        batch_window=2.0,
        max_queue_size=1,
    ):
        self.batch_window = batch_window
        self.max_queue_size = max_queue_size

        # topic -> set(queue)
        self.listeners = defaultdict(set)

        # topic -> latest successfully built payload
        self.buffer = {}

        # topic -> latest state received in current batch
        self.pending = {}

        # topic -> batch task
        self.batch_tasks = {}

        self.lock = asyncio.Lock()

        # Incoming publish requests are queued here.
        self.publish_queue = asyncio.Queue(
            maxsize=10000
        )

        self.publish_worker_task = None

    async def register(self, topic):
        queue = asyncio.Queue(
            maxsize=self.max_queue_size
        )

        async with self.lock:
            self.listeners[topic].add(queue)

            # Latest state already available.
            buffered = self.buffer.get(topic)

            logger.info(
                "Registered listener topic=%s listeners=%d",
                topic,
                len(self.listeners[topic]),
            )

        # Send latest state to new subscriber.
        if buffered is not None:
            await self._send_latest(
                queue,
                buffered,
                topic,
            )

        return queue

    async def broadcast(self, topic, data):
        """
        Store only the latest state for this topic.

        The timer starts with the FIRST packet.

        Example:

            0.0s -> A  starts batch
            0.1s -> B  replaces A
            0.5s -> C  replaces B
            1.9s -> D  replaces C
            2.0s -> FLUSH D

        Only D is sent.
        """

        async with self.lock:

            # Start a new batch.
            if topic not in self.pending:

                self.pending[topic] = data

                logger.debug(
                    "Started new batch topic=%s",
                    topic,
                )

                # Timer is created ONLY for first packet.
                task = asyncio.create_task(
                    self._flush_after_window(topic),
                    name=f"SSE-Batch-{topic}",
                )

                self.batch_tasks[topic] = task

            else:

                # Latest state wins.
                self.pending[topic] = data

                logger.debug(
                    "Replaced pending state topic=%s",
                    topic,
                )

    async def _flush_after_window(self, topic):
        """
        Wait a fixed amount of time and flush.

        This is NOT a debounce timer.
        New packets do NOT reset the timer.
        """

        try:
            await asyncio.sleep(self.batch_window)

            async with self.lock:

                # Get latest state.
                data = self.pending.pop(
                    topic,
                    None,
                )

                current_task = asyncio.current_task()

                if self.batch_tasks.get(topic) is current_task:
                    self.batch_tasks.pop(topic, None)

                listeners = list(
                    self.listeners.get(topic, set())
                )

                if data is not None:
                    # Save latest state as buffer.
                    self.buffer[topic] = data

            if data is None:
                return

            message = self._format_sse(data)

            logger.info(
                "Flushing latest state topic=%s listeners=%d",
                topic,
                len(listeners),
            )

            if not listeners:
                return

            await asyncio.gather(
                *[
                    self._send_to_queue(
                        queue,
                        message,
                        topic,
                    )
                    for queue in listeners
                ],
                return_exceptions=True,
            )

        except asyncio.CancelledError:
            logger.debug(
                "Batch task cancelled topic=%s",
                topic,
            )
            raise

        except Exception:
            logger.exception(
                "Error flushing topic=%s",
                topic,
            )

    def _format_sse(self, data):
        return (
            "data: "
            + json.dumps(
                data,
                separators=(",", ":"),
            )
            + "\n\n"
        )

    async def _send_to_queue(
        self,
        queue,
        message,
        topic,
    ):
        try:

            # Queue size = 1:
            # keep only newest message.
            if queue.full():

                try:
                    queue.get_nowait()

                    logger.debug(
                        "Dropped stale SSE message topic=%s",
                        topic,
                    )

                except asyncio.QueueEmpty:
                    pass

            queue.put_nowait(message)

        except asyncio.QueueFull:

            logger.warning(
                "Queue full, removing listener topic=%s",
                topic,
            )

            await self.unregister(
                topic,
                queue,
            )

        except Exception:

            logger.exception(
                "Failed sending SSE topic=%s",
                topic,
            )

            await self.unregister(
                topic,
                queue,
            )

    async def _send_latest(
        self,
        queue,
        data,
        topic,
    ):
        try:

            message = self._format_sse(data)

            queue.put_nowait(message)

        except asyncio.QueueFull:

            logger.warning(
                "Could not send buffered message topic=%s",
                topic,
            )

    async def unregister(
        self,
        topic,
        queue,
    ):
        async with self.lock:

            listeners = self.listeners.get(topic)

            if not listeners:
                return

            listeners.discard(queue)

            logger.info(
                "Unregistered listener topic=%s remaining=%d",
                topic,
                len(listeners),
            )

            if listeners:
                return

            del self.listeners[topic]

            # Keep pending state alive.
            # The current batch can still finish.
            #
            # Keep buffer as well so a future subscriber
            # receives the latest known state.

    async def start_cleanup_task(self):
        async def cleanup():

            while True:

                await asyncio.sleep(5)

                async with self.lock:

                    for topic in list(
                        self.listeners.keys()
                    ):

                        listeners = self.listeners[topic]

                        for queue in list(listeners):

                            if queue.qsize() >= self.max_queue_size:

                                logger.warning(
                                    "Removing stale queue topic=%s",
                                    topic,
                                )

                                listeners.discard(queue)

                        if not listeners:

                            del self.listeners[topic]

        asyncio.create_task(
            cleanup(),
            name="SSE-Cleanup",
        )
