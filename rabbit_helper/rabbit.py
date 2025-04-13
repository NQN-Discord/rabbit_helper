import aio_pika
import json
import asyncio
import inspect
import re
import struct
import traceback
import sys
from functools import wraps
from typing import Dict
from logging import getLogger

from prometheus_client import Counter

try:
    import orjson
    json_dumps = orjson.dumps
    json_loads = orjson.loads
except ImportError:
    json_dumps = lambda body: bytes(json.dumps(body), encoding="UTF8")
    json_loads = json.loads

try:
    from sentry_sdk import capture_exception
except ImportError:
    def capture_exception(exception):
        pass

parser_regex = re.compile(r"parse_(\w+)_(\d+)")
log = getLogger(__name__)

if True:
    events = Counter(
        "queue_events",
        "Events",
        labelnames=["queue"],
        namespace="rabbit",
    )

    queue_binds = Counter(
        "queue_binds",
        "Direct queues dynamically bound",
        labelnames=["queue"],
        namespace="rabbit",
    )

    queue_unbinds = Counter(
        "queue_unbinds",
        "Direct queues dynamically unbound",
        labelnames=["queue"],
        namespace="rabbit",
    )


class Rabbit:
    senders = set()
    _direct_senders = set()
    BODY = "_BODY_"

    def __init__(self, uri, connection_id: str = ""):
        self.uri = uri
        self.connection = None
        self.channel = None
        self.exchanges = {}
        self.connection_id = f"_{connection_id}" if connection_id else ""

        self.queues = {}
        self.parsers, self.reverse_parsers = self.init_parsers()

    @classmethod
    def sender(cls, queue_name, version, *, routing_key: str = None, direct: bool = False):
        packed_version = struct.pack("!B", version)

        def outer(func):
            if routing_key == cls.BODY:
                @wraps(func)
                async def inner(self, *args, **kwargs):
                    body = func(self, *args, **kwargs)
                    await self.exchanges[queue_name].publish(
                        aio_pika.Message(packed_version + json_dumps(body)),
                        routing_key=str(body)
                    )
            elif routing_key is not None:
                @wraps(func)
                async def inner(self, *args, **kwargs):
                    body = func(self, *args, **kwargs)
                    await self.exchanges[queue_name].publish(
                        aio_pika.Message(packed_version + json_dumps(body)),
                        routing_key=body[routing_key]
                    )
            else:
                @wraps(func)
                async def inner(self, *args, **kwargs):
                    body = func(self, *args, **kwargs)
                    await self.exchanges[queue_name].publish(
                        aio_pika.Message(packed_version + json_dumps(body)),
                        routing_key=""
                    )
            return inner
        cls.senders.add(queue_name.upper())
        if direct:
            cls._direct_senders.add(queue_name.upper())
        return outer

    @classmethod
    def receiver(cls, auto_delete: bool = False, auto_shard: bool = False, direct: bool = False, **kwargs):
        def outer(func):
            func.kwargs = {
                "auto_delete": auto_delete,
                "auto_shard": auto_shard,
                "direct": direct,
                "arguments": {k.replace("_", "-"): v for k, v in kwargs.items()}
            }
            return func
        return outer

    def init_parsers(self):
        parsers = {}
        reverse_parsers = {}
        for attr, func in inspect.getmembers(self):
            match = parser_regex.match(attr)
            if match is not None:
                name, version = match.groups()
                parsers[(name, int(version))] = func
                reverse_parsers[func] = name, int(version)
        return parsers, reverse_parsers

    async def connect(self):
        self.connection = await aio_pika.connect_robust(self.uri)
        self.channel = await self.connection.channel()
        all_queues = self.senders | {parser.upper() for parser, version in self.parsers.keys()}
        shard_queues = (
            parser.upper()
            for (parser, version), func in self.parsers.items()
            if getattr(func, "kwargs", {}).get("auto_shard", False)
        )
        direct_queues = self._direct_senders | {
            parser.upper()
            for (parser, version), func in self.parsers.items()
            if getattr(func, "kwargs", {}).get("direct", False)
        }

        self.exchanges = {}
        for queue in all_queues:
            queue_type = aio_pika.ExchangeType.DIRECT if queue in direct_queues else aio_pika.ExchangeType.FANOUT
            self.exchanges[queue] = await self.channel.declare_exchange(queue, type=queue_type)
        for queue in shard_queues:
            dest = f"{queue}-AUTOSHARDED-{self.__class__.__name__}"
            self.exchanges[f"{queue}-SHARD"] = await self.channel.declare_exchange(dest, type=aio_pika.ExchangeType.X_CONSISTENT_HASH)
            await self.channel.channel.exchange_bind(
                destination=dest,
                source=queue,
            )

        return await self.create_queues()

    async def create_queues(self, passive: bool = False) -> Dict[str, int]:
        queues = {parser.upper(): func for (parser, version), func in self.parsers.items()}
        queue_sizes = {}
        for queue, func in queues.items():
            kwargs = getattr(func, "kwargs", {})
            auto_shard = kwargs.pop("auto_shard", False)
            direct = kwargs.pop("direct", False)
            self.queues[queue] = created_queue = await self.channel.declare_queue(
                f"{queue}_{self.__class__.__name__}{self.connection_id}",
                passive=passive,
                **kwargs
            )
            queue_sizes[queue] = created_queue.declaration_result.message_count
            if direct:
                # Direct queues have their bindings generated dynamically
                pass
            elif auto_shard:
                await created_queue.bind(self.exchanges[f"{queue}-SHARD"], routing_key="1")
            else:
                await created_queue.bind(self.exchanges[queue])
        return queue_sizes
    
    async def dynamic_create_queue(self, func):
        match = parser_regex.match(func.__name__)
        name, version = match.groups()
        self.parsers[(name, int(version))] = func
        self.reverse_parsers[func] = name, int(version)
        queue = name.upper()
        self.exchanges[queue] = await self.channel.declare_exchange(queue, type=aio_pika.ExchangeType.FANOUT)
        created_queue = await self.channel.declare_queue(
            f"{queue}_{self.__class__.__name__}{self.connection_id}",
            passive=False
        )
        await created_queue.bind(self.exchanges[queue])
        await self.consume_queue(name, created_queue)

    async def fetch_queue_sizes(self) -> Dict[str, int]:
        return await self.create_queues(passive=True)

    async def bind_direct_queue(self, func, routing_key: str):
        queue_name: str = self.reverse_parsers[func][0].upper()
        queue_binds.labels(queue=queue_name).inc()
        await self.queues[queue_name].bind(self.exchanges[queue_name], routing_key=routing_key)

    async def unbind_direct_queue(self, func, routing_key: str):
        queue_name: str = self.reverse_parsers[func][0].upper()
        queue_unbinds.labels(queue=queue_name).inc()
        await self.queues[queue_name].unbind(self.exchanges[queue_name], routing_key=routing_key)

    async def consume(self):
        await asyncio.gather(*[self.consume_queue(name.lower(), queue) for name, queue in self.queues.items()])

    async def consume_queue(self, name, queue, parser_caller=lambda parser, loaded: parser(loaded)):
        queue_metric_inc = events.labels(queue=name).inc
        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    queue_metric_inc()
                    await self._process_message(message, name, parser_caller)

    async def _process_message(self, message: aio_pika.IncomingMessage, queue_name: str, parser_caller):
        body = message.body[1:]
        version, = struct.unpack_from("!B", message.body)
        loaded = json_loads(body)
        parser = self.parsers.get((queue_name, version))
        if parser:
            try:
                await parser_caller(parser, loaded)
            except Exception as e:
                tb = traceback.format_exc()
                sys.stderr.write(tb)
                capture_exception(e)
