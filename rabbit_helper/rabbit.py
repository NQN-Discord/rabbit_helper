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
try:
    from sentry_sdk import capture_exception
except ImportError:
    def capture_exception(exception):
        pass

parser_regex = re.compile(r"parse_(\w+)_(\d+)")


class Rabbit:
    senders = set()

    def __init__(self, uri):
        self.uri = uri
        self.connection = None
        self.channel = None
        self.exchanges = {}

        self.queues = {}
        self.parsers = self.init_parsers()

    @classmethod
    def sender(cls, queue_name, version):
        packed_version = struct.pack("!B", version)

        def outer(func):
            @wraps(func)
            async def inner(self, *args, **kwargs):
                body = func(self, *args, **kwargs)
                await self.exchanges[queue_name].publish(
                    aio_pika.Message(packed_version + bytes(json.dumps(body), encoding="UTF8")),
                    routing_key=queue_name
                )
            return inner
        cls.senders.add(queue_name.upper())
        return outer

    @classmethod
    def receiver(cls, auto_delete: bool = False, **kwargs):
        def outer(func):
            func.kwargs = {
                "auto_delete": auto_delete,
                "arguments": {k.replace("_", "-"): v for k, v in kwargs.items()}
            }
            return func
        return outer

    def init_parsers(self):
        parsers = {}
        for attr, func in inspect.getmembers(self):
            match = parser_regex.match(attr)
            if match is not None:
                name, version = match.groups()
                parsers[(name, int(version))] = func
        return parsers

    async def connect(self) -> Dict[str, int]:
        self.connection = await aio_pika.connect_robust(self.uri)
        self.channel = await self.connection.channel()
        queues = self.senders | {parser.upper() for parser, version in self.parsers.keys()}
        self.exchanges = {queue: await self.channel.declare_exchange(queue, type=aio_pika.ExchangeType.FANOUT) for queue in queues}
        return await self.create_queues()

    async def create_queues(self, passive: bool = False) -> Dict[str, int]:
        queues = {parser.upper(): func for (parser, version), func in self.parsers.items()}
        queue_sizes = {}
        for queue, func in queues.items():
            kwargs = getattr(func, "kwargs", {})
            self.queues[queue] = created_queue = await self.channel.declare_queue(
                f"{queue}_{self.__class__.__name__}",
                passive=passive,
                **kwargs
            )
            await created_queue.bind(self.exchanges[queue])
            queue_sizes[queue] = created_queue.declaration_result.message_count
        return queue_sizes

    async def fetch_queue_sizes(self) -> Dict[str, int]:
        return await self.create_queues(passive=True)

    async def consume(self):
        await asyncio.gather(*[self.consume_queue(name.lower(), queue) for name, queue in self.queues.items()])

    async def consume_queue(self, name, queue, parser_caller=lambda parser, loaded: parser(loaded)):
        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    body = message.body[1:]
                    version, = struct.unpack_from("!B", message.body)
                    loaded = json.loads(body)
                    parser = self.parsers.get((name, version))
                    if parser:
                        try:
                            await parser_caller(parser, loaded)
                        except Exception as e:
                            tb = traceback.format_exc()
                            sys.stderr.write(tb)
                            capture_exception(e)
