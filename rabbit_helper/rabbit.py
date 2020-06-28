import aio_pika
import json
import asyncio
import inspect
import re
import struct
import traceback
import sys
from functools import wraps
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

    def init_parsers(self):
        parsers = {}
        for attr, func in inspect.getmembers(self):
            match = parser_regex.match(attr)
            if match is not None:
                name, version = match.groups()
                parsers[(name, int(version))] = func
        return parsers

    async def connect(self):
        self.connection = await aio_pika.connect_robust(self.uri)
        self.channel = await self.connection.channel()
        queues = self.senders | {parser.upper() for parser, version in self.parsers.keys()}
        self.exchanges = {queue: await self.channel.declare_exchange(queue, type=aio_pika.ExchangeType.FANOUT) for queue in queues}
        await self.create_queues()

    async def create_queues(self):
        queues = {parser.upper() for parser, version in self.parsers.keys()}
        for queue in queues:
            self.queues[queue] = created_queue = await self.channel.declare_queue(f"{queue}_{self.__class__.__name__}")
            await created_queue.bind(self.exchanges[queue])

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
