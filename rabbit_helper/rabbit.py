import aio_pika
import json
import asyncio
import inspect
import re
import struct
import traceback
import sys

parser_regex = re.compile(r"parse_(\w+)_(\d+)")


class Rabbit:
    EXCHANGE = ""

    def __init__(self, uri):
        self.uri = uri
        self.connection = None
        self.channel = None
        self.exchange = None

        self.queues = {}
        self.parsers = self.init_parsers()

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
        self.exchange = await self.channel.declare_exchange(self.EXCHANGE)
        await self.create_queues()

    async def create_queues(self):
        queues = {parser.upper() for parser, version in self.parsers.keys()}
        for queue in queues:
            self.queues[queue] = await self.channel.declare_queue(queue)
            await self.queues[queue].bind(self.exchange)

    async def consume(self):
        async def consume_queue(name, queue):
            async with queue.iterator() as queue_iter:
                async for message in queue_iter:
                    async with message.process():
                        body = message.body[1:]
                        version, = struct.unpack_from("!B", message.body)
                        loaded = json.loads(body)
                        parser = self.parsers.get((name, version))
                        if parser:
                            try:
                                await parser(loaded)
                            except:
                                tb = traceback.format_exc()
                                sys.stderr.write(tb)

        await asyncio.gather(*[consume_queue(name.lower(), queue) for name, queue in self.queues.items()])
