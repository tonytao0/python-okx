import asyncio
import json
import logging
import time

from okx.websocket.WebSocketFactory import WebSocketFactory
from websockets.exceptions import ConnectionClosedError
from datetime import datetime

logger = logging.getLogger(__name__)

class WsPublicAsync:
    def __init__(self, url):
        self.url = url
        self.subscriptions = set()
        self.callback = None
        self.loop = asyncio.get_event_loop()
        self.factory = WebSocketFactory(url)

    async def connect(self):
        self.websocket = await self.factory.connect()

    async def consume(self):
        try:
            async for message in self.websocket:
                # if 'pong' == message:
                #     logger.info('recv pong')
                logger.debug("Received message: {%s}", message)
                if self.callback:
                    self.callback(message)
        except ConnectionClosedError as e:
            logger.error(f"WebSocket connection closed unexpectedly: {e}. time:{datetime.now()}")
            await self.connect()
        except Exception as e:
            logger.error(f"consume() Exception: {e}.")

    async def subscribe(self, params: list, callback):
        self.callback = callback
        payload = json.dumps({
            "op": "subscribe",
            "args": params
        })
        await self.websocket.send(payload)
        # await self.consume()

    async def unsubscribe(self, params: list, callback):
        self.callback = callback
        payload = json.dumps({
            "op": "unsubscribe",
            "args": params
        })
        logger.info(f"unsubscribe: {payload}")
        await self.websocket.send(payload)

    async def stop(self):
        await self.factory.close()
        self.loop.stop()

    async def start(self):
        logger.info("Connecting to WebSocket...")
        await self.connect()
        self.loop.create_task(self.consume())

    def stop_sync(self):
        self.loop.run_until_complete(self.stop())
    
    async def keep_send_ping(self):
        while not self.websocket.closed:
            await self.websocket.send('ping')
            await asyncio.sleep(20)
