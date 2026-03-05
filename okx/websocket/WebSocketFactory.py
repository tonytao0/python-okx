import asyncio
import logging
import ssl

import certifi
import websockets
from websockets.asyncio.client import connect

logger = logging.getLogger(__name__)


class WebSocketFactory:

    def __init__(self, url,proxy=None):
        self.url = url
        self.proxy = proxy
        self.websocket = None
        self.loop = asyncio.get_event_loop()

    async def connect(self):
        ssl_context = ssl.create_default_context()
        ssl_context.load_verify_locations(certifi.where())
        # PROXY_URL = "http://127.0.0.1:10808"
        try:
            if self.proxy:
                self.websocket = await websockets.connect(self.url, ssl=ssl_context)
                self.websocket = await connect(
                    self.url,
                    ssl=ssl_context,
                    proxy=self.proxy,
                    ping_interval=None,
                    ping_timeout=None,
                    max_queue=None
                )
                logger.info("WebSocket connection established.")
                return self.websocket
            else:
                self.websocket = await websockets.connect(self.url, ssl=ssl_context)
                logger.info("WebSocket connection established.")
                return self.websocket
        except Exception as e:
            logger.error(f"Error connecting to WebSocket: {e}")
            return None

    async def close(self):
        if self.websocket:
            await self.websocket.close()
            self.websocket = None
