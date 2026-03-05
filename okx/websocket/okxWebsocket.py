
"""
下面给你一份 专门针对 OKX 的 WebSocket Engine（生产级模板）。
特点：
✅ 代理支持
✅ 自动重连
✅ 指数退避重连
✅ 心跳检测
✅ 自动恢复订阅
✅ gzip 自动解压
✅ asyncio.Queue 解耦
✅ 延迟监控
✅ 多 channel 处理
"""


import asyncio
import json
import ssl
import time
import gzip
import logging
from typing import Optional, Dict, Any

import certifi
from websockets.asyncio.client import connect

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("okx_ws")


class OKXWebSocketEngine:

    def __init__(
        self,
        url: str = "wss://ws.okx.com:8443/ws/v5/public",
        proxy: Optional[str] = None,
        ping_interval: int = 20,
        max_backoff: int = 60,
    ):

        self.url = url
        self.proxy = proxy

        self.websocket = None

        self.ping_interval = ping_interval

        self.max_backoff = max_backoff
        self.backoff = 1

        self.subscriptions = []

        self.queue: asyncio.Queue = asyncio.Queue()

        self.last_msg_time = time.time()

        self.ssl_context = ssl.create_default_context()
        self.ssl_context.load_verify_locations(certifi.where())

    async def connect(self):

        logger.info("connecting %s", self.url)

        self.websocket = await connect(
            self.url,
            ssl=self.ssl_context,
            proxy=self.proxy,
            ping_interval=None,
            ping_timeout=None,
            max_queue=None,
        )

        logger.info("connected")

        self.backoff = 1

        await self.resubscribe()

    async def send(self, msg: Dict[str, Any]):

        data = json.dumps(msg)
        await self.websocket.send(data)

    async def subscribe(self, args):

        self.subscriptions.extend(args)

        msg = {
            "op": "subscribe",
            "args": args,
        }

        await self.send(msg)

        logger.info("subscribe %s", args)

    async def resubscribe(self):

        if not self.subscriptions:
            return

        msg = {
            "op": "subscribe",
            "args": self.subscriptions,
        }

        await self.send(msg)

        logger.info("resubscribe success")

    async def heartbeat(self):

        while True:

            await asyncio.sleep(self.ping_interval)

            try:

                await self.websocket.ping()

                if time.time() - self.last_msg_time > 60:
                    raise Exception("no message timeout")

            except Exception as e:

                logger.warning("heartbeat error %s", e)

                await self.websocket.close()

                break

    def decompress(self, message):

        if isinstance(message, bytes):

            try:
                return gzip.decompress(message).decode()

            except Exception:
                return message.decode()

        return message

    async def recv_loop(self):

        async for message in self.websocket:

            self.last_msg_time = time.time()

            message = self.decompress(message)

            try:
                data = json.loads(message)
            except Exception:
                logger.warning("json parse error %s", message)
                continue

            await self.queue.put(data)

    async def consumer(self):

        while True:

            data = await self.queue.get()

            await self.handle_message(data)

    async def handle_message(self, data):

        if "event" in data:

            logger.info("event %s", data)

            return

        arg = data.get("arg", {})
        channel = arg.get("channel")

        if channel == "tickers":

            ticker = data["data"][0]

            print(
                "ticker",
                ticker["instId"],
                ticker["last"],
                ticker["ts"],
            )

        else:

            print(data)

    async def monitor_latency(self):

        while True:

            await asyncio.sleep(10)

            latency = time.time() - self.last_msg_time

            logger.info("ws latency %.2f sec", latency)

    async def run(self):

        while True:

            try:

                await self.connect()

                tasks = [
                    asyncio.create_task(self.recv_loop()),
                    asyncio.create_task(self.consumer()),
                    asyncio.create_task(self.heartbeat()),
                    asyncio.create_task(self.monitor_latency()),
                ]

                await asyncio.gather(*tasks)

            except Exception as e:

                logger.error("connection error %s", e)

            logger.info("reconnect in %s sec", self.backoff)

            await asyncio.sleep(self.backoff)

            self.backoff = min(self.backoff * 2, self.max_backoff)


async def main():

    ws = OKXWebSocketEngine(
        proxy="http://127.0.0.1:10808"
    )

    await ws.connect()

    await ws.subscribe([
        {
            "channel": "tickers",
            "instId": "BTC-USDT"
        }
    ])

    await ws.run()


if __name__ == "__main__":
    asyncio.run(main())