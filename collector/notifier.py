import logging

import aiohttp

logger = logging.getLogger(__name__)


class BarkNotifier:
    def __init__(self, server: str, key: str):
        self.server = server.rstrip("/")
        self.key = key

    async def send_notification(self, title: str, body: str, group: str = "IBKR"):
        if not self.key:
            return False

        url = f"{self.server}/{self.key}/{title}/{body}"
        params = {"group": group}

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, params=params, timeout=10) as response:
                    if response.status == 200:
                        return True
                    else:
                        logger.error(
                            f"Bark notification failed with status {response.status}"
                        )
                        return False
        except Exception as e:
            logger.error(f"Error sending Bark notification: {e}")
            return False
