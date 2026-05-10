from unittest.mock import AsyncMock, patch

import pytest
from collector.notifier import BarkNotifier


@pytest.mark.asyncio
async def test_send_notification_success():
    notifier = BarkNotifier(server="https://api.day.app", key="test_key")
    with patch("aiohttp.ClientSession.post") as mock_post:
        # Mocking the context manager for aiohttp
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_post.return_value.__aenter__.return_value = mock_response

        success = await notifier.send_notification("Title", "Body")
        assert success is True
