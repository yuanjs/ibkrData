import pytest
import asyncio
import os

# Workaround for Python 3.12+ / 3.14 event loop initialization issue
try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

from ib_insync import IB, Future
from dotenv import load_dotenv

# Load env for config
load_dotenv()

@pytest.fixture(scope="session")
def api_base_url():
    port = os.getenv("API_PORT", "8002")
    return f"http://localhost:{port}"

@pytest.fixture(scope="session")
def auth_headers():
    token = os.getenv("JWT_TOKEN", "yuanjs666")
    return {"Authorization": f"Bearer {token}"}

@pytest.fixture(scope="session")
async def active_spi_contract():
    """
    Connects to IBKR to find the rolling active SPI futures contract via CONTFUT.
    """
    ib = IB()
    host = os.getenv("IB_HOST", "127.0.0.1")
    port = int(os.getenv("IB_PORT", "7497"))
    
    try:
        # Use a high clientId for testing to avoid conflicts
        await asyncio.wait_for(ib.connectAsync(host, port, clientId=999), timeout=10)
        
        # Use CONTFUT to find the IBKR rolling active contract
        from ib_insync import Contract
        cont = Contract(secType='CONTFUT', symbol='SPI', exchange='SNFE', currency='AUD')
        cont_details = await ib.reqContractDetailsAsync(cont)
        
        if cont_details:
            resolved = cont_details[0].contract
            # Build a real FUT contract from the CONTFUT result
            active = Future(
                symbol='SPI',
                exchange='SNFE',
                currency='AUD',
                lastTradeDateOrContractMonth=resolved.lastTradeDateOrContractMonth,
                tradingClass=resolved.tradingClass,
                multiplier=resolved.multiplier,
            )
            qualified = await ib.qualifyContractsAsync(active)
            if qualified:
                active = qualified[0]
            print(f"\n[INFO] CONTFUT active SPI contract: {active.localSymbol} (Exp: {active.lastTradeDateOrContractMonth})")
            return active
        
        # Fallback: use reqContractDetails on FUT
        spi = Future(symbol='SPI', exchange='SNFE', currency='AUD')
        contracts = await ib.reqContractDetailsAsync(spi)
        
        if not contracts:
            pytest.fail("Could not find any SPI futures contracts on IBKR.")
        
        sorted_contracts = sorted([c.contract for c in contracts], key=lambda x: x.lastTradeDateOrContractMonth)
        active = sorted_contracts[0]
        print(f"\n[INFO] Fallback SPI contract: {active.localSymbol} (Exp: {active.lastTradeDateOrContractMonth})")
        return active
        
    except Exception as e:
        pytest.fail(f"IBKR connection/contract error: {e}")
    finally:
        if ib.isConnected():
            ib.disconnect()

