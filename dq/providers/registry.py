from .fred import FREDProvider
from .stooq import StooqProvider
from .yfinance_provider import YFinanceProvider
from .ecb_fx import ECBFXProvider, ECBFXCrossProvider
from .twelvedata import TwelveDataProvider

PROVIDERS = {
    "fred": FREDProvider(),
    "stooq": StooqProvider(),
    "yfinance": YFinanceProvider(),
    "ecb_fx": ECBFXProvider(),
    "ecb_fx_cross": ECBFXCrossProvider(),
    "twelvedata": TwelveDataProvider(),
}

def get_provider(name: str):
    if name not in PROVIDERS:
        raise KeyError(f"Unknown provider {name}. Available: {sorted(PROVIDERS)}")
    return PROVIDERS[name]
