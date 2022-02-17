import config as conf
import alpaca_trade_api as aapi

# Alpaca query for all available stock symbols

api = aapi.REST(conf.alp_apikey, conf.alp_secret, base_url=conf.alp_base_url) # or use ENV Vars shown below
assets = api.list_assets()
i = 0
for asset in assets:
    # Only count Active Tradable Stocks
    if asset.status == 'active' and asset.tradable:
        i = i + 1
print(i)