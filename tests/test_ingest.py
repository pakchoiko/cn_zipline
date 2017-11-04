from tdx.engine import Engine
from cn_zipline.bundles.tdx_bundle import *
import os
from zipline.data.bundles import register
from zipline.data import bundles as bundles_module
from functools import partial


def target_ingest(assets):
    import cn_stock_holidays.zipline.default_calendar

    if assets:
        if not os.path.exists(assets):
            raise FileNotFoundError
        df = pd.read_csv(assets, names=['symbol', 'name'], dtype=str)
        assets = df['symbol'].tolist()
        register('tdx', partial(tdx_bundle, assets[:1]), 'SHSZ')
    else:
        register('tdx', partial(tdx_bundle, ['000521']), 'SHSZ')

    bundles_module.ingest('tdx',
                          os.environ,
                          pd.Timestamp.utcnow(),
                          show_progress=True,
                          )


def test_target_ingest():
    yield target_ingest,'tests/ETF.csv'
    yield target_ingest,None
