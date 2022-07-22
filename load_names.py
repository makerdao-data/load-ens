

# 0xb7d29e911041e8d9b843369e890bcb72c9388692ba48b65ac54e7214c4c348f7


import os, sys
from sf import _clear_stage, _write_to_stage, _write_to_table
from web3 import Web3
from snowflake.connector import connect
from dotenv import load_dotenv

load_dotenv()

SNOWFLAKE_CONNECTION = dict(
    account = os.getenv('SNOWFLAKE_HOST'),
    user = os.getenv('SNOWFLAKE_USERNAME'),
    password = os.getenv('SNOWFLAKE_PASSWORD'),
    warehouse = os.getenv('SNOWFLAKE_WAREHOUSE'),
    role = os.getenv('SNOWFLAKE_ROLE'))

connection = connect(**SNOWFLAKE_CONNECTION)
sf = connection.cursor()


calls = sf.execute(f"""
    select c.block, c.timestamp, c.tx_hash, tx.from_address as owner, c.call_data
    from edw_share.raw.calls c
    join edw_share.raw.transactions tx
    on c.tx_hash = tx.tx_hash
    where c.to_address = lower('0xA2C122BE93b0074270ebeE7f6b7292C7deB45047')
    and left(c.call_data, 10) = '0x77372213'
    and c.status
    order by c.block;"""
).fetchall()

# v = """0x10f13a8cd68dc6e1bbcb187b152d26c621cd9f922c24d3b9bcbdadb2e5ec302bb3697354000000000000000000000000000000000000000000000000000000000000006000000000000000000000000000000000000000000000000000000000000000a0000000000000000000000000000000000000000000000000000000000000000375726c0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000009168747470733a2f2f6f70656e7365612e696f2f6173736574732f3078666163376265613235356136393930663734393336333030323133366166363535366233316530342f31343132303930353335303734323139303136323838343037353433363134333032383231343438313036363831323733303835333339363934353632333837333035353330373339373637000000000000000000000000000000"""

etl_data = list()

for block, timestamp, tx_hash, owner, call_data in calls:

    node = '0x' + call_data[10:74]
    name = Web3.toText(hexstr='0x' + call_data[202:]).strip('\x00')

    etl_data.append([
        block,
        timestamp.__str__()[:19],
        tx_hash,
        owner,
        node,
        'name',
        name
    ])

pattern = None
if etl_data:
    pattern = _write_to_stage(sf, etl_data, f"test_mcd.events.manager_storage")

    _write_to_table(
        sf,
        f"test_mcd.events.manager_storage",
        f"maker.public.ens",
        pattern,
    )
    _clear_stage(sf, f"test_mcd.events.manager_storage", pattern)