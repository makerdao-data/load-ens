

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
    select block, timestamp, tx_hash, concat('0x', lpad(ltrim(lower(topic2), '0x'), 40, '0')) as topic2, log_data
    from edw_share.raw.events
    where topic0 = '0xca6abbe9d7f11422cb6ca7629fbf6fe9efb1c621f71ce8f02b9f2a230097404f';"""
).fetchall()

etl_data = list()

for block, timestamp, tx_hash, owner, call_data in calls:

    name = Web3.toText(hexstr='0x' + call_data[258:]).strip('\x00')

    etl_data.append([
        block,
        timestamp.__str__()[:19],
        tx_hash,
        owner,
        None,
        'name',
        name + '.eth'
    ])

    # sf.execute(f"""
    #     insert into maker.public.ens
    #     values({block}, '{timestamp.__str__()[:19]}', '{tx_hash}', '{owner}', '{node}', '{txt}', '{val}')
    # """)

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