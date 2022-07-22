import os, sys
from sf import _clear_stage, _write_to_stage, _write_to_table
from web3 import Web3
from snowflake.connector import connect


SNOWFLAKE_CONNECTION = dict(
    account = SNOWFLAKE_HOST,
    user = SNOWFLAKE_USERNAME,
    password = SNOWFLAKE_PASSWORD,
    warehouse = SNOWFLAKE_WAREHOUSE,
    role = SNOWFLAKE_ROLE)

connection = connect(**SNOWFLAKE_CONNECTION)
sf = connection.cursor()


calls = sf.execute(f"""
    select c.block, c.timestamp, c.tx_hash, tx.from_address as owner, c.call_data
    from edw_share.raw.calls c
    join edw_share.raw.transactions tx
    on c.tx_hash = tx.tx_hash
    where c.from_address = '0x4976fb03c32e5b8cfe2b6ccb31c09ba78ebaba41'
    and c.to_address = '0x4976fb03c32e5b8cfe2b6ccb31c09ba78ebaba41'
    and left(c.call_data, 10) = '0x10f13a8c';"""
).fetchall()

# v = """0x10f13a8cd68dc6e1bbcb187b152d26c621cd9f922c24d3b9bcbdadb2e5ec302bb3697354000000000000000000000000000000000000000000000000000000000000006000000000000000000000000000000000000000000000000000000000000000a0000000000000000000000000000000000000000000000000000000000000000375726c0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000009168747470733a2f2f6f70656e7365612e696f2f6173736574732f3078666163376265613235356136393930663734393336333030323133366166363535366233316530342f31343132303930353335303734323139303136323838343037353433363134333032383231343438313036363831323733303835333339363934353632333837333035353330373339373637000000000000000000000000000000"""

etl_data = list()

for block, timestamp, tx_hash, owner, call_data in calls:

    func = call_data[0:10]
    node = '0x' + call_data[10:74]
    txt = Web3.toText(hexstr='0x' + call_data[266:330]).strip('\x00')
    val = Web3.toText(hexstr='0x' + call_data[394:len(call_data)]).strip('\x00').replace('\n', '\\n')

    etl_data.append([
        block,
        timestamp.__str__()[:19],
        tx_hash,
        owner,
        node,
        str(txt),
        str(val)
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
