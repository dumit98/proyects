from sqlalchemy import create_engine, types
from .db_credentials import *
import pandas as pd
import logging
import cx_Oracle
import sys


logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

servers = {
    'edm': edm_db_config,
    'fra': fra_db_config,
    'houstby': houstby_db_config,
    'houprod': houprod_db_config,
    'nor': nor_db_config,
    'sha': sha_db_config,
    'dw': datawarehouse_db_config
}

def quick_etl(sql, table_name, target_db, source_db):
    """
params
    sql           str, sql statement
    table_name    str, name of staging table name
    target_db     str, dw, ds or excel
    source_db     str or list, "all" or a list of sites "['houstby','edm',...]"
                  current sites are edm, fra, houstby, houprod, nor, sha and dw
    """

    if target_db.lower() == 'dw':
        eng = create_engine('oracle+cx_oracle://', connect_args=datawarehouse_db_config, echo=False)
    elif target_db.lower() == 'ds':
        eng = create_engine('oracle+cx_oracle://', connect_args=dataservices_db_config, echo=False)
    elif target_db.lower() == 'excel':
        pass
    else:
        exit('incorrect target')

    if source_db == 'all':
        selected_sites = list(servers.keys())
        selected_sites.remove('houprod')
        selected_sites.remove('dw')
    else:
        selected_sites = source_db

    data = []
    for site in selected_sites:

        print('{div} {site} {div}'.format(site=site.upper(), div='='*30))

        if site not in servers.keys():
            print('%s not valid!' % site.upper(), file=sys.stderr)
            continue
        try:
            conn = create_engine('oracle+cx_oracle://', connect_args=servers[site], echo=False)
            d = conn.execute(sql)
            meta = d.cursor.description
            resultset = d.fetchall()

            print('ROWCOUNT %d' % d.rowcount)

            df = pd.DataFrame.from_records(resultset, columns=[c[0] for c in meta])
            data.append(df)
        except Exception as e:
            print(e, file=sys.stderr)
            if site == 'houstby' and 'read-only' in str(e):
                selected_sites.append('houprod')
            continue

    df = pd.concat(data, sort=False)

    dtyp = {}
    for col in meta:
        name = col[0]
        type = col[1]
        size = col[2] if col[2] else 1

        if type == cx_Oracle.STRING:
            dtyp[name] = types.VARCHAR(size)
        elif type == cx_Oracle.FIXED_CHAR:
            dtyp[name] = types.VARCHAR(size)
        elif type == cx_Oracle.NUMBER:
            dtyp[name] = types.FLOAT(size)
        elif type == cx_Oracle.DATETIME:
            dtyp[name] = types.DATE
        elif type == cx_Oracle.TIMESTAMP:
            dtyp[name] = types.TIMESTAMP

    print('{div} {site} {div}'.format(site=target_db.upper(), div='='*30))
    print('META')
    print(meta)
    # print('\n'.join(meta))

    if target_db == 'excel':
        with pd.ExcelWriter(table_name) as writer:
            df.to_excel(writer, index=False, sheet_name='Sheet1', freeze_panes=(1, 0))
            writer.book.create_sheet('Sheet2')
            writer.book.worksheets[1]['A2'] = sql
    else:
        df.to_sql(table_name.lower(), eng, if_exists='replace', index=False, dtype=dtyp,
                  chunksize=20000)
