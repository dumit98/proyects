import time
import os
import re
import string
import pandas
import numpy as np

from halo import Halo
from sqlalchemy import types
from tabulate import tabulate
from datetime import datetime
from .command import Command


empty_pattern = re.compile(r"^\s*$")
query_name_pattern = re.compile(r"--[^\n]+")
query_columns_pattern = re.compile(r"select.+?(?=from)", re.S | re.I)
db_error_pattern = re.compile(r"^[^\[]+")

date = datetime.now().strftime('%Y%m%d')

logname_db = 'misc_vl_log'
logname_sql = f'vl-sql_export-{date}.sql'


class QueryExecuter(Command):
    
    def execute(self):
        fname = self.fname
        outfile = self.outfile
        sql_pckg = self.package_list[self.pckg_no -1]
        queries = self.get_queries(sql_pckg, fname)

        log1 = []
        log1.append(['run_date', 'table_name', 'vl_mode', 'sql_package',
                     'sql_name', 'result', 'sql_definition'])

        for qname, query in queries.items():
            time = datetime.now().strftime('%H:%M:%S')

            if self.show_summary:
                if 'count' in query_columns_pattern.search(query).group():
                    self.exec_query(qname, query, 'stdout')
                    print()
            else:
                self.exec_query(qname, query, outfile)

                result = self.status if self.status else self.rowcount

                query_name_str = f'--{result}\n--{time}\n--'
                datetime_str = f'{date} {time}'

                query = query.replace('--', query_name_str, 1)

                log1.append([datetime_str, self.table_name, fname,
                            os.path.basename(sql_pckg), qname, result, query])

                log2 = open(logname_sql, 'a')
                log2.write(query + ';')
                log2.close()

        if len(log1) > 1:
            self.log_to_db(log1)

    
    def get_queries(self, sql_pckg=None, fname=None):
        msg_nofile = 'No sql file found'
        msg_no_qname = 'A query has an invalid title'
        flist = [f for f in os.listdir(sql_pckg) if fname in f.lower()]
        sql_file = flist[0] if len(flist) == 1 else exit(msg_nofile)
        sql_list = open(os.path.join(sql_pckg, sql_file)).read().split(';')

        def format_query(query):
            servers = {'edm':'edmtc','fra':'fratc','hou':'houtc_stby',
                       'houp':'houtc','nor':'krstc','sha':'shatc', 'train':'houtrain'}

            placeholders = [v[1] for v in string.Formatter().parse(query)]
            placeholders = filter(None, placeholders)
            
            fmt_map = {}
            for var in placeholders:
                var = var.replace('py_', '').lower()

                if not hasattr(self, var):
                   val = input(var + "? >> ")
                   setattr(self, var, val)
                   var_prefixed = 'py_' + var
                   fmt_map[var_prefixed] = val
                else:
                    var_prefixed = 'py_' + var
                    if 'linked' in var:
                        fmt_map[var_prefixed] = servers[self.linked_server]
                    else:
                        fmt_map[var_prefixed] = getattr(self, var)

            query = query.format_map(fmt_map)
            return query

        queries = {}
        for sql_text in sql_list:
            if not empty_pattern.match(sql_text):
                qname = query_name_pattern.search(sql_text)
                qname = qname.group().lstrip('--').strip() if qname else exit(msg_no_qname)
                queries[qname] = format_query(sql_text)

        return queries


    def exec_query(self, query_name, query, output):
        msg_running = 'Running {0}...'
        msg_success = 'Done\t{0}\t{1}{2}'
        msg_success2 = 'Done {0} {1}{2}'
        msg_fail = 'Fail\t\t{0}  ERR:{1}'
        self.status = ''
        table = ''
        spinner = Halo(spinner='line', text=msg_running.format(query_name))

        try:
            spinner.start()
            df = pandas.read_sql(query, self.engine)
        except Exception as e:
            err = db_error_pattern.search(str(e)).group()
            self.status = 'fail'
            return spinner.fail(text=msg_fail.format(query_name, err))

        rowcount = str(len(df.index))
        setattr(self, 'rowcount', rowcount)

        fname_xlsx = f'{query_name} - Rows {rowcount}.xlsx'
        fname_text = f'{query_name}_{rowcount}.txt'

        if not df.empty:
            if output == 'excel':
                df.columns = map(str.upper, df.columns)
                df.to_excel(fname_xlsx, index=False, encoding='utf8', freeze_panes=(1,0))
            elif output == 'stdout':
                msg_success = msg_success2
                table = '\n' + tabulate(df, headers='keys', tablefmt='psql', showindex=False)
            elif output == 'txt' and len(df.columns) == 1:
                with open(fname_text, 'wb') as outfile:
                    outfile.write(b'INPUT\r\n')
                    np.savetxt(outfile, df, fmt='%s', encoding='utf8', newline='\r\n')
            else:
                df.columns = map(str.upper, df.columns)
                df.to_excel(fname_xlsx, index=False, encoding='utf8', freeze_panes=(1,0))

        return spinner.succeed(text=msg_success.format(rowcount, query_name, table))


    def log_to_db(self, array):
        msg_input = '\nload log to DB?[y/n] >> '
        df = pandas.DataFrame(columns=array[0])

        for i in range(1, len(array)):
            df.loc[i] = array[i]

        dtyp = {}
        for col in df.columns:
            if 'date' in col:
                df[col] = df[col].astype('datetime64')
            elif 'result' in col:
                df[col].replace('fail', np.nan, inplace=True)
                dtyp[col] = types.FLOAT
            elif 'definition' in col:
                dtyp[col] = types.CLOB
            else:
                dtyp[col] = types.VARCHAR(55)
                if 'sql_name' in col:
                    pass
                else:
                    df[col] = df[col].str.upper()

        while True:
            resp =  input(msg_input).lower()
            if resp == 'y':
                break
            elif resp == 'n':
                return
            else:
                continue

        return df.to_sql(logname_db, self.engine, index=True,
                         index_label='seq', if_exists='append', dtype=dtyp)
