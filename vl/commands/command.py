import datetime
from sqlalchemy import create_engine
#  from cx_Oracle import Connection, Cursor


class Command:
    def __init__(self, **kwargs):
        for key,val in kwargs.items():
            setattr(self, key, val)

    def __enter__(self):
        print("Connecting... ")
        #  self.con = Connection('cdmuser/cdmuser@//srvhouplmtdb02:1521/cdm',
                               #  encoding='utf8', nencoding='utf8')
        #  self.cur = self.__db.cursor()
        self.engine = create_engine(
            'oracle+cx_oracle://cdmuser:C3tDa7aUs3R@srvhouplmtdb02.nov.com/cdm',
            # 'oracle+cx_oracle://system:oracle@localhost:49161/xe',
            connect_args={'encoding': 'utf8', 'nencoding': 'utf8'},
            echo=False
        )
        if hasattr(self, 'linked_server'):
            print('Site: ', self.linked_server.upper(), '\n')

    def __exit__(self, type, value, traceback):
        print("Closing Connection... ")
        #  self.cur.close()
        #  self.con.close()
