
import mysql.connector
from mysql.connector.errors import DatabaseError



class MySQL(object):
    db_config = {
            'user': 'username',
        'password': 'password',
            'host': 'host ip address',
        'database': 'name of database'
         }
    def __init__(self):


        try:
            self.cnxn = mysql.connector.connect(**self.db_config)
        except DatabaseError as err:
            if getattr(err, 'errno',0) == 2003:
                raise Exception('Likely the mysql database is stopped')
            raise

    def execute(self, qry):
        cursor = self.cnxn.cursor()
        cursor.execute(qry)
        cursor.close()
    
    def fetchall(self, qry):
        cursor = self.cnxn.cursor()
        cursor.execute(qry)
        data = cursor.fetchall()
        cursor.close()
        return data

    def insert(self, qry, val):
        cursor = self.cnxn.cursor()
        cursor.execute(qry, val)
        self.cnxn.commit()
        cursor.close()

    def insertmany(self, qry, val):
        cursor = self.cnxn.cursor()
        cursor.executemany(qry, val)
        self.cnxn.commit()
        cursor.close()

   



