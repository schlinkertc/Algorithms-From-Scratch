from sqlalchemy import create_engine
import pandas as pd
import os

class MyDatabase:
    # http://docs.sqlalchemy.org/en/latest/core/engines.html
    """
    Custom class for instantiating a SQL Alchemy connection. Configured here for SQLite, but intended to be flexible.
    Credit to Medium user Mahmud Ahsan:
    https://medium.com/@mahmudahsan/how-to-use-python-sqlite3-using-sqlalchemy-158f9c54eb32
    """
    DB_ENGINE = {
       'sqlite': 'sqlite:////{DB}'
    }

    # Main DB Connection Ref Obj
    db_engine = None
    def __init__(self, dbtype, 
                 username='', password='', 
                 dbname='',path=os.getcwd()+'/'):
        dbtype = dbtype.lower()
        if dbtype in self.DB_ENGINE.keys():
            engine_url = self.DB_ENGINE[dbtype].format(DB=path+dbname)
            self.db_engine = create_engine(engine_url)
            print(self.db_engine)
            
        else:
            print("DBType is not found in DB_ENGINE")
            
db = MyDatabase('sqlite',dbname='mlb.db',path="/Users/schlinkertc/code/MLB/")


    
tables = ['pitches','pitch_data']
dfs = [
    pd.read_sql(f'select * from {table}',db.db_engine) 
    for table in tables
]

pitches = pd.merge(dfs[0],dfs[1],how='inner')

df = pitches[
    ['pitchData_endSpeed',
     'pitchData_breaks_spinRate',
     'details_type_description'
    ]
]
df.columns = ['endSpeed','spinRate','pitchType']

df.to_csv('datasets/pitchSpeed_spinRate.csv',index=False)