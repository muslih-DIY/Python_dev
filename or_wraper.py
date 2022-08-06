from typing import List, Tuple
from functools import wraps
import cx_Oracle 
from io import StringIO
import csv
from .dbwrapType import DbWrapBase
from .decorators import with_connection
"""
coder: muslih
SID can be generated using cx_Oracle.makedsn("oracle.sub.example.com", "1521", "ora1")
location: https://raw.githubusercontent.com/muslih-DIY/Python_dev/master/module/cx_oracle_wrapper/or_wraper.py
version : 2
update_date :24-06-2022 
"""

class oracle_base_wrap(DbWrapBase):

    def __init__(self,connector,**kwargs):
        self.connector=connector
        self.keyattr=kwargs
        self.con = None
        self.query=''
        self.error=''

    def close(self):
        self.con.close()

    def is_connected(self):
        if self.con is None:return 0
  
    
    def re_connect_if_not(self):
        pass

    def connect(self):
        if self.con is None:
            self.con = self.orconnect(self.connector,**self.keyattr)
        return 1


    @staticmethod
    def orconnect(connector,**kwargs):
        return cx_Oracle.connect(connector['user'],connector['password'],connector['sid'],**kwargs)


    @with_connection.update
    def upd(self,query,cur,con):
        self.query=query
        cur.execute(query)
   
    @with_connection.update
    def execute(self,query,cur,con):
        self.query=query
        cur.execute(query)

    @with_connection.update
    def execute_many(self,query,dataset:List[Tuple],cur,con,batcherrors=False):        
        if not dataset:
            return 1
        self.query = query
        cur.executemany(query,dataset,batcherrors=batcherrors)
        if batcherrors:
            return cur.getbatcherrors()
                
    
    def insert_many_list(self,table: str,column: list,dataset:List[Tuple],batcherrors=False):
        cols  = ','.join(column)
        params= ','.join([f':{i}' for i in range(0,len(column))])
        quary = f"insert into {table} ({cols}) values ({params})"
        errors = self.execute_many(query=quary,dataset=dataset,batcherrors=batcherrors)
        if batcherrors and errors:
            return [(dataset[error.offset],error.message) for error in errors ]

    @with_connection.update
    def dict_insert(self,values:dict,table:str,cur,con):
        cols  = ','.join(values.keys())
        params= ','.join( f':{k}' for k in values.keys())
        query=f"insert into {table} ({cols}) values ({params})"
        self.query = query
        cur.execute(query,values)
        
    @with_connection.select
    def select(self,query,cur,con,rtype=None,header=0):
        self.query=query
        head=None
        try:
            cur.execute(self.query)
        except Exception as E:
            self.error=str(E)
            return None,0,head

        if rtype=='dict':
            cur.rowfactory = lambda *args: dict(zip([d[0] for d in cur.description], args))
            data = cur.fetchall()
            return data,1,head
        if header and rtype !='dict' :
            head=[x[0] for x in cur.description]
        data=cur.fetchall()
        if rtype=='list':
            if len(cur.description)==1:
                return [x[0] for x in data],1,head
            else:
                return [list(x) for x in data],1,head
        return data,1,head
        
    def sel_to_IOstring(self,query,cur,con,fdata:Tuple=None,arraysize:int=500,headcase=str.upper):
        """
        Return:
            => StringIO,status,heads

        headcase  
        ----
            :   str.lower
            :   str.upper
        ---
        data:list
        For adding new fixed fdata into the csv     
        """
        self.query=query
        #if con is None:con=self.con
        head=None
        if fdata is not None and not isinstance(fdata,tuple):
            raise TypeError("data => should be Tuple eg: (4,) or (2,4)")
        try:
            cur.arraysize=arraysize
            cur.execute(query)
        except Exception as E:
            self.error=str(E)
            return None,0,head
        else:
            sio = StringIO()
            writer = csv.writer(sio)
            if not fdata:
                writer.writerows(cur.fetchall())                                  
            else:
                #print([(*fdata,*row) for row in cur])
                [writer.writerows([(*fdata,*row)]) for row in cur if row ]    
            sio.count = cur.rowcount
            sio.len = sio.tell()
            sio.seek(0)
            return sio,1,[headcase(x[0]) for x in cur.description]   
        


class oracle_wrap(oracle_base_wrap):
    def __init__(self, connector,**kwargs):
        super().__init__(connector,**kwargs)
        self.connect()

class oracle_thread_pooled(oracle_base_wrap):
    def  __init__(self, connector,min=1,max=3,increment=1,threaded=False,**kwargs):
        super().__init__(connector, **kwargs)
        self.min = min
        self.max = max
        self.inc = increment
        self.threaded = threaded
        self.pool = cx_Oracle.SessionPool(self.connector['user'],self.connector['password'],self.connector['sid'], min=self.min,
                             max=self.max, increment=self.inc,threaded =self.threaded, getmode = cx_Oracle.SPOOL_ATTRVAL_WAIT)
        
    def close(self):
        self.pool.close()

    def is_connected(self):
        pass
      
    def re_connect_if_not(self):
        pass

    def connect(self):
        pass

    def sel(self,query,rtype=None,header=0,con=None):
        with self.pool.acquire() as con:
            return super().sel(query,rtype=rtype,header=header,con=con)

    def upd(self,query,con=None):
        with self.pool.acquire() as con:
            return super().upd(query,con=con)
    def dict_insert(self,values:dict,table:str,con=None):
        with self.pool.acquire() as con:
            return super().dict_insert(values=values,table=table,con=con)
    def sel_to_IOstring(self,query,fdata:Tuple=None,arraysize:int=500,headcase=str.upper,con=None):
        with self.pool.acquire() as con:
            return super().sel_to_IOstring(query,fdata=fdata,arraysize=arraysize,headcase=headcase,con=con)