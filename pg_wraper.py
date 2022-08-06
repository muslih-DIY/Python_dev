from typing import Tuple,List
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from contextlib import contextmanager
import time
from .decorators import with_connection
from psycopg2.extras import execute_values
from .dbwrapType import DbWrapBase

"""
coder: muslih
location:https://raw.githubusercontent.com/muslih-DIY/Python_dev/master/module/psycopg_wraper/pg_wraper.py
version : 2
update_date :03-08-2022
"""

class pg2_base_wrap(DbWrapBase):
    def __init__(self,connector,**kwargs):
        self.connector=connector
        self.keyattr=kwargs
        self.con=None
        self.query=''
        self.error=''
        

    def close(self):
        self.con.close()

    def is_connected(self):
        if self.con is None:return 0
        return not self.con.closed  
    
    def re_connect_if_not(self):
        if self.con.closed:
            time.sleep(2)
            self.con = self.pgconnect(self.connector)
    def connect(self):
        if self.con is None or self.con.closed:
            self.con = self.pgconnect(self.connector,**self.keyattr)
        return 1


    @staticmethod
    def pgconnect(pgconfig,**kwargs):
        return psycopg2.connect(
            user=pgconfig['user'],
            password=pgconfig['password'],
            host=pgconfig['host'],
            database=pgconfig['database'],
            port=pgconfig['port'],**kwargs)

    def copy_from_csv(self,csvfile,table,header,sep=",",con=None):
        if con is None:con=self.con
        with con.cursor() as cur:
            try:
                cur.copy_from(
                    file=csvfile,
                    table=table,
                    columns=header,
                    sep=sep)
            except Exception as E:
                con.rollback()
                self.error=str(E)
                return 0
            else:
                con.commit()
                return 1
    

        
    @with_connection.update
    def dict_insert(self,values:dict,table:str,cur,con):
        query=f"insert into {table} ({','.join(values.keys())}) values ({','.join(['%s' for i in range(len(values))])})"
        self.query = query
        cur.execute(query,tuple(values.values()))
        return 1
        
    @with_connection.update
    def execute(self,query,cur,con):
        self.query=query
        cur.execute(query)
        return 1

    @with_connection.update
    def upd(self,query,cur,con):
        self.query=query
        cur.execute(query)
        return 1
        
    @with_connection.update
    def execute_many(self,query,dataset,cur,con):
        self.query=query
        cur.executemany(query,dataset)
        return 1

    @with_connection.update
    def update_many(self,query: str,dataset:List[Tuple],cur,con):
        self.query=query
        execute_values(cur,query, dataset)
        return 1
                
    # def dict_insert(self,values:dict,table:str,con=None):
    #     if con is None:con=self.con
    #     self.query=f"insert into {table} ({','.join(values.keys())}) values ({','.join(['%s' for i in range(len(values))])})"
    #     with con.cursor() as cur:
    #         try:
    #             cur.execute(self.query,tuple(values.values()))
    #         except Exception as E:
    #             con.rollback()
    #             self.error=str(E)
    #             return 0
    #         else:
    #             con.commit()
    #             return 1

    @with_connection.select
    def select(self,query,cur,con,rtype=None,header=0):
        query=f"select json_agg(t) from ({query}) t" if rtype=='json' else query
        self.query = query
        head=None
        try:
            cur.execute(query)
        except Exception as E:
            self.error=str(E)
            return None,0,head
        if header or rtype=='dict':
            head=[x[0] for x in cur.description]
        
        data=cur.fetchall()

        if rtype=='list':
            if len(cur.description)==1:
                return [x[0] for x in data],1,head
            else:
                return [list(x) for x in data],1,head
        if rtype=='dict': 

                return [{k:v for k,v in zip(head,value)} for value in data],1
        return data,1,head

    
    @with_connection.select
    def sel(self,query,cur,con,rtype=None,header=0):

        query=f"select json_agg(t) from ({query}) t" if rtype=='dict' else query
        self.query =  query
        head=None
        try:
            cur.execute(query)
        except Exception as E:
            self.error=str(E)
            return None,0,head
        if header and rtype !='dict' :
            head=[x[0] for x in cur.description]
        data=cur.fetchall()
        if rtype=='list':
            if len(cur.description)==1:
                return [x[0] for x in data],1,head
            else:
                return [list(x) for x in data],1,head
        if rtype=='dict': return data[0][0] or [],1,head
        return data,1,head

class pg2_wrap(pg2_base_wrap):
    def __init__(self, connector,**kwargs):
        super().__init__(connector,**kwargs)
        self.connect()

class pg2_thread_pooled(pg2_base_wrap):
    def  __init__(self, connector,min=1,max=3,**kwargs):
        super().__init__(connector, **kwargs)
        self.min = min
        self.max = max
        self.pool = ThreadedConnectionPool(
            minconn=self.min,maxconn=self.max,
            user=self.connector['user'],
            password=self.connector['pass'],
            host=self.connector['host'],
            database=self.connector['database'],
            port=self.connector['port'],**kwargs)
        
    def close(self):
        self.pool.closeall()

    def is_connected(self):
        pass
      
    def re_connect_if_not(self):
        pass
    @contextmanager
    def connect(self):
        conn = self.pool.getconn()
        try:
            yield conn
        finally:
            self.pool.putconn(conn) 

    def sel(self,query,rtype=None,header=0,con=None):                
        with self.connect() as con:
            return super().sel(query,rtype=rtype,header=header,con=con)

    def upd(self,query,con=None):
        with self.connect() as con:
            return  super().upd(query,con=con)     
         
    def dict_insert(self,values:dict,table:str,con=None):
        with self.connect() as con:
            return super().dict_insert(values=values,table=table,con=con)


    def copy_from_csv(self,csvfile,table,header,sep=",",con=None):
        with self.connect() as con:
            return super().copy_from_csv(csvfile=csvfile,table=table,header=header,sep=sep,con=con)
