from typing import Tuple,List
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from contextlib import contextmanager
import time
from psycopg2.extras import execute_values
from functools import wraps

"""
coder: muslih
location:https://raw.githubusercontent.com/muslih-DIY/Python_dev/master/module/psycopg_wraper/pg_wraper.py
version : 2
update_date :03-08-2022
"""

class with_connection:
    def reconnect(function):
        @wraps(function)
        def inner(self,*args,**kwargs): 
            for _ in range(self.retry_max+1):                
                try:
                    print('hi')
                    return function(self,*args,**kwargs)
                except psycopg2.OperationalError:
                    print("except")
                    if self.retry_max != 0 :
                        self.reconnect()
                        continue
                    raise
        inner._inner = function
        return inner
    def select(function):
        
        @wraps(function)
        def inner(self,*args,**kwargs):
            #print(kwargs)
            
            con = kwargs.pop('con',self.con)
            kwargs['con']=con
            #print(self.con)
            #print(con)
            if con is None: raise psycopg2.InterfaceError                   
            with con.cursor() as cur:
                kwargs['cur']=cur
                try:
                    print('hi',function)
                    data = function(self,*args,**kwargs)
                except psycopg2.InterfaceError :
                    raise  psycopg2.OperationalError   
                except psycopg2.OperationalError:
                    raise  psycopg2.OperationalError                     
                except Exception as E:
                    self.error=str(E)
                    return 0
                else:
                    return data
        inner._inner = function
        return inner

    def update(function):
        @wraps(function)
        def inner(self,*args,**kwargs):
            
            con = kwargs.pop('con',self.con)
            commit = kwargs.pop('commit',True)
            rollback = kwargs.pop('rollback',True)
            kwargs['con']=con
            if con is None: raise psycopg2.InterfaceError
            for _ in range(self.retry_max+1):
                try:           
                    with con.cursor() as cur:
                        kwargs['cur']=cur
                        try:
                            data = function(self,*args,**kwargs)
                        except psycopg2.InterfaceError :
                            raise  psycopg2.OperationalError     
                        except psycopg2.OperationalError:
                            raise  psycopg2.OperationalError                                 
                        except Exception as E:
                            if rollback:
                                self.con.rollback()
                            self.error=str(E)
                            return 0
                        else:
                            if commit:
                                con.commit()
                            if data is None:return 1
                            return data
                except  psycopg2.OperationalError:
                    
                    if self.retry_max != 0 :
                        self.reconnect()
                        continue
                    raise        
        inner._inner = function
        return inner

class pg2_base_wrap():
    con: psycopg2.connect =None
    def __init__(self,connector: dict,**kwargs):
        """
        retry_max : number of times need to retry if the system got disconnected
        """
        self.connector=connector
        self.retry_max = kwargs.pop('retry_max',0)
        self.keyattr=kwargs        
        self.query=''
        self.error=''
        

    def close(self):
        self.con.close()
        self.con = None

    def is_connected(self):
        if self.con is None:return 0
        return not self.con.closed 

    def reconnect(self):
        try:
            self.close()
        except:pass
        self.con = None
        return self.connect()

    def re_connect_if_not(self):
        if self.con.closed:
            time.sleep(2)
            self.con = self.pgconnect(self.connector)
    def connect(self):
        if self.con is None or self.con.closed:
            self.con = self.pgconnect(self.connector,**self.keyattr)
        return 1


    @staticmethod
    def pgconnect(pgconfig:dict,**kwargs):
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

    @with_connection.reconnect
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
    def __init__(self, connector:dict,**kwargs):
        super().__init__(connector,**kwargs)
        self.connect()

class pg2_thread_pooled(pg2_base_wrap):
    def  __init__(self, connector:dict,min=1,max=3,**kwargs):
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


class SingletonPg(pg2_base_wrap):
  
    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, 'instance') or not cls.instance:
          cls.instance = super().__new__(cls) 
        return cls.instance

    def __init__(self, connector:dict,**kwargs):
        super().__init__(connector,**kwargs)
        




