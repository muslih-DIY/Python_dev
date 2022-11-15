from typing import Tuple,List,Callable
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

"""

class with_connection:
    def reconnect(function:Callable):
        """
        reconnection
            ------------
            controlling parameters are
            self.retry_max  : number of retry
            self.retry_step : duration increasing step.
        """

        @wraps(function)
        def inner(self,*args,**kwargs):
            try:
                return function(self,*args,**kwargs)
            except (psycopg2.OperationalError ,psycopg2.InterfaceError):

                for i in range(self.retry_max):
                    time.sleep((i+1)*self.retry_step)
                    try:
                        self.attempt = i
                        print("attempts : ",i)
                        self.reconnect()
                        return function(self,*args,**kwargs)
                    except (psycopg2.OperationalError,psycopg2.InterfaceError):
                        continue
                raise
        inner.inner = function
        return inner

    def select(function):
        "select function decorator"
        @wraps(function)
        def inner(self,*args,**kwargs):
            con = kwargs.pop('con',self.con)
            kwargs['con']=con

            if con is None:
                raise psycopg2.InterfaceError                   
            with con.cursor() as cur:
                kwargs['cur']=cur
                try:
                    data = function(self,*args,**kwargs)
                except psycopg2.InterfaceError :
                    raise  psycopg2.OperationalError   
                except psycopg2.OperationalError:
                    raise  psycopg2.OperationalError                     
                except Exception as error:
                    self.error=str(error)
                    return 0
                else:
                    return data
        inner.inner = function
        return inner

    def update(function):
        @wraps(function)
        def inner(self,*args,**kwargs):
            
            con = kwargs.pop('con',self.con)
            commit = kwargs.pop('commit',True)
            rollback = kwargs.pop('rollback',True)
            kwargs['con']=con
            if con is None: raise psycopg2.InterfaceError
  
            with con.cursor() as cur:
                kwargs['cur']=cur
                try:
                    data = function(self,*args,**kwargs)
                
                except psycopg2.InterfaceError :
                    raise  psycopg2.OperationalError     
                except psycopg2.OperationalError:
                    raise  psycopg2.OperationalError                                 
                except Exception as ex:
                    if rollback:
                        self.con.rollback()
                    self.error=str(ex)
                    return 0
                else:
                    if commit:
                        con.commit()
                    if data is None:
                        return 1
                    return data
        inner.inner = function
        return inner

class pg2_base_wrap():
    con: psycopg2.connect =None

    def __init__(self,connector: dict,*args,**kwargs):
        """
        retry_max : number of times need to retry if the system got disconnected
        """
        self.connector=connector
        self.retry_max = kwargs.pop('retry_max',0)
        self.retry_step = kwargs.pop('retry_step',5)
        self.attempt = None
        self.keyattr=kwargs
        self.query=''
        self.error=''

    def close(self):
        'close and set con as None'
        self.con.close()
        self.con = None

    def is_connected(self):
        'return connextion status'
        if self.con is None:
            return 0
        return not self.con.closed

    def reconnect(self):
        'reconnect after ensuring close'
        if self.con:
            try:
                self.close()
            except psycopg2.InterfaceError :
                self.con = None

        return self.connect()

    def re_connect_if_not(self):
        'rconnect only if closed'
        time.sleep(2)
        return self.connect()

    def connect(self):
        'connect to database'
        if not self.is_connected():
            self.con = self.pgconnect(self.connector,**self.keyattr)
        return 1


    @staticmethod
    def pgconnect(pgconfig:dict,**kwargs)-> str :
        'static method to connect to the database'
        return psycopg2.connect(
            user=pgconfig['user'],
            password=pgconfig['password'],
            host=pgconfig['host'],
            database=pgconfig['database'],
            port=pgconfig['port'],**kwargs)

    @with_connection.reconnect
    @with_connection.update
    def copy_from_csv(self,csvfile,table,header,sep=",",cur=None,con=None):
        "copy from csv or file like object"
        # if con is None:
        #     con=self.con
        # with con.cursor() as cur:
        #     try
        cur.copy_from(
            file=csvfile,
            table=table,
            columns=header,
            sep=sep)
            # except Exception as error:
            #     con.rollback()
            #     self.error=str(error)
            #     return 0
            # else:
            #     con.commit()
            #     return 1

    @with_connection.reconnect
    @with_connection.update
    def dict_insert(self,values:dict,table:str,cur,con):

        query=f"insert into {table} ({','.join(values.keys())}) values ({','.join(['%s' for i in range(len(values))])})"
        self.query = query
        cur.execute(query,tuple(values.values()))
        return 1
        
    @with_connection.reconnect
    @with_connection.update
    def execute(self,query,cur,con):
        self.query=query
        cur.execute(query)
        return 1
    @with_connection.reconnect
    @with_connection.update
    def upd(self,query,cur,con):
        self.query=query
        cur.execute(query)
        return 1
        
    @with_connection.reconnect
    @with_connection.update
    def execute_many(self,query,dataset,cur,con):
        self.query=query
        cur.executemany(query,dataset)
        return 1

    @with_connection.reconnect
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
    def select(self,query,cur=None,con=None,rtype=None,header=0):
        "con,cur are expected to be passed from decorators"
        query=f"select json_agg(t) from ({query}) t" if rtype=='json' else query
        self.query = query
        head=None

        cur.execute(query)

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

    @with_connection.reconnect
    @with_connection.select
    def sel(self,query,cur,con,rtype=None,header=0):

        query=f"select json_agg(t) from ({query}) t" if rtype=='dict' else query
        self.query =  query
        head=None
        try:
            cur.execute(query)
        except Exception as error:
            self.error=str(error)
            return None,0,head
        if header and rtype !='dict' :
            head=[x[0] for x in cur.description]
        data=cur.fetchall()
        if rtype=='list':
            if len(cur.description)==1:
                return [x[0] for x in data],1,head
            else:
                return [list(x) for x in data],1,head
        if rtype=='dict':
            return data[0][0] or [],1,head
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

    def sel(self,query,rtype=None,header=0,cur=None,con=None):
        with self.connect() as con:
            return super().sel(query,rtype=rtype,header=header,cur=cur,con=con)

    def upd(self,query,cur=None,con=None):
        with self.connect() as con:
            return  super().upd(query,cur=cur,con=con)
         
    def dict_insert(self,values:dict,table:str,cur=None,con=None):
        with self.connect() as con:
            return super().dict_insert(values=values,table=table,cur=cur,con=con)


    def copy_from_csv(self,csvfile,table,header,sep=",",con=None):
        with self.connect() as con:
            return super().copy_from_csv(csvfile=csvfile,table=table,header=header,sep=sep,con=con)


class SingletonPg(pg2_base_wrap):
    "It is a singletone class return the same connection always"
    instances:dict = {}
    def __new__(cls,*args,**kwargs):
        name = kwargs.pop('name','default')
        if  not name in cls.instances:
            cls.instances[name] = super().__new__(cls)
        return cls.instances[name]

    def __init__(self, connector:dict,*args,**kwargs):
        kwargs.pop('name','default')
        super().__init__(connector,*args,**kwargs)
