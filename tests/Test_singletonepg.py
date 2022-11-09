import psycopg2
import os,time
import pytest
from functools import wraps
from src.pg_wraper import SingletonPg,pg2_base_wrap,with_connection
from src import pg_wraper

env_postgresdb_conf_mapper:dict = {
                        'database':'database' ,
                        'user':'dbuser',
                        'host':'dbhost',
                        'password':'dbpassword',
                        'port':'dbport' }


pgconf_env :dict = {}
for key,var in env_postgresdb_conf_mapper.items():
    if os.environ.get(var,None):pgconf_env[key]=os.environ.get(var,None)


def Mockreconnect_Deco(function):
    """
    Reconnection Decorator only for mocking the reconection
    """
    @wraps(function)
    def inner(self,*args,**kwargs): 
        f = None
        for i in range(self.retry_max+1):                
            try:
                if f is None: return function(self,*args,**kwargs)
                return f(self,*args,**kwargs)
                
            except psycopg2.OperationalError:
                if self.retry_max != 0 :
                    time.sleep((i+1)*self.retry_step)
                    f = self.reconnect(i)
                    continue
                raise
    return inner

def reconnecting_closed_con_test():
    """Reconnection testing of closed connection"""

    pgconf_env['port'] = 5432
    pgcon = pg2_base_wrap(pgconf_env)
    pgcon.connect()
    pgcon.close()
    with pytest.raises(psycopg2.InterfaceError):
        pgcon.select('select 1')
    pgcon.connect()
    assert pgcon.select('select 1')


def connecting_operation_error_test():
    """Reconnection testing of OperationalError"""

    pgconf_env['port'] = 5431

    with pytest.raises(psycopg2.OperationalError):
        pgcon = pg2_base_wrap(pgconf_env)
        pgcon.connect()
        pgcon.select('select 1')   

    pgconf_env['port'] = 5432
    pgcon.__init__(pgconf_env)
    pgcon.connect()
    assert pgcon.select('select 1')               

@pytest.mark.skip
def reconnecting_test(monkeypatch):
    """Reconnection testing of OperationalError"""
    
    original_select = pg_wraper.pg2_base_wrap.select    

    @Mockreconnect_Deco
    @with_connection.select
    def mockoperationerror(*args,**kwargs):
        "A function rises Operationalerror"
        raise psycopg2.OperationalError
    
    def mockreconnect(self,*args,**kwargs):
        "moking reconnection function which change the function"
        print(f"Reconnecting {args[0]}")
        if args[0]<1:return None
        monkeypatch.setattr(pg2_base_wrap,'select',original_select)
        print("Connected")
        return original_select._inner

    
    monkeypatch.setattr(pg2_base_wrap,'reconnect',mockreconnect)
    monkeypatch.setattr(pg2_base_wrap,'select',mockoperationerror)
    pgconf_env['port'] = 5432
    pgcon = pg2_base_wrap(pgconf_env,retry_max=2)
    pg2_base_wrap.connect(pgcon)
    assert pg2_base_wrap.select(pgcon,"select 1")
    assert pg2_base_wrap.select(pgcon,"select * from cdr limit 2")

def singleton_test():
    "working of singletone postgres connection"
    pgconf_env['port'] = 5432
    pgcon1 = SingletonPg(pgconf_env,name=__name__)
    pgcon2 = SingletonPg(pgconf_env,name=__name__+'2')
    pgcon3 = SingletonPg(pgconf_env,name=__name__)
    pgcon1.connect()

    assert not pgcon1 == pgcon2
    assert pgcon1 == pgcon3
    pgcon1.close()

def singleton_multithreaded_test():
    "working of singletone with multiple thread"
    import concurrent.futures
    pgconf_env['port'] = 5432
    pgcon1 = SingletonPg(pgconf_env,name=__name__)
    pgcon1.connect()
    request_count = 1000
    with concurrent.futures.ThreadPoolExecutor(5) as executor:
        start = time.perf_counter()
        threads = [executor.submit(pgcon1.select,'select * from cdr ') for i in range(request_count)]
        comp = concurrent.futures.as_completed(threads)
        # duration = time.perf_counter() - start
        result = [f.result() for f in comp]

    assert len(threads) == request_count
    assert len(result) == request_count
    duration = time.perf_counter() - start

    print(f"singletone performance with {request_count=} {duration=}")
    pgcon1.close()