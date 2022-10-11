from pg_wraper import SingletonPg,pg2_base_wrap,with_connection
import pg_wraper
import psycopg2
import os
import pytest
from functools import wraps

env_postgresdb_conf_mapper:dict = {
                        'database':'database' ,
                        'user':'dbuser',
                        'host':'dbhost',
                        'password':'dbpassword',
                        'port':'dbport' }

pgconf_env :dict = {}
for key,var in env_postgresdb_conf_mapper.items():
    if os.environ.get(var,None):pgconf_env[key]=os.environ.get(var,None)

def Mockreconnectdec(function):
    
    @wraps(function)
    def inner(self,*args,**kwargs): 
        f = None
        for _ in range(self.retry_max+1):                
            try:
                if f is None: return function(self,*args,**kwargs)
                return f(self,*args,**kwargs)
                
            except psycopg2.OperationalError:
                print("except")
                if self.retry_max != 0 :
                    f = self.reconnect()
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


def reconnecting_operation_error_test():
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

def reconnecting_test(monkeypatch):
    """Reconnection testing of OperationalError"""
    original_select = pg_wraper.pg2_base_wrap.select._original
    


    @Mockreconnectdec
    @with_connection.select
    def mockoperationerror(*args,**kwargs):
        raise psycopg2.OperationalError
    
    def mockreconnect(self,*args,**kwargs):
        # monkeypatch.delattr(pg2_base_wrap,'select',pg2_base_wrap.select)
        monkeypatch.setattr(pg2_base_wrap,'select',original_select)
        # (pgcon,'select',mockoperationerror)
        print("Reconnecting..")
        return original_select

    
    monkeypatch.setattr(pg2_base_wrap,'reconnect',mockreconnect)
    monkeypatch.setattr(pg2_base_wrap,'select',mockoperationerror)
    pgconf_env['port'] = 5432
    pgcon = pg2_base_wrap(pgconf_env,retry_max=2)
    pg2_base_wrap.connect(pgcon)
    print(pg2_base_wrap.select(pgcon,"select 1"))
    
