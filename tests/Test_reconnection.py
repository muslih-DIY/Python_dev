from functools import wraps
import time
import psycopg2
import pytest
from wrapers.pg_wraper import pg2_base_wrap,with_connection
from wrapers import pg_wraper

"""_summary_
    This test actually mock the disconnection during the operation
    select method is mocked to raise OperationalError
    reconnection decorator also mocked to get a reconnected operation by replacing select method with a valid response
"""

def mock_reconnect_decorator(function):
    """
    Reconnection Decorator only for mocking the reconection 
    """
    @wraps(function)
    def inner(self,*args,**kwargs): 
        f = None
        for i in range(self.retry_max+1):                
            try:
                if f is None: 
                    return function(self,*args,**kwargs)
                return f(self,*args,**kwargs)
                
            except psycopg2.OperationalError:
                if self.retry_max != 0 :
                    time.sleep((i+1)*self.retry_step)
                    f = self.reconnect(i)
                    continue
                raise
    return inner




         

@pytest.mark.skip
def reconnecting_test(monkeypatch,con_str):
    """Reconnection testing of OperationalError"""

    original_select = pg_wraper.pg2_base_wrap.select   

    @mock_reconnect_decorator
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
    con_str['port'] = 5432
    pgcon = pg2_base_wrap(con_str,retry_max=2)
    pg2_base_wrap.connect(pgcon)
    assert pg2_base_wrap.select(pgcon,"select 1")
    assert pg2_base_wrap.select(pgcon,"select * from cdr limit 2")
