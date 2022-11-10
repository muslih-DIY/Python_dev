from psycopg2 import OperationalError,InterfaceError
from wrapers.pg_wraper import pg2_base_wrap
import pytest

def basic_connection_test(con_str):
    """
    Test basic connection operation
    """
    pgcon = pg2_base_wrap(con_str)
    pgcon.connector=con_str
    pgcon.connect()
    assert pgcon.select('select 1')
    pgcon.close()

def reconnect_with_new_con_str_test(con_str):
    """
    Test reconnection  after updating connection string
    """

    con_str['port'] = 5431
    with pytest.raises(OperationalError):
        pgcon = pg2_base_wrap(con_str)
        pgcon.connect()
        pgcon.select('select 1')   

    con_str['port'] = 5432
    pgcon.connector=con_str
    pgcon.connect()
    assert pgcon.select('select 1') 
    pgcon.close()             

def reconnecting_closed_con_test(con_str):
    """Reconnection testing of closed connection"""

    con_str['port'] = 5432
    pgcon = pg2_base_wrap(con_str)
    pgcon.connect()
    pgcon.con.close()
    with pytest.raises(InterfaceError):
        pgcon.select('select 1')
    pgcon.connect()
    assert pgcon.select('select 1')