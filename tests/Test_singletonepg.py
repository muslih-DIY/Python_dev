from pg_wraper import SingletonPg,pg2_base_wrap
import psycopg2
import os

env_postgresdb_conf_mapper:dict = {
                        'database':'database' ,
                        'user':'dbuser',
                        'host':'dbhost',
                        'password':'dbpassword',
                        'port':'dbport' }

pgconf_env :dict = {}
for key,var in env_postgresdb_conf_mapper.items():
    if os.environ.get(var,None):pgconf_env[key]=os.environ.get(var,None)


pgcon = SingletonPg(pgconf_env)

try:
    pgcon.connect()
except psycopg2.OperationalError:
    pass



def test_reconnecting_pg():
    """Reconnection testing"""
    ## wrong port given and raised connection error
    pgconf_env['port'] = 5431
    pgcon = pg2_base_wrap(pgconf_env)
    try:
        pgcon.connect()
    except:
        pass
    
    pgconf_env['port'] = 5432

    try:
        pgcon.select('select 1')
    except psycopg2.OperationalError:
        pgcon.__init__(pgconf_env)
        pgcon.connect()
    
    # assert pgcon.select('select 1')

               

# def test_singletone_pg():
#     pass