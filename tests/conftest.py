import pytest


@pytest.fixture()
def con_str():
    "collect environment variable and create connection dictionary"
    pgconf_env = {
                'database':'postgres' ,
                'user':'postgres',
                'host':'localhost',
                'password':'postgres',
                'port':'5432' }
    return pgconf_env