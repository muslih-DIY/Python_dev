import datetime
import pytest
from dotenv import load_dotenv
import os
from dbwraper.or_wraper import oracle_base_wrap,cx_Oracle
import traceback


load_dotenv()

USER = os.environ.get('ORACLE_USER')
PASSWORD = os.environ.get('ORACLE_PASSWORD')
HOST = os.environ.get('ORACLE_HOST')
PORT = os.environ.get('ORACLE_PORT')
DBNAME = os.environ.get('ORACLE_DBNAME')

connector = {}
connector['user'] = USER
connector['password'] = PASSWORD
connector['sid'] = f"{HOST}:{PORT}/{DBNAME}"

database  = oracle_base_wrap(connector)
database.connect()

def oracle_timestamp_neglect_ms_test():
    data = [(datetime.datetime(2023, 8, 25, 15, 54, 17, 715467), datetime.datetime(2023, 2, 1, 1, 30, 1, 524419)), (datetime.datetime(2023, 8, 25, 15, 54, 17, 715467), datetime.datetime(2023, 2, 25, 7, 58, 21, 460504)), (datetime.datetime(2023, 8, 25, 15, 54, 17, 715467), datetime.datetime(2023, 1, 28, 9, 17, 57, 792684)), (datetime.datetime(2023, 8, 25, 15, 54, 17, 715467), datetime.datetime(2023, 1, 28, 13, 23, 22, 946983)), (datetime.datetime(2023, 8, 25, 15, 54, 17, 715467), datetime.datetime(2023, 1, 28, 0, 2, 50, 337668)), (datetime.datetime(2023, 8, 25, 15, 54, 17, 715467), datetime.datetime(2023, 1, 28, 0, 2, 50, 515776)), (datetime.datetime(2023, 8, 25, 15, 54, 17, 715467), datetime.datetime(2023, 1, 28, 0, 4, 3, 569171)), (datetime.datetime(2023, 8, 25, 15, 54, 17, 715467), datetime.datetime(2023, 1, 28, 0, 6, 40, 539905)), (datetime.datetime(2023, 8, 25, 15, 54, 17, 715467), datetime.datetime(2023, 1, 28, 0, 6, 40, 717301)), (datetime.datetime(2023, 8, 25, 15, 54, 17, 715467), datetime.datetime(2023, 1, 28, 0, 7, 12, 784816))]
    database.upd('truncate table DATA_TYPE_TEST')
    error = database.insert_many_list('DATA_TYPE_TEST',['ENTRYDATE','DATETIME'],dataset=data,batcherrors=True)

    assert error == []

    data_read,status,er = database.select('select ENTRYDATE from DATA_TYPE_TEST where rownum<2',rtype=list)

    # datetime is not matching since the ms is missing
    assert data_read[0][0]!=data[0][0]

    database.upd('truncate table DATA_TYPE_TEST')
    # adding type
    dtype = [None,cx_Oracle.TIMESTAMP]
    error = database.insert_many_list('DATA_TYPE_TEST',['ENTRYDATE','DATETIME'],dataset=data,batcherrors=True,dtype=dtype)
    assert error == []


    # verify the TIMESTAMP is updated for second column only 

    data_read,status,er = database.select('select ENTRYDATE,DATETIME from DATA_TYPE_TEST where rownum<2',rtype=list)

    assert data_read[0][0]!=data[0][0]
    assert data_read[0][1]==data[0][1]



    database.upd('truncate table DATA_TYPE_TEST')
    # adding type
    dtype = [cx_Oracle.TIMESTAMP,cx_Oracle.TIMESTAMP]
    error = database.insert_many_list('DATA_TYPE_TEST',['ENTRYDATE','DATETIME'],dataset=data,batcherrors=True,dtype=dtype)
    assert error == []


    # verify the TIMESTAMP is updated for second column only 

    data_read,status,er = database.select('select ENTRYDATE,DATETIME from DATA_TYPE_TEST where rownum<2',rtype=list)


    assert data_read[0][0]==data[0][0]
    assert data_read[0][1]==data[0][1]

    



