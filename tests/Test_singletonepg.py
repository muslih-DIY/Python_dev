import time
from dbwraper.pg_wraper import SingletonPg
import concurrent.futures



def singleton_test(con_str):
    "working of singletone postgres connection"
    con_str['port'] = 5432
    pgcon1 = SingletonPg(con_str,name=__name__)
    pgcon2 = SingletonPg(con_str,name=__name__+'2')
    pgcon3 = SingletonPg(con_str,name=__name__)
    pgcon1.connect()

    assert pgcon1 != pgcon2
    assert pgcon1 == pgcon3
    pgcon1.close()

def singleton_multithreaded_test(con_str):
    "working of singletone with multiple thread"
    con_str['port'] = 5432
    pgcon1 = SingletonPg(con_str,name=__name__)
    pgcon1.connect()
    request_count = 1000
    with concurrent.futures.ThreadPoolExecutor(5) as executor:
        start = time.perf_counter()
        threads= [executor.submit(pgcon1.select,'select * from cdr ') for i in range(request_count)]
        comp = concurrent.futures.as_completed(threads)
        # duration = time.perf_counter() - start
        result = [f.result() for f in comp]

    assert len(threads) == request_count
    assert len(result) == request_count
    duration = time.perf_counter() - start

    print(f"singletone performance with {request_count=} {duration=}")
    pgcon1.close()
