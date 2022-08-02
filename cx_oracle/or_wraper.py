from typing import Tuple
import cx_Oracle 
from io import StringIO
import csv
"""
coder: muslih
SID can be generated using cx_Oracle.makedsn("oracle.sub.example.com", "1521", "ora1")
location: https://raw.githubusercontent.com/muslih-DIY/Python_dev/master/module/cx_oracle_wrapper/or_wraper.py
version : 2
update_date :24-06-2022 
"""

class oracle_base_wrap:

    def __init__(self,connector,**kwargs):
        self.connector=connector
        self.keyattr=kwargs
        self.con = None
        self.query=''
        self.error=''

    def close(self):
        self.con.close()

    def is_connected(self):
        if self.con is None:return 0
  
    
    def re_connect_if_not(self):
        pass

    def connect(self):
        if self.con is None:
            self.con = self.orconnect(self.connector,**self.keyattr)
        return 1


    @staticmethod
    def orconnect(connector,**kwargs):
        return cx_Oracle.connect(connector['user'],connector['password'],connector['sid'],**kwargs)

    def upd(self,query,con=None):
        if con is None:con=self.con
        self.query=query
        """ will committ if no error found otherwise rollback itself(no need to commit or rollback externally when using as function)"""
        self.error=''
        with con.cursor() as cur:
            try:cur.execute(self.query)
            except Exception as E:
                self.con.rollback()
                self.error=str(E)
                return 0
            else:
                self.con.commit()
                return 1
                
    def dict_insert(self,values:dict,table:str,con=None):
        if con is None:con=self.con
        self.query=f"insert into {table} ({','.join(values.keys())}) values ({','.join(['%s' for i in range(len(values))])})"
        with con.cursor() as cur:
            try:
                cur.execute(self.query,tuple(values.values()))
            except Exception as E:
                self.con.rollback()
                self.error=str(E)
                return 0
            else:
                self.con.commit()
                return 1


    def sel(self,query,rtype=None,header=0,con=None):
        if con is None:con=self.con
        self.query=query
        head=None
        with con.cursor() as cur:
            try:
                cur.execute(self.query)
            except Exception as E:
                self.error=str(E)
                return None,0,head

            if rtype=='dict':
                cur.rowfactory = lambda *args: dict(zip([d[0] for d in cur.description], args))
                data = cur.fetchall()
                return data,1,head
            if header and rtype !='dict' :
                head=[x[0] for x in cur.description]
            data=cur.fetchall()
            if rtype=='list':
                if len(cur.description)==1:
                    return [x[0] for x in data],1,head
                else:
                    return [list(x) for x in data],1,head
            return data,1,head
        
    def sel_to_IOstring(self,query,fdata:Tuple=None,arraysize:int=500,headcase=str.upper,con=None):
        """
        Return:
            => StringIO,status,heads

        headcase  
        ----
            :   str.lower
            :   str.upper
        ---
        data:list
        For adding new fixed fdata into the csv     
        """
        self.query=query
        if con is None:con=self.con
        head=None
        if fdata is not None and not isinstance(fdata,tuple):
            raise TypeError("data => should be Tuple eg: (4,) or (2,4)")

        with con.cursor() as cur:
            try:
                cur.arraysize=arraysize
                cur.execute(self.query)
            except Exception as E:
                self.error=str(E)
                return None,0,head
            else:
                sio = StringIO()
                writer = csv.writer(sio)
                if not fdata:
                    writer.writerows(cur.fetchall())                                  
                else:
                    #print([(*fdata,*row) for row in cur])
                    [writer.writerows([(*fdata,*row)]) for row in cur if row ]    
                sio.count = cur.rowcount
                sio.len = sio.tell()
                sio.seek(0)
                return sio,1,[headcase(x[0]) for x in cur.description]   
        return None,0,None


class oracle_wrap(oracle_base_wrap):
    def __init__(self, connector,**kwargs):
        super().__init__(connector,**kwargs)
        self.connect()

class oracle_thread_pooled(oracle_base_wrap):
    def  __init__(self, connector,min=1,max=3,increment=1,threaded=False,**kwargs):
        super().__init__(connector, **kwargs)
        self.min = min
        self.max = max
        self.inc = increment
        self.threaded = threaded
        self.pool = cx_Oracle.SessionPool(self.connector['user'],self.connector['password'],self.connector['sid'], min=self.min,
                             max=self.max, increment=self.inc,threaded =self.threaded, getmode = cx_Oracle.SPOOL_ATTRVAL_WAIT)
        
    def close(self):
        self.pool.close()

    def is_connected(self):
        pass
      
    def re_connect_if_not(self):
        pass

    def connect(self):
        pass

    def sel(self,query,rtype=None,header=0,con=None):
        with self.pool.acquire() as con:
            return super().sel(query,rtype=rtype,header=header,con=con)

    def upd(self,query,con=None):
        with self.pool.acquire() as con:
            return super().upd(query,con=con)
    def dict_insert(self,values:dict,table:str,con=None):
        with self.pool.acquire() as con:
            return super().dict_insert(values=values,table=table,con=con)
    def sel_to_IOstring(self,query,fdata:Tuple=None,arraysize:int=500,headcase=str.upper,con=None):
        with self.pool.acquire() as con:
            return super().sel_to_IOstring(query,fdata=fdata,arraysize=arraysize,headcase=headcase,con=con)