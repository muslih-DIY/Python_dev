import cx_Oracle as ora
"""
coder: muslih
SID can be generated using cx_Oracle.makedsn("oracle.sub.example.com", "1521", "ora1")
"""

class oracle_wrap:

    def __init__(self,connector):
        self.connector=connector
        self.con = self.orconnect(connector)
        self.command=''
        self.error=''

    def close(self):
        self.con.close()


    @staticmethod
    def orconnect(connector):
        return ora.connect(connector['user'],connector['password'],connector['sid'])

    def upd(self,command):

        self.command=command
        """ will committ if no error found otherwise rollback itself(no need to commit or rollback externally when using as function)"""
        self.error=''
        with self.con.cursor() as cur:
            try:cur.execute(self.command)
            except Exception as E:
                self.con.rollback()
                self.error=str(E)
                return 0
            else:
                self.con.commit()
                return 1
                
    def dict_insert(self,values:dict,table:str):
        self.command=f"insert into {table} ({','.join(values.keys())}) values ({','.join(['%s' for i in range(len(values))])})"
        with self.con.cursor() as cur:
            try:
                cur.execute(self.command,tuple(values.values()))
            except Exception as E:
                self.con.rollback()
                self.error=str(E)
                return 0
            else:
                self.con.commit()
                return 1


    def sel(self,command,rtype=None,header=0):
        self.command=command
        head=None
        with self.con.cursor() as cur:
            try:
                cur.execute(self.command)
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
        
    def sel1(self,command,rtype=None,header=0):
        #self.command=f"select json_agg(t) from ({command}) t" if rtype=='dict' else command
        self.command=command
        head=None
        with self.con.cursor() as cur:
            try:
                cur.execute(self.command)
            except Exception as E:
                self.error=str(E)
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
                cur.execute(self.command)
                columns = [col[0] for col in cur.description]
                cur.rowfactory = lambda *args: dict(zip(columns, args))
                data = cur.fetchall()
                return(data,1,1)
                #return data[0][0] or [],1,head
            return data,1,head
