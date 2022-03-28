import psycopg2



"""
coder: muslih
"""


class pg2_wrap:

    def __init__(self,connector):
        self.connector=connector
        self.con=self.pgconnect(connector)
        self.command=''
        self.error=''

    def close(self):
        self.con.close()


    @staticmethod
    def pgconnect(pgconfig):
        return psycopg2.connect(
            user=pgconfig['user'],
            password=pgconfig['pass'],
            host=pgconfig['host'],
            database=pgconfig['database'],
            port=pgconfig['port'])


    def upd(self,command):

        self.command=command
        """pgupd will committ if no error found otherwise rollback itself(no need to commit or rollback externally when using as function)"""
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


    def sel(self,command,rtype=None,header=0):
        self.command=f"select json_agg(t) from ({command}) t" if rtype=='dict' else command
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
            if rtype=='dict': return data[0][0] or [],1,head
            return data,1,head
