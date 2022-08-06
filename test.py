from ast import Index
from ivrs_interface.contrib.dbs import pg_wraper,or_wraper
from ivrs_interface.dbbackups.config import PgConf,OracleConf


pg = pg_wraper.pg2_base_wrap({
        'user':PgConf.dbuser,
        'password':PgConf.password,
        'host':PgConf.host,
        'database':PgConf.dbname,
        'port':PgConf.port
        })
oracle = or_wraper.oracle_base_wrap({
        'user':OracleConf.dbuser,
        'password':OracleConf.password,
        'sid':OracleConf.sid})

pg.connect()
# pg.insert("insert into api_logs values(now(),'tester')")
pg.upd("update cdr set deletflag=1")
cdr_data,_,head = pg.select("""select CALLDATE::timestamp(6),CONTAINER_ID,CLID,SRC,DST,DCONTEXT,CHANNEL,DSTCHANNEL,LASTAPP,LASTDATA,DURATION,BILLSEC,DISPOSITION,AMAFLAGS,ACCOUNTCODE,UNIQUEID,USERFIELD,PHONENO,CALLSTAGES,ZONE,REGION_NO,STD_CODE,DOCKET_ID,COMP_FLAG,STD_CODE_BILL,PHONENO_BILL,BILL_FLAG,AGENT_REGION,AGENT_LANG,AGENT_SERVICE,AGENT_PURPOSE,AGENT_CUSCAT,AGENT_CLID,AGENT_TELNO,AGENT_ORGTIME,CDRID_AGENT,REPTIME_AGENT,ZONE_BILL,CHANGED_PHONE,TRANSID from cdr""",header=1)

oracle.connect()
#print(head)
errors = oracle.insert_many_list('TEST_CDR_1500',head,cdr_data,batcherrors=True)
keys = ['UNIQUEID']
keyidex = [head.index(k.lower()) for k in keys]
print(keyidex)
error_data =[]
for row,msg in errors:
        error_data.append({head[index]:row[index] for index in keyidex})
        
print(error_data)
upq="update cdr set deletflag=2 where uniqueid=%(uniqueid)s"
pg.execute_many(upq,error_data)
print(pg.error)
oracle.close()
pg.close()