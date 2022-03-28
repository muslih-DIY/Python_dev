from pg_wraper import pg2_wrap

pgd = pg2_wrap({
    'user':'postgres',
    'pass':'',
    'host':'127.0.0.1',
    'database':'postgres',
    'port':'5432'})

command="select * from information_schema.tables"
data_normal,status,header = pgd.sel(command,header=1)
print('normal',data_normal)

data_list,status,header = pgd.sel(command,rtype='list',header=1)
print('data_list',data_list)

data_dict,status,header = pgd.sel(command,rtype='dict',header=1)
print('data_dict',data_dict)

pgd.close()
