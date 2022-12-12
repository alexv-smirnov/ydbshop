import ydb
import os
import psycopg2
import pymongo
import time
from urllib.parse import urlparse
import socket
import psycopg2.pool
import json
# import ydb.iam
from os.path import expanduser
import timer
from timer import MyTimer
import profile

# MongoDB
# mongoURL = 'mongodb://user1:' + pg_pwd + '@rc1c-p4l3hxws8tjmvsrq.mdb.yandexcloud.net:27018'
# mongoDB = 'db1'

# maxthreads = 60
# maxretries = 0

cockroach_pg = None
pool_ydb = None
driver = None
sql_prefix = ""

def read_bytes(f):
    with open(f, 'rb') as fr:
        return fr.read()

def ping( host, port, ipv6 ):
    ts = time.time()
    if ipv6=="1" or ipv6==1:
        addr_family = socket.AF_INET6
        fs = 'IPv6'
    else:
        addr_family = socket.AF_INET
        fs = 'IPv4'
    with socket.socket(addr_family, socket.SOCK_STREAM) as s:
        s.connect((host, port))
    print( 'Ping ' + fs + ' ' + host+' :',port,': ', round((time.time() - ts)*1000), 'ms' )

def connect_ydb( profileName ):
    x = profile.getDriverParsFromProfile( profileName )
    global ydb_database
    ydb_database = x['database']
    with MyTimer('Driver get'):
        driver = ydb.Driver( **x )
    with MyTimer('Driver wait'):
        driver.wait(timeout=5)
    if timer.timer_on:
        with MyTimer('Create sample session'):
            session_i = driver.table_client.session().create()
    else:
        session_i = None
    with MyTimer('Session pool'):
        sessionPool = ydb.SessionPool(driver)
    return (driver, session_i, sessionPool)


def connect_ydb_old( endpoint, database, sa_key_file, root_certs_file, iam_endpoint ):
    #iam_channel_credentials = {}
    #if root_certs_file > "":
    #    iam_channel_credentials = {'root_certificates': read_bytes(expanduser(root_certs_file))}
    with MyTimer('Credentials'):
        if not sa_key_file is None:
            creds = iam.ServiceAccountCredentials.from_file(
                expanduser(sa_key_file),
            #    iam_channel_credentials=iam_channel_credentials,
                iam_endpoint=iam_endpoint or 'iam.api.cloud.yandex.net:443'
            )
        else:
            creds = ydb.construct_credentials_from_environ() # YDB_TOKEN expected to reference Yandex internal databases
    root_certs = None
    if root_certs_file is not None:
        with MyTimer('Root cert'):
            root_certs = read_bytes(expanduser(root_certs_file))
    with MyTimer('Driver config'):
        driver_config = ydb.DriverConfig(
                endpoint, database,
                credentials=creds,
                root_certificates=root_certs,
                use_all_nodes=True
            )
    with MyTimer('Driver get'):
        driver = ydb.Driver(driver_config)
    with MyTimer('Driver wait'):
        driver.wait(timeout=5)
    if timer.timer_on:
        with MyTimer('Create sample session'):
            session_i = driver.table_client.session().create()
    else:
        session_i = None
    with MyTimer('Session pool'):
        sessionPool = ydb.SessionPool(driver)
    return (driver, session_i, sessionPool)

def connect_pg( connstr, poolsize ):
    conn = psycopg2.connect( connstr )
    pool_pg = psycopg2.pool.ThreadedConnectionPool( poolsize, poolsize, connstr )
    return (conn, pool_pg)

def connect_mongo( poolsize ):
    # MongoDB
    kv=urlparse(mongoURL)
    x=kv.netloc.split('@')
    y=x[1].split(":")
    ping(y[0], int(y[1]))
    client = pymongo.MongoClient(mongoURL, ssl_ca_certs = '/Users/alexv-smirnov/.mongodb/root.crt', ssl_cert_reqs=ssl.CERT_REQUIRED, 
        maxPoolSize = poolsize, minPoolSize = poolsize )
    db = client[mongoDB]
    return (client, db)

def connect( profileName, poolsize ):
    print( 'Connecting to YDB database from CLI profile <' + profileName + '>, pool size:', poolsize )
    ts = time.time()
    global driver, session_i, pool_ydb, ydb_database
    (driver, session_i, pool_ydb) = connect_ydb( profileName )
    print( 'Connected, connection time:', round((time.time() - ts)*1000), 'ms' )


def connect_old( dbname, poolsize ):
    with open('db.json') as f:
        d = json.load(f)
    dbs = d['databases']
    dbj = next((item for item in dbs if item['name']==dbname),None)
    if dbj is None:
        print('Database with the name <' + dbname + '> is not found in db.json')
        exit(1)
    print( 'Connecting to ' + dbj['name'] + ' (' + dbj['caption'] + ' at ' + dbj['host'] +'), pool size:', poolsize )
    ping( dbj.get('hostaddr') or dbj['host'], dbj['port'], dbj.get('ipv6'))
    global dbkind 
    dbkind = dbj['type']
    global sql_prefix
    sql_prefix = dbj.get('sql_prefix',"")
    ts = time.time()
    if dbj['type']=='ydb':
        global driver, session_i, pool_ydb, ydb_database
        endpoint = dbj['protocol'] + '://' + dbj['host'] + ':' + str(dbj['port'])
        ydb_database = dbj['dbname']
        (driver, session_i, pool_ydb) = connect_ydb( 
            endpoint, 
            ydb_database, 
            dbj.get('sa_key_file'), 
            dbj.get('root_cert'),
            dbj.get('iam_endpoint')
        )
    elif dbj['type']=='pg':
        cs = ""
        keys = dbj.keys()
        if 'host' in keys:
            cs = cs + """
            host=""" + dbj['host']
        if 'hostaddr' in keys:
            cs = cs + """
            hostaddr=""" + dbj['hostaddr']
        if 'root_cert' in keys:
            # sslrootcert by default is taken from ~/.postgresql/root.crt
            cs = cs + """
            sslrootcert=""" + expanduser(dbj.get('root_cert'))
        if 'client_cert' in keys and 'client_key' in keys:
            cs = cs + """
            sslcert=""" + expanduser(dbj['client_cert']) + """
            sslkey=""" + expanduser(dbj['client_key']) + """
            user=""" + dbj['user']
        else:
            pwd_var = dbj.get('password_var','PG_PWD')
            pg_pwd = os.getenv(pwd_var)
            if pg_pwd is None:
                raise ValueError('Please specify PostgreSQL password in '+pwd_var+' environment variable')
            cs = cs + """
            user=""" + dbj['user'] + """
            password=""" + pg_pwd
        global conn_pg, pool_pg, cockroach_pg
        conn = cs + """
            port=""" + str(dbj['port']) + """
            dbname=""" + dbj['dbname'] + """
            target_session_attrs=read-write
            sslmode=verify-full
        """
        # print('Conn: ' + conn)
        cockroach_pg = dbj.get('cockroach',False)
        (conn_pg, pool_pg) = connect_pg( conn, poolsize ) 

    else:
        print('Unknown database type: ' + dbj['type'])
        exit(1)
    print( dbname + ' connected, connection time:', round((time.time() - ts)*1000), 'ms' )

def disconnect():
    print('Disconnect')
    if pool_ydb:
        pool_ydb.stop()
    if driver:
        driver.stop()
    

if __name__=='__main__':
    connect('crdbsn',1)