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
import ydb_profile

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
    x = ydb_profile.getDriverParsFromProfile( profileName )
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

def connect_pg( connstr, poolsize ):
    conn = psycopg2.connect( connstr )
    pool_pg = psycopg2.pool.ThreadedConnectionPool( poolsize, poolsize, connstr )
    return (conn, pool_pg)

def connect( profileName, poolsize ):
    print( 'Connecting to YDB database from CLI profile <' + profileName + '>, pool size:', poolsize )
    ts = time.time()
    global driver, session_i, pool_ydb, ydb_database
    (driver, session_i, pool_ydb) = connect_ydb( profileName )
    print( 'Connected, connection time:', round((time.time() - ts)*1000), 'ms' )

def disconnect():
    print('Disconnect')
    if pool_ydb:
        pool_ydb.stop()
    if driver:
        driver.stop()    

if __name__=='__main__':
    connect('crdbsn',1)