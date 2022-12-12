import ydb
import initdb3
import time
import datetime
import threading
import json
import app
import argparse
from urllib.parse import urlparse
import argshop
import connect
import random
import picker
import timer
from argshop import Th
import atexit
import logging

def initdb( dbkind, products, quantity ):
    if dbkind=='ydb':
        initdb3.run_ydb( connect.driver.table_client, connect.pool_ydb, connect.ydb_database, products, quantity )
    elif dbkind=='mongo':
        initdb3.run_mongo( connect.mongo_client, connect.mongo_db, products, quantity )
    elif dbkind=='pg':
        initdb3.run_pg( connect.conn_pg, products, quantity, connect.cockroach_pg )
    else:
        raise ValueError("Invalid dbkind")


# orders = [ ("Name" + str(n), [{"product":"p"+str(n*2),"quantity":1},{"product":"p"+str(n*2+1),"quantity":1}]) for n in range(initdb.prodcount//2)]

rlk = threading.Lock()
stotal = 0
ftotal = 0
stotaltime = 0
ftotaltime = 0
# thcnt = []

def prepareSubmitOrderData( a, th:Th, thread=0 ):
    totalprods = 10000
    if a.command in ('submitRandomOrder','insertRandomOrder'):
        pr = picker.pickProducts(picker.nextOrderPositionCount())
        data = (
            "Name" + str(picker.nextProduct()), 
            [ {"product":"p"+("000000"+str(pr[x]))[-6:],"quantity":1} for x in range(len(pr)) ],
            'RC'
        )
    elif a.command=='submitSameOrder':
        data = [ (
            "Name" + str(thread+a.tstart), 
            [ {"product":"p"+("000000"+str(x))[-6:],"quantity":1} for x in range(a.products) ],
            'RC'
        ) for t in range(th.fi_threads)]
    elif a.command=='submitOrder':
        data = [ (
            "Name" + str(t+a.tstart), 
            [ {"product":"p"+("000000"+str(
                (t+a.tstart)*(totalprods//a.partitions) % totalprods + ((t+a.tstart)//a.partitions)*a.products + x
            ))[-6:],"quantity":1} for x in range(a.products) ],
            'RC'
        ) for t in range(th.fi_threads)]
    return data    

def prepareGetOrderByIDData( a, th:Th, thread=0 ):
    data = [( a.orderid, ) for t in range(th.fi_threads)]
    return data    

maxcustomers = None
run_threads = None

def prepareCustomerHistoryData( a, th:Th, thread = 0 ):
    global maxcustomers
    if maxcustomers is None:
        if connect.dbkind=='ydb':
            #with connect.pool_ydb.checkout() as session:
            #    query = session.prepare( "select max(customer) as maxid from orders view ix_cust" )
            #    result_sets = session.transaction(ydb.SerializableReadWrite()).execute( query, {}, commit_tx=True )
            #maxcustomers = int(result_sets[0].rows[0].maxid[4:])
            maxcustomers = 10000
        elif connect.dbkind=='pg':
            conn = connect.pool_pg.getconn()
            try:
                query = "select max(customer) as maxid from orders"
                cur = conn.cursor()
                cur.execute(query)
                result = cur.fetchone()
                maxcustomers = int(result[0][4:])
            finally:
                cur.close()
                connect.pool_pg.putconn( conn )
    # print('Max: ', maxcustomers)  
    # print('Getcustomer called')
    if a.command=='getRandomCustomerHistory':
        c = "Name" + str(random.randint(0, maxcustomers))
        # c = "Name661"
        # print('Random customer: ' + c)
        return (c,)
    elif a.command=='getCustomerHistory':
        return [("Name" + str(maxcustomers),)]*th.fi_threads

def getFunc( command, dbkind ):
    func_exec = None
    func_prepare = None
    prepareOnce = True
    if dbkind=='pg':
        dbpars = ( connect.pool_pg, )
    elif dbkind=='ydb':
        dbpars = ( connect.pool_ydb, )
    elif dbkind=='mongo':
        dbpars = ( connect.mongo_client, connect.mongo_db )

    if command in ('submitOrder','submitRandomOrder','submitSameOrder'):
        func_prepare = prepareSubmitOrderData
        if dbkind=='pg':
            func_exec = app.submitOrder_pg
        elif dbkind=='ydb':
            func_exec = app.submitOrder
        elif dbkind=='mongo':
            func_exec = app.submitOrder_mongo
        if command in 'submitRandomOrder':
            prepareOnce = False
    elif command in ('insertRandomOrder'):
        func_prepare = prepareSubmitOrderData
        prepareOnce = False
        if dbkind=='pg':
            func_exec = app.insertOrder_pg
        elif dbkind=='ydb':
            func_exec = app.insertOrder
        elif dbkind=='mongo':
            func_exec = app.insertOrder_mongo
    elif command in ('getCustomerHistory','getRandomCustomerHistory'):
        func_prepare = prepareCustomerHistoryData
        if dbkind=='ydb':
            func_exec = app.getOrderHistory
        elif dbkind=='pg':
            func_exec = app.getOrderHistory_pg
        if command == 'getRandomCustomerHistory':
            prepareOnce = False
    elif command == 'getOrderByID':
        func_prepare = prepareGetOrderByIDData
        func_exec = app.getOrderByID
    if func_exec is None or func_prepare is None:
        raise ValueError('Cannot match function for command ' + command + ', dbkind ' + dbkind )
    return ( func_exec, func_prepare, dbpars, prepareOnce )

interrupt = False

def repeat( a, th:Th, i, func, funcprep, args ):
    global stotal, ftotal, stotaltime, ftotaltime
    ds = datetime.datetime.now()
    delta = datetime.timedelta( seconds = a.seconds )
    scnt = 0
    stime = 0
    ftime = 0
    fcnt = 0
    ts = time.time()
    while datetime.datetime.now() < ds + delta: 
        global interrupt
        if interrupt:
            break
        try:
            tf = time.time()
            if not funcprep is None:
                iargs = args + funcprep( a, th, i )
            else:
                iargs = args
            func( *iargs )
            scnt = scnt + 1
            stime = stime + time.time()-tf
            # thcnt[i] = thcnt[i] + 1
        except Exception as e:
            fcnt = fcnt + 1
            ftime = ftime + time.time()-tf
            em = str(e).replace("\n", " ")
            if not em.startswith('could not serialize access due to read/write dependencies among transactions') and \
               not em.startswith('message: "Execution" issue_code: 1060 severity: 1 issues { message: "Transaction locks invalidated.'): print( em )
            # raise
        if a.window>0:
            if ts + a.window/10 < time.time():
                ts = time.time()
                rlk.acquire()
                stotal = stotal + scnt
                stotaltime = stotaltime + stime
                ftotaltime = ftotaltime + ftime
                ftotal = ftotal + fcnt
                rlk.release()
                scnt = 0
                stime = 0
                fcnt = 0
                ftime = 0

    rlk.acquire()
    stotal = stotal + scnt
    stotaltime = stotaltime + stime
    ftotaltime = ftotaltime + ftime
    ftotal = ftotal + fcnt
    rlk.release()



def startThreads( a, th: Th ):
    global run_threads
    run_threads = []
    ( func_exec, func_prepare, dbpar, prepareOnce ) = getFunc( a.command, 'ydb' )
    if prepareOnce:
        dataprep = func_prepare( a, th )
    step = 0
    for i in range(th.fi_threads):
        global interrupt
        if interrupt:
            break
        args = dbpar
        if prepareOnce:
            data = dataprep[i]
            fp = None
            args = args + data
        else:
            fp = func_prepare
        t = threading.Thread(target = repeat, args = (a, th, i, func_exec, fp, args ) )
        t.start()
        run_threads.append(t)
        if (i+1) >= th.st_threads + th.step_threads * step:
            time.sleep( th.step_seconds )
            step = step + 1

#def stats( kind, tm, value ):
#    print( kind + ' per', tm, ':', value, ', average per second:', round( value / tm.seconds, 2 ) )

def consumeStats( tstart, i, dmax, threads ):
    # print('Thread orders count:', thcnt )
    global stotal, ftotal, stotaltime, ftotaltime
    rlk.acquire()
    st = stotal
    ft = ftotal
    stm = stotaltime
    ftm = ftotaltime
    stotal = 0
    ftotal = 0
    stotaltime = 0
    ftotaltime = 0
    rlk.release()
    global run_threads
    results = {'th':len(run_threads), 'i':i, 'seconds':dmax, 'success':st, 'fail':ft }
    if st>0:
        results = { **results, **{'btps':round(st/dmax,2 ), 'latency_ms':round( stm * 1000 / st )}}
    if ft>0:
        results = { **results, **{'fail_perc':round(ft / (ft+st) * 100, 2), 'latency_fail_ms':round( ftm * 1000 / ft )}}
    return results

def run( a, th ):
    print( a.dbname + ': Starting ' + a.threads + ' thread(s), for', a.seconds, 'seconds' )
    ts = time.time()
    tinit = ts
    it = 0
    startThread = threading.Thread(target = startThreads, args = (a, th ) )
    startThread.start()

    #x = runThreads( a )
    #if a.window>0:
    try:
        while ts + a.window < tinit + a.seconds + 0.1:
            delta = tinit + (a.window or a.seconds)*(it+1) - time.time()
            # print('Delta:', delta)
            time.sleep( max( delta, 0 ) )
            newt = time.time()
            print( json.dumps(consumeStats( a.tstart, it, newt-ts, a.threads )))
            ts = newt
            it = it + 1
    except KeyboardInterrupt:
        global interrupt
        interrupt = True

    startThread.join()
    global run_threads
    for x in run_threads:
        x.join()
    if a.window == 0:
        print ( json.dumps(consumeStats( a.tstart, it, time.time()-ts, a.threads )))

def termit():
    global interrupt
    interrupt = True
    global run_threads
    if not run_threads is None:
        for x in run_threads:
            x.join()
    connect.disconnect()

def main():
    parser = argparse.ArgumentParser(description='Load generator.')
    argshop.init( parser )
    args = parser.parse_args()
    # print(args)
    th = Th( args.threads )
    if args.command == 'connect':
        timer.timer_on = True
        logging.basicConfig(
        format='%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S')
        l = logging.getLogger('ydb.connection')
        l.setLevel(logging.DEBUG)
    connect.connect(args.dbname,th.fi_threads)
    if args.command == 'connect':
        connect.disconnect()
        return
    app.sql_prefix = connect.sql_prefix
    atexit.register( termit )
    if args.command in ('submitOrder','getCustomerHistory','getRandomCustomerHistory','submitRandomOrder','submitSameOrder',
                        'insertRandomOrder', 'getOrderByID'):
        run( args, th )
    elif args.command=='init':
        initdb( 'ydb', args.products, args.quantity )

main()

