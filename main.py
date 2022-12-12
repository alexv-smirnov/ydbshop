import shop2 as shop
import idx_perf as ip
import initdb3 as initdb
import time
import datetime

maxthreads = 60

def testorder():
    testorder = [{"product":"p"+str(pr),"quantity":2} for (pr) in range(1000)]
    ds = datetime.datetime.now()
    # submitOrder( "Name0", testorder )
    df = datetime.datetime.now()
    print('Order created for ', df - ds )    

def explore():
    with shop.pool_pg.getconn() as conn:
        c = conn.cursor()
        c.execute('insert into ttp values (%s)', (datetime.datetime.now(),))
        conn.commit()
        c.execute('select * from ttp' )
        print(c.fetchall())

    # with shop.pool_ydb.checkout() as s:
    #     pq = s.prepare('declare $x as date;select $x as colx;')
    #     t = s.transaction()
    #     rs = t.execute( pq, {'$x':time.time()})
    #     print(rs[0].rows[0]['colx'])

shop.connect( 1 )
# testorder()
# initdb.run_yql( shop.driver.table_client, shop.database, 150000, 100001 )
# initdb.run_pg( shop.conn_pg, 1000000, 100000 )
# ip.run_yql( shop.driver.table_client, shop.database, 30000, 100001 )
# shop.main( 'ydb', 3, 0, maxthreads, 20 )

explore()

# cProfile.runctx('main()',globals(),locals())

