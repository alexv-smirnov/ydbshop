import threading
import ydb
import psycopg2
import psycopg2.extras
import random
import datetime
import pymongo
import time
from timer import MyTimer
import connect

maxid = 2**64

sql_prefix = ""

def getNextId():
    return random.randrange( maxid )

def insertOrder_pg_int( conn, customer, lines ):
    # Insert order data ==============================================================
    # pq_addord_header = "INSERT INTO orders( id, customer, created ) values ( nextval('seq_orders'), %($cust)s, Now()) RETURNING id"
    pq_addord_header = sql_prefix + "INSERT INTO orders( id, customer, created ) values ( %($ido)s, %($cust)s, Now())" #
    pq_addord_lines = sql_prefix + "INSERT INTO orderLines( id_order, product, quantity ) values %s"
    t = conn.cursor()
    idOrder = getNextId()-2**63 #
    t.execute( pq_addord_header, {'$cust':customer,'$ido':idOrder } ) #
    # t.execute( pq_addord_header, {'$cust':customer} )
    # idOrder = t.fetchone()[0]
    lines2 = [(idOrder,lines[x]['product'],lines[x]['quantity']) for x in range(len(lines))]
    psycopg2.extras.execute_values( t, pq_addord_lines, lines2 )
    conn.commit()
    return idOrder
    
def executeOrder_pg( conn, idOrder ):
    # Update stock and order status ==================================================
    pq_mark_processed = sql_prefix + "UPDATE orders SET processed = Now() WHERE id = %($ido)s"
    pq_getqty = sql_prefix + """
        SELECT p.product as prod, COALESCE(s.quantity,0)-p.quantity AS qnew
        FROM   orderLines as p
        JOIN stock AS s on s.product = p.product
        WHERE  p.id_order = %s FOR UPDATE of s
    """
    pq_updqty = sql_prefix + """
        UPDATE stock s 
        SET    quantity = l.qnew
        FROM   (values %s) as l (prod, qnew)
        WHERE  s.product = l.prod
    """
    t = conn.cursor()
    # Get stock
    t.execute( pq_getqty, ( idOrder, ) )
    linesnew = t.fetchall()
    #if len(linesnew)<len(lines):
    #    raise ValueError( 'Not all products are on the stock list' )
    # Verify availability
    for x in linesnew:
        if x[1]<0:
            conn.rollback()
            raise ValueError( 'Not enough product ' + x[0] )
    # Update stock
    psycopg2.extras.execute_values( t, pq_updqty, linesnew )
    t.execute( pq_mark_processed, {'$ido':idOrder})
    conn.commit()
    return idOrder

def cockroach_execute( conn, op ):
    retries = 0
    max_retries = 5
    with conn:
        while True:
            retries +=1
            if retries == max_retries:
                err_msg = "Transaction did not succeed after {} retries".format(max_retries)
                raise ValueError(err_msg)

            try:
                return op(conn)

                # If we reach this point, we were able to commit, so we break
                # from the retry loop.
                # break
            except psycopg2.Error as e:
                # print( "e.pgcode: {}".format(e.pgcode) )
                # logging.debug("e.pgcode: {}".format(e.pgcode))
                
                if e.pgcode == '40001':
                    # print('psycopg2.Error 40001')
                    # This is a retry error, so we roll back the current
                    # transaction and sleep for a bit before retrying. The
                    # sleep time increases for each failed transaction.
                    conn.rollback()
                    # logging.debug("EXECUTE SERIALIZATION_FAILURE BRANCH")
                    sleep_ms = (2**retries) * 0.1 * (random.random() + 0.5)
                    # logging.debug("Sleeping {} seconds".format(sleep_ms))
                    time.sleep(sleep_ms)
                    continue
                else:
                    # logging.debug("EXECUTE NON-SERIALIZATION_FAILURE BRANCH")
                    raise e


def submitOrder_pg( pool, customer, lines, isolation_level=None ):
    conn = pool.getconn()
    try:
        idOrder = insertOrder_pg_int( conn, customer, lines )
        cockroach_execute( conn, lambda conn: executeOrder_pg( conn, idOrder ))

        # idOrder = insertOrder_pg( conn, customer, lines )
        # executeOrder_pg( conn, idOrder )
    finally:
        pool.putconn( conn )

def insertOrder_pg( pool, customer, lines, isolation_level=None ):
    conn = pool.getconn()
    try:
        insertOrder_pg_int( conn, customer, lines )
    finally:
        pool.putconn( conn )


def submitOrder_mongo( client, db, customer, lines, isolation_level ):

    with client.start_session() as session:
        idOrder = getNextId()
        lines2 = [{"idOrder":idOrder,"product":lines[x]['product'],"quantity":lines[x]['quantity']} for x in range(len(lines))]
        prods = [lines[x]['product'] for x in range(len(lines))]
        with session.start_transaction( read_concern=pymongo.read_concern.ReadConcern('majority'), write_concern=pymongo.write_concern.WriteConcern('majority')):
            db.orders.insert_one({'_id':idOrder,'customer':customer,'created':datetime.datetime.now()}, session = session)
            db.orderLines.insert_many( lines2, session = session )
        
        with session.start_transaction( read_concern=pymongo.read_concern.ReadConcern('snapshot'), write_concern=pymongo.write_concern.WriteConcern('majority')):
            for x in lines2:
                s = db.stock.find_one({"_id":x['product']}, session = session )
                if s is None:
                    raise ValueError( 'Product not on the stock list ' + x['product'] )
                nv = s['quantity'] - x['quantity']
                if nv<0:
                    raise ValueError( 'Not enough product ' + x['product'] )
                s['quantity'] = nv
                # time.sleep(5)
                db.stock.replace_one( {"_id":x['product']}, s, session = session )
            # cursor = db.stock.find({"_id":{"$in":prods}}, session = session )

    return idOrder

def insertOrder( pool, customer, lines, isolation_level=None ):
    # print('Enter')
    idOrder = getNextId()
    orderCaption = 'Order '+str(idOrder)

    def tr( session ):
        # insert order data
        # PRAGMA kikimr.UseNewEngine = "true";
        pq_addord = session.prepare( sql_prefix + """
            DECLARE $ido AS UInt64;DECLARE $cust as Utf8;
            DECLARE $lines AS List<Struct<product:Utf8,quantity:UInt64>>;
            DECLARE $time AS DateTime;
            INSERT INTO orders( id, customer, created ) values ($ido, $cust, $time);
            UPSERT INTO orderLines( id_order, product, quantity ) SELECT $ido, product, quantity from AS_TABLE( $lines );
        """)
        t = session.transaction(ydb.SerializableReadWrite())
        with MyTimer(orderCaption + ', execute addord + commit'): 
            t.execute( pq_addord, {"$ido":idOrder,"$cust":customer,"$lines":lines,"$time":int(datetime.datetime.now().timestamp())}, commit_tx = True )

    pool.retry_operation_sync( tr )
    return idOrder

def executeOrder( pool, idOrder):
    orderCaption = 'Order '+str(idOrder)
    def tr( session ):
        # modify all if enough products
        # PRAGMA kikimr.UseNewEngine = "true";
        pq_update = session.prepare( sql_prefix + """
            DECLARE $ido AS UINT64;
            DECLARE $time AS DateTime;
            $prods = SELECT * FROM orderLines as p WHERE p.id_order = $ido;
            $cnt = SELECT count(*) FROM $prods;
            $newq = SELECT p.product AS product, COALESCE(s.quantity,0)-p.quantity AS quantity
                    FROM   $prods as p LEFT JOIN stock AS s on s.product = p.product;
            $check = SELECT count(*) as cntd FROM $newq as q where q.quantity >= 0;
            UPSERT INTO stock SELECT product, quantity FROM $newq where $check=$cnt;
            $upo = SELECT id, $time as tm FROM orders WHERE id = $ido and $check = $cnt;
            UPSERT INTO orders SELECT id, tm as processed FROM $upo;
            SELECT * from $newq as q where q.quantity < 0
        """)
        t = session.transaction(ydb.SerializableReadWrite())
        with MyTimer(orderCaption + ', update stock and order + commit'): 
            res = t.execute( pq_update, {"$ido":idOrder, "$time":int(datetime.datetime.now().timestamp())}, commit_tx = True )[0].rows
            ne = False
            sl = ''
            for p in res:
                ne = True
                sl = sl + ', ' + p.product
            if ne:
                t.rollback()
                raise ValueError( 'Not enough products: ' + sl[:-2] )
    
    pool.retry_operation_sync( tr )

def submitOrder( pool, customer, lines, isolation_level=None ):
    idOrder = insertOrder( pool, customer, lines )
    executeOrder( pool, idOrder )

def causeError( pool : ydb.SessionPool ):
    result = [None]
    def getit( session ):

        query = session.prepare( """weird sql""" )
        result[0] = session.transaction(ydb.StaleReadOnly()).execute(
            query, { "$ido": id }, commit_tx=True
        )[0].rows
    
    pool.retry_operation_sync( getit )

    return result[0]

def getOrderByID( pool : ydb.SessionPool, id ):
    result = [None]
    def getit( session ):

        #query = """
        query = session.prepare( sql_prefix + """
            DECLARE $ido as UInt64;
            select * 
            from   orders
            where  id = $ido
        """
        )
        result[0] = session.transaction(ydb.StaleReadOnly()).execute(
            query, { "$ido": id }, commit_tx=True
        )[0].rows
    
    pool.retry_operation_sync( getit )

    return result[0]

def getOrderHistory( pool : ydb.SessionPool, customer, limit=10 ):
    # print('Customer: ', customer )
    result = [None]
    def getit( session ):

        #query = """
        # PRAGMA kikimr.UseNewEngine = "true";
        #    -- filler comment: """ + "X"*9000 + """
        query = session.prepare( sql_prefix + """
            DECLARE $cust as Utf8;
            DECLARE $limit as UInt32;
            select * 
            from   orders view ix_cust
            where  customer = $cust
            order by customer desc, created desc
            limit $limit;
        """
        )
        #result[0] = session.transaction(ydb.StaleReadOnly()).execute(
        #    ydb.DataQuery(  query, 
        #                    {'$cust':ydb.PrimitiveType.Utf8, '$limit': ydb.PrimitiveType.Uint32}),
        #                    { "$cust": customer, "$limit": limit }, commit_tx=True
        #)[0].rows
        result[0] = session.transaction(ydb.StaleReadOnly()).execute(
            query, { "$cust": customer, "$limit": limit }, commit_tx=True
        )[0].rows
    
    pool.retry_operation_sync( getit )

    return result[0]

def getOrderHistory_pg( pool, customer, limit=10 ):
    conn = pool.getconn()
    try:
        query = sql_prefix + "select * from orders where customer = %s order by customer desc, created desc limit %s"
        cur = conn.cursor()
        cur.execute(query, ( customer, limit ))
        result = cur.fetchall()
        return result
    except psycopg2.Error as e:
        raise ValueError( 'PG Error Code:', e.pgcode, ', message:', e.pgerror )
    finally:
        cur.close()
        pool.putconn( conn )
