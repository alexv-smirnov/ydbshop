import ydb
import datetime
import psycopg2.extras

schema = [
    {   'tablename':'stock',
        'columns':[
            {'product':ydb.PrimitiveType.Utf8},
            {'quantity':ydb.PrimitiveType.Int64}
        ],
        'uniformPartitions':20,
        'partitionByLoad':1,
        'pk':['product']
    },
    {   'tablename':'orders',
        'columns':[
            {'id':ydb.PrimitiveType.Uint64},
            {'customer':ydb.PrimitiveType.Utf8},
            {'created':ydb.PrimitiveType.Datetime},
            {'processed':ydb.PrimitiveType.Datetime}
        ],
        'uniformPartitions':20,
        'partitionByLoad':1,
        'pk':['id'],
        'indexes':[
            { 'name':'ix_cust', 'columns': ['customer','created'] }
        ],
        'read_replicas_per_az':1
    },
    {   'tablename':'orderLines',
        'columns':[
            {'id_order':ydb.PrimitiveType.Uint64},
            {'product':ydb.PrimitiveType.Utf8},
            {'quantity':ydb.PrimitiveType.Uint64}
        ],
        'uniformPartitions':20,
        'partitionByLoad':1,
        'pk':['id_order','product']
    },
    {   'sequencename':'seq_orders'
    }
]


def pgType( yqlType ):
    ret = 'unknown'
    if (yqlType == ydb.PrimitiveType.Utf8): ret = 'Varchar'
    elif (yqlType == ydb.PrimitiveType.Uint64): ret = 'BigInt'
    elif (yqlType == ydb.PrimitiveType.Int64): ret = 'BigInt'
    elif (yqlType == ydb.PrimitiveType.Datetime): ret = 'Timestamp'
    return ret

def run_pg( conn, pcount, quantity = 5000, cockroach = False ):
    stock_pg = [ ( "p" + ("000000" + str(n))[-6:], quantity+n ) for n in range(pcount)]
    filldata_pg = {'stock':stock_pg }
    # Generate tables if not exist
    cur = conn.cursor()
    cols = {}
    pkcols = {}
    npkcols = {}
    for t in schema:
        if 'tablename' in t.keys():
            cdef = ''
            collist = ''
            npklist = []
            for c in t['columns']:
                colname = list(c.keys())[0]
                cdef = cdef + ', ' + colname + ' ' + pgType(c[colname])
                collist = collist + ', ' + colname
                if not colname in t['pk']: npklist.append( colname )
            pk = (', ').join(t['pk'])
            q = 'CREATE TABLE IF NOT EXISTS ' + t['tablename'] + '( ' + cdef[2:] + ', PRIMARY KEY (' + pk + '))'
            cur.execute(q)
            cols[t['tablename']] = collist[2:]
            pkcols[t['tablename']] = pk
            npkcols[t['tablename']] = npklist
            if cockroach and 'uniformPartitions' in t:
                up = t['uniformPartitions']
                spli = ""
                if t['tablename']=='stock':
                    for i in range(up):
                        p = stock_pg[len(stock_pg)//up*i][0]
                        spli = spli + ", ('" + p + "')"
                elif t['tablename'] in ('orders','orderLines'):
                    for i in range(up):
                        p = 2**64//up*i-2**63
                        spli = spli + ", (" + str(p) + ")"
                if spli > "":
                    q = 'ALTER TABLE ' + t['tablename'] + ' SPLIT AT VALUES ' + spli[1:]
                    # print(q)
                    cur.execute(q)
            if 'indexes' in t.keys():
                for i in t['indexes']:
                    cl = ''
                    for c in i['columns']:
                        cl = cl + c + ', '
                    q = 'CREATE INDEX IF NOT EXISTS ' + i['name'] + ' on ' + t['tablename'] + ' ( ' + cl[:-2] + ' ) '
                    cur.execute(q)
        elif 'sequencename' in t.keys():
            q = 'CREATE SEQUENCE IF NOT EXISTS ' + t['sequencename'] + ' CACHE 1000'
            cur.execute(q)
        else:
            raise ValueError( 'Unknown schema object' )
    conn.commit()
    # Fill tables with initial data

    ds = datetime.datetime.now()
    rowcount = 0
    for tablename in list(filldata_pg.keys()):
        rows = filldata_pg[tablename]
        q = 'INSERT INTO ' + tablename + '( ' + cols[tablename] + ' ) VALUES %s ON CONFLICT( ' + pkcols[tablename] +' ) DO UPDATE SET '
        for x in npkcols[tablename]:
            q = q + x + ' = EXCLUDED.' + x + ', '
        q = q[:-2]
        psycopg2.extras.execute_values( cur, q, rows )
        rowcount = rowcount + len(rows)
    conn.commit()
    df = datetime.datetime.now()
    print('Fill data,', rowcount, 'record(s):', df - ds )

def run_ydb( tableclient, pool, database, pcount, quantity = 5000 ):
    stock = [ { "product": "p" + ("000000" + str(n))[-6:], "quantity": quantity } for n in range(pcount)]
    # Generate tables if not exist
    colbuc = {}
    with pool.checkout() as session:
        for t in schema:
            if 'tablename' in t.keys():
                td = ydb.TableDescription()
                buc = ydb.BulkUpsertColumns() # why it's a different object to TableDescription?!
                for c in t['columns']:
                    colname = list(c.keys())[0]
                    td = td.with_column(ydb.Column(colname, ydb.OptionalType(c[colname])))
                    buc = buc.add_column(colname, c[colname])
                if 'partitionByLoad' in t or 'uniformPartitions' in t:
                    part_settings = ydb.PartitioningSettings()
                    # policy = ydb.PartitioningPolicy().with_auto_partitioning(ydb.AutoPartitioningPolicy.AUTO_SPLIT)
                    if 'partitionByLoad' in t and t['partitionByLoad']:
                        part_settings = part_settings.with_partitioning_by_load(ydb.FeatureFlag.ENABLED)
                    if 'uniformPartitions' in t:
                        up = t['uniformPartitions']
                        if t['tablename']=='stock':
                            pdef = []
                            pdefs = []
                            for i in range(up):
                                p = stock[len(stock)//up*i]['product']
                                pdef.append( ydb.KeyBound((p, )))
                                pdefs.append(p)
                            print(pdefs)
                            pdeft = tuple(pdef)
                            # policy = policy.with_explicit_partitions( ydb.ExplicitPartitions( pdeft ))
                            td = td.with_partition_at_keys( ydb.ExplicitPartitions( pdeft ))
                        else:
                            # policy = policy.with_uniform_partitions( up )
                            td = td.with_uniform_partitions( up )
                        part_settings = part_settings.with_min_partitions_count( up * 3 )
                    # profile = ydb.TableProfile().with_partitioning_policy(policy)
                    td = td.with_partitioning_settings( part_settings )
                    # td = td.with_profile( profile )
                if 'indexes' in t:
                    for i in t['indexes']:
                        td = td.with_indexes( ydb.TableIndex(i['name']).with_index_columns(*i['columns']) )
                if 'read_replicas_per_az' in t:
                    td = td.with_read_replicas_settings(ydb.ReadReplicasSettings().with_per_az_read_replicas_count(t['read_replicas_per_az']))
                    
                session.create_table(
                        database + '/' + t['tablename'],
                        td.with_primary_keys(*t['pk'])
                    )
                colbuc[t['tablename']] = buc

        # Fill tables with initial data
        filldata = {'stock':stock }
        ds = datetime.datetime.now()
        rowcount = 0
        #pq_addord = session.prepare("""
        #        DECLARE $stock AS List<Struct<product:Utf8,quantity:UInt64>>;
        #        UPSERT INTO stock (product, quantity) SELECT product, quantity FROM AS_TABLE( $stock );
        #    """)
        t = session.transaction(ydb.SerializableReadWrite())
        for tablename in list(filldata.keys()):
            rows = filldata[tablename]
            # t.execute( pq_addord, {"$stock":rows}, commit_tx = True )
            tableclient.bulk_upsert( database + '/' + tablename, rows, colbuc[tablename]) # two times faster than exec(upsert from as_table()), but 4MB grpc limit is still in place!
            rowcount = rowcount + len(rows)
        df = datetime.datetime.now()
        print('Fill data,', rowcount, 'record(s):', df - ds )

def run_mongo( client, db, pcount, quantity = 5000 ):
    exist = db.list_collection_names()
    must = [schema[x]['tablename'] for x in range(len(schema))]
    for t in set(must) - set(exist):
        db.create_collection( t )

    stock = [ { "_id": "p" + ("000000" + str(n))[-6:], "quantity": quantity+n } for n in range(pcount)]
    ds = datetime.datetime.now()
    for x in stock:
        db.stock.update( 
            {'_id':x['_id']},
            x,
            upsert=True)
    df = datetime.datetime.now()
    print('Fill data,',  len(stock), 'record(s):', df - ds )