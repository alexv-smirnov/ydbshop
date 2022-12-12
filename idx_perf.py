import ydb
import datetime

schema = [
    {   'tablename':'table1',
        'columns':[
            {'product':ydb.PrimitiveType.Utf8},
            {'quantity1':ydb.PrimitiveType.Int64},
            {'quantity2':ydb.PrimitiveType.Int64},
            {'quantity3':ydb.PrimitiveType.Int64},
            {'quantity4':ydb.PrimitiveType.Int64}
        ],
        'pk':['product']
    }
]

def run_yql( tableclient, database, pcount, quantity = 5000 ):
    # Generate tables if not exist
    session = tableclient.session().create()
    colbuc = {}
    for t in schema:
        td = ydb.TableDescription()
        buc = ydb.BulkUpsertColumns() # why it's a different object to TableDescription?!
        for c in t['columns']:
            colname = list(c.keys())[0]
            td = td.with_column(ydb.Column(colname, ydb.OptionalType(c[colname])))
            buc = buc.add_column(colname, c[colname])
        session.create_table(
                database + '/' + t['tablename'],
                td.with_primary_keys(*t['pk'])
            )
        colbuc[t['tablename']] = buc

    # Fill tables with initial data
    table1 = [ { "product": "p" + str(n), "quantity1": quantity, "quantity2": quantity+1, "quantity3": quantity+2, "quantity4": quantity+3 } for n in range(pcount)]
    filldata = {'table1':table1 }
    ds = datetime.datetime.now()
    rowcount = 0
    t = session.transaction(ydb.SerializableReadWrite())
    for tablename in list(filldata.keys()):
        rows = filldata[tablename]
        pq_addord = session.prepare("""
                DECLARE $data AS List<Struct<product:Utf8,quantity1:UInt64,quantity2:UInt64,quantity3:UInt64,quantity4:UInt64>>;
                UPSERT INTO table1 (product, quantity1, quantity2, quantity3, quantity4) SELECT product, quantity1, quantity2, quantity3, quantity4 FROM AS_TABLE( $data );
            """)
        t.execute( pq_addord, {"$data":rows}, commit_tx = True )
        # tableclient.bulk_upsert( database + '/' + tablename, rows, colbuc[tablename]) # two times faster than exec(upsert from as_table()), but 4MB grpc limit is still in place!
        rowcount = rowcount + len(rows)
    df = datetime.datetime.now()
    print('Fill data,', rowcount, 'record(s):', df - ds )

# Unique: 5, 8, 9.1, 10.4, 11.5
# Same: 5, ., ., 11.4, fail
