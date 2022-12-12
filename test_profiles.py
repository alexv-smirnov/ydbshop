import ydb_profile

def test_1():
    assert ydb_profile.getDriverPars( 'my1', {'profiles':{'my1':{'endpoint':'eee'}}}) == {'endpoint':'eee'}