from posixpath import expanduser
import ydb
import ydb.iam
import yaml
import os

def read_bytes(f):
    with open(f, 'rb') as fr:
        return fr.read()

def read_string(f):
    with open(f, 'r') as fr: 
        return fr.readline().rstrip()

def getDriverPars( profileName, d ):
    res = {}
    
    if not profileName is None:
        p = d['profiles'][profileName]
        if 'endpoint' in p:
            res['endpoint'] = p['endpoint']
        if 'database' in p:
            res['database'] = p['database']
        iam_endpoint = p.get('iam-endpoint')
        if 'authentication' in p:
            auth = p['authentication']
            if not auth is None:
                method = auth['method']
                data = auth.get('data')
                if method in ('iam-token', 'ydb-token'):
                    res['credentials'] = ydb.AccessTokenCredentials( data )
                elif method == 'token-file':
                    res['credentials'] = ydb.AccessTokenCredentials( read_string( data ) )
                elif method == 'sa-key-file':
                    res['credentials'] = ydb.iam.ServiceAccountCredentials.from_file( data, iam_endpoint, )
                elif method == 'use-metadata-credentials':
                    res['credentials'] = ydb.iam.MetadataUrlCredentials()
                else:
                    res['credentials'] = ydb.AnonymousCredentials()
        # res['root_certificates']=read_bytes() -- not implemented in profiles yet
    return res

def getDriverParsFromProfile( profileName = None, profileFile = "~/ydb/config/config.yaml" ):
    res = {}
    if not profileName is None:
        with open( os.path.expanduser(profileFile) ) as f:
            d = yaml.load( f, Loader=yaml.SafeLoader )
        return getDriverPars( profileName, d )
    return res


