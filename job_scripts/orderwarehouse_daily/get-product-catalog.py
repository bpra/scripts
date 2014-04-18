#!/usr/bin/python27
"""Pull product data from MongoDB and product catalog service and store in flattened mysql tables.

First, a list of all styleNames is compiled from three mongodb collections:
Accessory
Dress
SaleableProduct

For each styleName, fetch product prodCatData from product service call.

Flatten JSON response and store in MySQL.
"""

import MySQLdb
from pprint import pprint as pp

def main():
    """main entry point"""
    global config,logger
    
    bootstrap()

    try:
        logger.info('STARTING')

        # connect to mysql
        global mysql
        mysql = connect_db()

        # get all style names
        getStyleNames()

    except:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        logger.error(traceback.format_exception(exc_type, exc_value,exc_traceback))
        raise
    finally:
        logger.info('FINISHED')

def connect_db():
    """Connect to mysql"""
    global mysql,dbCursor

    dbKey = config['default']['db']
    mysql = MySQLdb.connect(
        host=config[dbKey]['host'],
        user=config[dbKey]['user'],
        db=config[dbKey]['database'],
        passwd=config[dbKey]['password'],
        use_unicode=True,
        charset='utf8')
    return mysql

def run_query(*args):
    """Execute a sql statement"""
    global mysql

    try:
        cursor = mysql.cursor()
        cursor.execute(*args)
    except (AttributeError, MySQLdb.OperationalError):
        mysql = connect_db()
        cursor = mysql.cursor()
        cursor.execute(*args)
    return cursor

def getStyleNames():
    """Get the full list of style names from mongodb"""
    global config,logger

    # connect to mysql
    # initialize MySQL tables
    sql = "truncate table rtrbi.mongo_product_catalog"
    dbCursor = run_query(sql)
    dbCursor = run_query('commit')

    sql = "truncate table rtrbi.mongo_product_catalog_types"
    dbCursor = run_query(sql)
    dbCursor = run_query('commit')

    # connect to mongo
    from pymongo import MongoClient
    (host,port) = config['mongo-prod']['host'].rsplit(':',2)
    mongoClient = MongoClient(host,int(port))
    mongoDatabase = mongoClient['product_catalog']

    # DEPRECATED - only pull collections with styleName
    # for collectionName in mongoDatabase.collection_names():

    for collectionName in ['Accessory','Dress','SaleableProduct']:
        # status updates
        statusCounter = 0
        logger.info("Getting {} data...".format(collectionName))

        # collect information about types
        mongoTypes = {}
        prodcatTypes = {}

        mongoCollection = mongoDatabase[collectionName]
        for mongoData in mongoCollection.find(timeout=False):
            # convert bson to json
            mongoData = cleanupBSON(mongoData,mongoTypes)

            # DEPRECATED - store mongo data
            # flatMongoData = flattenData(mongoData,'mongo',mongoTypes)

            # idKey = mongoData.get('_id')
            # for item in flatMongoData:
            #   item['idKey'] = idKey
            #   item['collectionKey'] = collectionName
            #   dbCursor = run_query("""
            #       insert into rtrbi.mongo_product_catalog
            #       (_id,_collection,attribute_name,attribute_index,attribute_value,collection)
            #       values
            #       (%(idKey)s,%(collectionKey)s,%(attribute_name)s,%(attribute_index)s,%(attribute_value)s,%(collection)s)
            #       """,item)
            #   dbCursor = run_query('commit')

            # get prodCatData from service
            if collectionName in ['Accessory','Dress','SaleableProduct']:
                prodCatData = getProductCatalogData(mongoData)
                if any(prodCatData):
                        # merge mongo with product catalog service
                        prodCatData = mergeDict(mongoData,prodCatData)
                        flatProdDatData = flattenData(prodCatData,'prodcat',prodcatTypes)

                        styleName = prodCatData['styleName']
                        for item in flatProdDatData:
                            item['collectionKey'] = collectionName
                            item['styleName'] = styleName
                            dbCursor = run_query("""
                                insert into rtrbi.mongo_product_catalog
                                (_collection,styleName,attribute_name,attribute_index,attribute_value)
                                values
                                (%(collectionKey)s,%(styleName)s,%(attribute_name)s,%(attribute_index)s,%(attribute_value)s)
                                """,item)
                            dbCursor = run_query('commit')

            statusCounter += 1
            if (statusCounter % 100) == 0:
                logger.info("{} {} items inserted...".format(statusCounter,collectionName))

        # merge types with precedence to mongo types
        mongoTypes = mergeDict(prodcatTypes,mongoTypes)
        for key in mongoTypes:
            insertData = {
                'collectionKey':collectionName,
                'attribute_name':key,
                'data_type':mongoTypes[key]['data_type'],
                'data_class':mongoTypes[key]['data_class'],
                'data_length':mongoTypes[key]['data_length'],
                'data_count':mongoTypes[key]['data_count'],
            }

            dbCursor = run_query("""
                insert into rtrbi.mongo_product_catalog_types
                (_collection,attribute_name,data_type,data_class,data_length,data_count)
                values
                (%(collectionKey)s,%(attribute_name)s,%(data_type)s,%(data_class)s,%(data_length)s,%(data_count)s)
                """,insertData)
            dbCursor = run_query('commit')

        logger.info("{} {} items done...".format(statusCounter,collectionName))

    buildSizeMapping(dbCursor)

    # update data_ready
    sql = """replace into rtrbi.data_ready(data_set,run_date) 
    select 'rtrbi.mongo_product_catalog',curdate() 
    UNION select 'rtrbi.mongo_product_catalog_types',curdate()
    UNION select 'rtrbi.products_canonical_size_map',curdate()
    """
    dbCursor = run_query(sql)
    dbCursor = run_query('commit')

def cleanupBSON(mongoData,dataTypes):
    """Convert BSON to JSON and cleanup some special keys"""
    import bson.json_util
    import json

    mongoData = json.loads(bson.json_util.dumps(mongoData))

    for key in mongoData:
        typeDict = dataTypes.setdefault(key,{'data_type':None,'data_class':None,'data_length':0,'data_count':0})

        keyType = type(mongoData[key])
        if typeDict['data_type'] is None:
            typeDict['data_type'] = keyType

        if type(mongoData[key]) is dict:
            if '$oid' in mongoData[key]:
                # ObjectId()
                mongoData[key] = mongoData[key]['$oid']

                if typeDict['data_class'] is None:
                    typeDict['data_class'] = 'ObjectId'

            elif '$date' in mongoData[key]:
                # Date()
                mongoData[key] = mongoData[key]['$date']

                if typeDict['data_class'] is None:
                    typeDict['data_class'] = 'Date'
                    
    return mongoData

def mergeDict(dict1,dict2):
    """Merge dict1 into dict2"""
    newDict = dict(dict1.items() + dict2.items())
    return newDict

def getProductCatalogData(styleResult):
    """Process one styleName:
    1. get prodCatData from product catalog service
    2. flatten prodCatData and store in MySQL
    """
    global config,logger

    styleName = styleResult.get('styleName')
    if styleName is None:
        # missing styleName, cannot continue
        return {}

    styleDetails = {'styleName':styleName}

    # get prodCatData from product catalog
    import json
    import urllib2
    try:
        url = config['productcatalog']['prod'] + "/all/style_name/{}".format(styleName)
        response = urllib2.urlopen(url)
        content = response.read()

        # check for error message
        if content.find('DOES NOT EXIST') > 0:
            pass
        else:
            # parse json into data
            styleDetails = json.loads(content)
    except:
        # ignore errors
        pass

    return styleDetails

def flattenData(data,format,dataTypes):
    """Flatten style data dictionary

    This function assumes a limited depth. Each top-level key either points to:
    - list = convert to index,value
    - dict = convert to key,value
    - other = keep as is

    The format parameter determines output format:
    'prodcat' = (attribute_name,attribute_index,attribute_value)
    'mongo' = (attribute_name,attribute_index,attribute_value,collection)
        - collection = collection name for DbRef types
    """
    global config,logger
    import json
    import copy

    # return a list of dicts
    flatDetails = []

    for attribute_name in data:
        defaultDict = {
            'attribute_name':attribute_name,
            'attribute_index':None,
            'attribute_value':None, 
        }
        if format == 'mongo':
            defaultDict['collection'] = None

        typeDict = dataTypes.setdefault(attribute_name,{'data_type':None,'data_class':None,'data_length':0,'data_count':0})

        attribute_type = type(data[attribute_name])
        if typeDict['data_type'] is None:
            typeDict['data_type'] = attribute_type

        if attribute_type is list:
            for i in range(len(data[attribute_name])):
                newDict = copy.copy(defaultDict)

                newDict['attribute_index'] = i
                newDict['attribute_value'] = data[attribute_name][i]

                if type(newDict['attribute_value']) is list:
                    newDict['attribute_value'] = json.dumps(newDict['attribute_value'])
                elif type(newDict['attribute_value']) is dict:
                    newDict['attribute_value'] = json.dumps(newDict['attribute_value'])

                newDict['attribute_value_string'] = makeString(newDict['attribute_value'])
                if len(newDict['attribute_value_string']) > typeDict['data_length']:
                    typeDict['data_length'] = len(newDict['attribute_value_string'])
                typeDict['data_count'] += 1

                flatDetails.append(newDict)
        elif attribute_type is dict:
            newDict = copy.copy(defaultDict)

            attributeDone = False

            if format == 'mongo':
                # check for DbRef
                if ('$ref' in data[attribute_name]) and ('$id' in data[attribute_name]):
                    if '$oid' in data[attribute_name]['$id']:
                        newDict['attribute_value'] = data[attribute_name]['$id']['$oid']
                    else:
                        newDict['attribute_value'] = data[attribute_name]['$id']
                    newDict['collection'] = data[attribute_name]['$ref']
                    flatDetails.append(newDict)

                    if 'data_class' not in typeDict:
                        typeDict['data_class'] = 'DBRef'

                    newDict['attribute_value_string'] = makeString(newDict['attribute_value'])
                    if len(newDict['attribute_value_string']) > typeDict['data_length']:
                        typeDict['data_length'] = len(newDict['attribute_value_string'])
                    typeDict['data_count'] += 1

                    attributeDone = True
                # check for ObjectId
                elif ('$oid' in data[attribute_name]):
                    newDict = copy.copy(defaultDict)
                    newDict['attribute_value'] = data[attribute_name]['$oid']
                    flatDetails.append(newDict)

                    if 'data_class' not in typeDict:
                        typeDict['data_class'] = 'ObjectId'

                    newDict['attribute_value_string'] = makeString(newDict['attribute_value'])
                    if len(newDict['attribute_value_string']) > typeDict['data_length']:
                        typeDict['data_length'] = len(newDict['attribute_value_string'])
                    typeDict['data_count'] += 1


                    attributeDone = True

            if not attributeDone:
                for k in data[attribute_name]:
                    newDict = copy.copy(defaultDict)

                    newDict['attribute_index'] = k
                    newDict['attribute_value'] = data[attribute_name][k]

                    if type(newDict['attribute_value']) is list:
                        newDict['attribute_value'] = json.dumps(newDict['attribute_value'])
                    elif type(newDict['attribute_value']) is dict:
                        newDict['attribute_value'] = json.dumps(newDict['attribute_value'])

                    flatDetails.append(newDict)

                    newDict['attribute_value_string'] = makeString(newDict['attribute_value'])
                    if len(newDict['attribute_value_string']) > typeDict['data_length']:
                        typeDict['data_length'] = len(newDict['attribute_value_string'])
                    typeDict['data_count'] += 1


        else:
            newDict = copy.copy(defaultDict)

            newDict['attribute_value'] = data[attribute_name]
            flatDetails.append(newDict)

            newDict['attribute_value_string'] = makeString(newDict['attribute_value'])
            if len(newDict['attribute_value_string']) > typeDict['data_length']:
                typeDict['data_length'] = len(newDict['attribute_value_string'])
            typeDict['data_count'] += 1


    return flatDetails

def makeString(something):
    """Convert something into a string representation"""

    # try unicode
    try:
        return unicode(something)
    except:
        pass

    # try json
    try:
        import json
        return json.dumps(something)
    except:
        pass

    # use python repr()
    return repr(something)

def buildSizeMapping(dbCursor):
    """Build mapping from canonical sizes to sizes"""
    global config,logger
    import json


    # initialize second connection for updates
    global mysql
    updateCursor = mysql.cursor()

    sql = "truncate table rtrbi.products_canonical_size_map"
    dbCursor = run_query(sql)
    dbCursor = run_query('commit')

    # look for explicit size mappings
    logger.info("Processing canonicalSizesFromSizeScale...")
    statusCount = 0

    sql = """
    select 
     styleName
    ,attribute_index
    ,attribute_value
    from rtrbi.mongo_product_catalog
    where attribute_value is not null
    and attribute_name = 'canonicalSizesFromSizeScale'
    order by styleName,attribute_index
    """
    dbCursor = run_query(sql)

    while True:
        try:
            result = dbCursor.fetchone()

            # this should be a tuple
            styleName = result[0]
            canonical_size = result[1]
            sizes = json.loads(result[2])
            for size in sizes:
                updateCursor = run_query("""
                    insert into rtrbi.products_canonical_size_map
                    (styleName,canonical_size,size,real_mapping)
                    values
                    (%(styleName)s,%(canonical_size)s,%(size)s,1)
                    """,
                    {
                    'styleName':styleName,
                    'canonical_size':canonical_size,
                    'size':size,
                })
                updateCursor = run_query('commit')

                statusCount += 1
                if (statusCount % 1000) == 0:
                    logger.info("{} sizes updated".format(statusCount))
        except:
            # ignore error and exit loop
            break

    # look for implied size mappings
    logger.info("Processing implied size mappings...")

    sql = """
    select *
    from (
    select 
     c.styleName
    ,c.attribute_index

    ,max(case when c.attribute_name = 'canonicalSizes' then c.attribute_value else null end) as canonical_size
    ,max(case when c.attribute_name = 'sizes' then c.attribute_value else null end) as size

    from rtrbi.mongo_product_catalog c
    where c.attribute_value is not null
    and c.attribute_name in 
    (
     'canonicalSizes'
    ,'sizes'
    )
    and not exists
    (select 1
    from rtrbi.products_canonical_size_map x
    where c.styleName = x.styleName)
    group by 1,2
    ) x
    where size is not null
      and canonical_size is not null    
    """
    dbCursor = run_query(sql)

    while True:
        try:
            result = dbCursor.fetchone()

            # this should be a tuple
            styleName = result[0]
            canonical_size = result[2]
            size = result[3]
            updateCursor = run_query("""
                insert into rtrbi.products_canonical_size_map
                (styleName,canonical_size,size,real_mapping)
                values
                (%(styleName)s,%(canonical_size)s,%(size)s,0)
                """,
                {
                'styleName':styleName,
                'canonical_size':canonical_size,
                'size':size,
            })
            updateCursor = run_query('commit')

            statusCount += 1
            if (statusCount % 1000) == 0:
                logger.info("{} sizes updated".format(statusCount))
        except:
            # ignore error and exit loop
            break

"""bootstrap code

Perform bootstrapping steps

"""
import os
import sys
import inspect
import traceback

def bootstrap():
    global config,logger

    # get path to script file
    scriptPath = inspect.getfile(inspect.currentframe()) # script filename (usually with path)
    scriptDir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))) # script directory

    # read path to codebase
    baseFile = os.path.join(scriptDir,'.analytics_scripts_base')
    with open(baseFile) as fh:
        basePath = fh.readline()
    basePath = basePath.strip()

    # construct a path to load bootstrap code

    # get the current directory
    # realpath() with make your script run, even if you symlink it :)
    bootPath = os.path.abspath(os.path.join(scriptDir,basePath,'bootstrap'))
    if bootPath not in sys.path:
        sys.path.insert(0,bootPath)

    # run bootstrap
    import rtrbi_bootstrap
    config,logger = rtrbi_bootstrap.run(bootPath)

### bootstrap end ###

if __name__ == "__main__":
    main()
