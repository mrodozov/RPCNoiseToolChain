__author__ = 'rodozov, Algirdas Beinaravicius'

"""
runDataToDB
Author: Algirdas Beinaravicius, modifications by Mircho Rodozov mrodozov@cern.ch
"""

from Singleton import Singleton
import json
import sqlalchemy
#from CommandClasses import *
from sqlalchemy.engine import reflection
import datetime

class DBService(object):

    __metaclass__ = Singleton

    def __init__(self, dbType='sqlite:///', host=None, port=None, user='', password='', schema='', dbName='dbfiles/runData.db'):
        self.__dbType = dbType
        self.__schema = schema
        self.__host = host
        self.__user = user
        self.__password = password
        self.__dbName = dbName
        self.__supportedDBs = {'sqlite': ['sqlite://', 'sqlite:///'], 'oracle': ['oracle://']}

        if (dbType in self.__supportedDBs['sqlite']):
            self.__alchemyDBString = dbType + dbName
        else:

            # create oracle connection string
            if host and port:
                self.__alchemyDBString = dbType + user + ':' + password + '@' + host + ':' + port + '/' + dbName
            else:
                self.__alchemyDBString = dbType + dbName
        self.__alchemyDBString = 'mysql+mysqldb://rodozov:BAKsh0321__@localhost/RPC?charset=utf8'
        #rodozov/tralala@localhost.localdomain:1521/rpc.localdomain
        self.__engine = sqlalchemy.create_engine(self.__alchemyDBString)

        print self.__alchemyDBString

    def createDBStrips(self):
        metadata = sqlalchemy.MetaData()
        runs = sqlalchemy.Table('RPC_NOISE_STRIPS', metadata,
        sqlalchemy.Column('run_number', sqlalchemy.Integer),
        sqlalchemy.Column('raw_id', sqlalchemy.Integer),
        sqlalchemy.Column('channel_number', sqlalchemy.Integer),
        sqlalchemy.Column('strip_number', sqlalchemy.Integer),
        sqlalchemy.Column('is_dead', sqlalchemy.Integer),
        sqlalchemy.Column('is_masked', sqlalchemy.Integer),
        sqlalchemy.Column('rate_hz_cm2', sqlalchemy.Numeric(13,7)),
                                schema=self.__schema)
        #sqlalchemy.Column('rate_hz_cm2', sqlalchemy.Numeric(13,7)))
        metadata.create_all(self.__engine)

    def createDBRolls(self):
        metadata = sqlalchemy.MetaData()
        runs = sqlalchemy.Table('RPC_NOISE_ROLLS', metadata,
        sqlalchemy.Column('run_number', sqlalchemy.Integer),
        sqlalchemy.Column('raw_id', sqlalchemy.Integer),
        sqlalchemy.Column('dead_strips', sqlalchemy.Integer),
        sqlalchemy.Column('masked_strips', sqlalchemy.Integer),
        sqlalchemy.Column('strips_to_unmask', sqlalchemy.Integer),
        sqlalchemy.Column('strips_to_mask', sqlalchemy.Integer),
        sqlalchemy.Column('rate_hz_cm2', sqlalchemy.Numeric(13,7)),
                                schema=self.__schema)
        #sqlalchemy.Column('rate_hz_cm2', sqlalchemy.Numeric(13,7)))
        metadata.create_all(self.__engine)

    def insertToDB(self, data, tableName, orderedColumnNames, argsList):
        retval = False
        #print self.__alchemyDBString
        metadata = sqlalchemy.MetaData()
        table = sqlalchemy.Table(tableName, metadata, schema='RPC', autoload=True, autoload_with=self.__engine)
        #table = sqlalchemy.Table(tableName, metadata, schema=self.__schema, autoload=True, autoload_with=self.__engine)
        connection = self.__engine.connect()
        #insp = reflection.Inspector.from_engine(self.__engine)
        #print insp.get_schema_names()
        #print insp.get_table_names()

        insertionList = []
        #print tableName
        print connection
        start_time = datetime.datetime.now().replace(microsecond=0)

        for line in data:
            insertion = {}
            argnum = 0
            for columnName in orderedColumnNames:
                if argnum in argsList:
                    value = None
                    if columnName == 'rate_hz_cm2': value = float(line[argnum]) # oracle complain about the non numberic type
                    else: value = int(line[argnum])
                    insertion[columnName] = value
                argnum += 1
            insertionList.append(insertion)

        transaction = connection.begin()
        try:
            queryResult = connection.execute(table.insert(), insertionList)
            transaction.commit()
        except:
            transaction.rollback()
            raise

        connection.close()
        endtime = datetime.datetime.now().replace(microsecond=0)
        print 'time it took: ', endtime-start_time
        #retval = queryResult
        return retval

    def deleteFromDB(self, runNumber=None, tableName=None):
        metadata = sqlalchemy.MetaData()
        table = sqlalchemy.Table(tableName, metadata, schema=self.__schema, autoload=True, autoload_with=self.__engine)
        # delete all rows with given runnumber from the data base
        connection = self.__engine.connect()
        delete = table.delete().where(table.c.runnumber==runNumber)
        connection.execute(delete)
        connection.close()

    def selectFromDB(self, runNumber=None, tableName=None):
        metadata = sqlalchemy.MetaData()
        table = sqlalchemy.Table(tableName, metadata, schema=self.__schema, autoload=True, autoload_with=self.__engine)
        #table = sqlalchemy.Table(tableName, metadata, schema=self.__schema, autoload=True, autoload_with=self.__engine)
        connection = self.__engine.connect()

        select = table.select()
        result = connection.execute(select)
        return result

    def getConnection(self):
        return self.__engine.connect()

if __name__ == "__main__":

    optionsObject = None
    with open('resources/options_object.txt', 'r') as optobj:
        optionsObject = json.loads(optobj.read())

    DBService('oracle://','localhost','1521','rodozov','tralala','','RPC')

    '''
    db_obj = DBService()

    print db_obj
    db_obj2 = DBService()
    print db_obj2

    #db_obj.createDBRolls()
    #db_obj.createDBStrips()

    dbup = DBDataUpload(args=optionsObject['dbdataupload'])
    dbup.options['filescheck'] = ['results/run220796/database_new.txt', 'results/run220796/database_full.txt']
    dbuptwo = DBDataUpload(args=optionsObject['dbdataupload'])
    dbuptwo.options['filescheck'] = ['results/run220796/database_new.txt', 'results/run220796/database_full.txt']
    dbuptwo.options['run'] = '220796'
    dbup.options['run'] = '220796'
    print dbup.args
    print dbup.options
    dbup.processTask()
    dbuptwo.processTask()
    '''
