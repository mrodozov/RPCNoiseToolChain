__author__ = 'rodozov, Algirdas Beinaravicius'

"""
runDataToDB
Author: Algirdas Beinaravicius, modifications by Mircho Rodozov mrodozov@cern.ch
"""

is_cx_Oracle = True
is_sqlalchemy = True

from sqlalchemy.engine import ResultProxy

try:
    import cx_Oracle
except Exception, e:
    is_cx_Oracle = False

try:
    import sqlalchemy
except Exception, e:
    is_sqlalchemy = False

class DBService(object):
    def __init__(self, dbType='sqlite:///', host=None, port=None, user='', password='', schema='', dbName='runData.db'):
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
                self.__alchemyDBString = dbType + user + ':' + password + '@' + dbName
        self.__engine = sqlalchemy.create_engine(self.__alchemyDBString)


    def insertToDB(self, data, tableName, orderedColumnNames, argsList):
        retval = False
        metadata = sqlalchemy.MetaData()
        table = sqlalchemy.Table(tableName, metadata, schema=self.__schema, autoload=True, autoload_with=self.__engine)
        connection = self.__engine.connect()
        insertionList = []
        for line in data:
            insertion = {}
            argnum = 0
            for columnName in orderedColumnNames:
                if argnum in argsList:
                    insertion[columnName] = line[argnum]
                argnum += 1
            insertionList.append(insertion)
            #print insertion
        #print  insertionList
        action = table.insert().values(insertionList)
        #queryResult = ResultProxy
        queryResult = connection.execute(action) # TODO - handle this execution more transparent
        connection.close()
        retval = queryResult
        return retval

    def deleteFromDB(self, runNumber=None, tableName=None):
        metadata = sqlalchemy.MetaData()
        table = sqlalchemy.Table(tableName, metadata, schema=self.__schema, autoload=True, autoload_with=self.__engine)
        # delete all rows with given runnumber from the data base
        connection = self.__engine.connect()
        delete = table.delete().where(table.c.runnumber==runNumber)
        connection.execute(delete)
        connection.close()


