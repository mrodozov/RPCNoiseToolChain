"""
runDataToDB
Author: Algirdas Beinaravicius
"""
import os
import optparse
import sys
import datetime
import re
is_cx_Oracle = True
is_sqlalchemy = True
try:
  import cx_Oracle
except Exception, e:
  is_cx_Oracle = False

try:
  import sqlalchemy
except Exception, e:
  is_sqlalchemy = False

class DBServiceStrips(object):
  def __init__(self, dbType='sqlite:///', host=None, port=None, user='', password= '', schema='', dbName='runData.db'):
    self.__dbType = dbType
    self.__schema = schema
    self.__host = host
    self.__user = user
    self.__password = password
    self.__dbName = dbName
    self.__supportedDBs = {'sqlite':['sqlite://', 'sqlite:///'], 'oracle':['oracle://']}
    if (dbType in self.__supportedDBs['sqlite']):
      self.__alchemyDBString = dbType + dbName 
    else:
      
      #create oracle connection string
      if host and port:
        self.__alchemyDBString = dbType + user + ':' + password + '@' + host + ':' + port + '/' + dbName
      else:
        self.__alchemyDBString = dbType + user + ':' + password + '@' + dbName
    self.__engine = sqlalchemy.create_engine(self.__alchemyDBString)
  
  def createDB(self):
    metadata = sqlalchemy.MetaData()
    runs = sqlalchemy.Table('RPC_NOISE_STRIPS', metadata,
    sqlalchemy.Column('run_number', sqlalchemy.Integer),
#    sqlalchemy.Column('exec_date', sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.Column('raw_id', sqlalchemy.Integer),
#    sqlalchemy.Column('roll', sqlalchemy.String(200)),
    sqlalchemy.Column('channel_number', sqlalchemy.Integer),
    sqlalchemy.Column('strip_number', sqlalchemy.Integer),
    sqlalchemy.Column('is_dead', sqlalchemy.Integer),
    sqlalchemy.Column('is_masked', sqlalchemy.Integer),
#    sqlalchemy.Column('rate_hz', sqlalchemy.Float(precision=10)),
    sqlalchemy.Column('rate_hz_cm2', sqlalchemy.Numeric(precision=13,scale=7),
    schema=self.__schema)
    )
    metadata.create_all(self.__engine)
  
  def deleteFromDB(self, runNumber=None):
    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table('RPC_NOISE_STRIPS', metadata, schema=self.__schema,
    autoload=True, autoload_with=self.__engine)
    
    #delete all rows with given runnumber from the data base
    connection = self.__engine.connect()
    delete = table.delete().where(table.c.runnumber==runNumber)
    connection.execute(delete)
    connection.close()
            
  def insertToDB(self, dataList):
    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table('RPC_NOISE_STRIPS', metadata, schema=self.__schema,
    autoload=True, autoload_with=self.__engine)
    connection = self.__engine.connect()
    for data in dataList:
      ins = table.insert().values(
          run_number=data['run'],
#          exec_date=data['date'],
          raw_id=data['rawId'],
#          roll=data['rollName'],
          channel_number=data['channelNumber'],
          strip_number=data['stripNumber'],
          is_dead=data['isDead'],
          is_masked=data['isMasked'],
#          rate_hz=data['noiseRate'],
          rate_hz_cm2=data['noiseRateCm2']
          )
      connection.execute(ins)   
    connection.close()

class FileListRetriever(object):
  def __init__(self):
    pass
  
  #retrieves a list of files from run****, which is placed in
  def retrieveList(self, runDir):
    fileList = [os.path.join(runDir, f) for f in os.listdir(runDir) if os.path.isfile(os.path.join(runDir, f))]
    return fileList

class FileReader(object):
  def __init__(self):
      #header of the file should contain only to numbers, separated with spaces or tabs
    self.__headerPattern = '^[0-9]+[ \t]+[0-9]+$'
    
    #-1 : these strips are not connected
    self.__runDataPatternOff = """
      ^
      ([0-9]+[ \t]+){2}
      ([-]?\d+[ \t]+)+
      (0|-99)[ \t]*
      $
      """
    #run data should contain run name, followed by 3 integers, separated by white spaces
    #followed by 2 floats, separated by white spaces
    self.__runDataPattern = """
      ^
      ([0-9]+[ \t]+){5}
      [0-9]+(\.*[0-9]+){0,1}([eE][+-]?\d+)?[ \t]*
      $
      """
  
  #checks if the file structure and its contents are correct
  def __contentCheck(self, contents):
    contentMeta = {'correct' : True, 'errors' : []}
    
    #check header length
    if not re.match(self.__headerPattern, contents[0]):
      contentMeta['correct'] = False
      contentMeta['errors'].append('File header (first line of the file) is incorrect')
    
    for run in contents[1:]:
      if not re.match(self.__runDataPattern, run, re.VERBOSE):
        if not re.match(self.__runDataPatternOff, run, re.VERBOSE):
          contentMeta['correct'] = False
          contentMeta['errors'].append('run data ' + run + ' is incorrect')
    return contentMeta
    
  def read(self, fileName):
    f = open(fileName, 'r')
    contents = f.read().strip().split('\n')
    
    #check if file content is correct
    contentMeta = self.__contentCheck(contents)
    if not contentMeta['correct']:
      print 'ERROR:\t' + fileName + ' content is incorrect'
      for error in contentMeta['errors']:
          print '\t' + error
      sys.exit(-1)
      
    #first line of a run file is a header
    header = contents[0].split()
    runID = header[0]
#    timeStamp = header[1]
#    timeStamp = datetime.datetime.fromtimestamp(float(header[1]))
    #following lines of a file are data
    runList = []
    for run in contents[1:]:
      data = run.split()
      runData = {}
      runData['run'] = runID
#      runData['date'] = timeStamp
      runData['rawId'] = data[0]
      runData['channelNumber'] = data[1]
      runData['stripNumber'] = data[2]
      runData['isDead'] = data[3]
      runData['isMasked'] = data[4]
      runData['noiseRateCm2'] = data[5]
      '''
      runData['rawId'] = data[0]
#      runData['rollName'] = data[1]
      runData['channelNumber'] = data[2]
      runData['stripNumber'] = data[3]
      runData['isDead'] = data[4]
      runData['isMasked'] = data[5]
#      runData['noiseRate'] = data[6]
      runData['noiseRateCm2'] = data[7]
      '''
      runList.append(runData)
    return runList
    
class ArgumentParser(object):
  def __init__(self):
    self.__parser = optparse.OptionParser()
    usageStr = 'Usage: ' + sys.argv[0] + ' [options] fileName.txt\n'
    usageStr += 'Example: \n'
    usageStr += sys.argv[0] + ' -s sqliteDB.db fileName.txt\n'
    usageStr += sys.argv[0] + ' -o oracleSID -u userName -p password --host=hostname --port=port fileName.txt\n'
    usageStr += sys.argv[0] + ' -o oracleSID -u userName -p password --host=hostname --port=port runDir\n'
    usageStr += sys.argv[0] + ' -o oracleTNS -u userName -p password runDir\n'
    usageStr += sys.argv[0] + ' -o oracleTNS --updaterun runDir\n'
    
    
    self.__parser.set_usage(usageStr)
    self.__parser.add_option('-s', '--sqlite',
              dest='sqliteDB',
              type='string',
              action='store',
              help='sqlite database name')
    self.__parser.add_option('-o', '--oracle',
              dest='oracleDB',
              type='string',
              action='store',
              help='oracle sid or oracle tns_name')
    self.__parser.add_option('-u', '--user',
              dest='username',
              action='store',
              type='string',
              help='username to connect to the database')
    self.__parser.add_option('-p', '--password',
              dest='password',
              action='store',
              type='string',
              help='password to connect to the database')
    self.__parser.add_option('-S', '--schema',
              dest='schema',
              action='store',
              type='string',
              help='schema of the database')
    self.__parser.add_option('--host',
              dest='hostname',
              action='store',
              type='string',
              help='hostname of the database')
    self.__parser.add_option('--port',
              dest='port',
              action='store',
              type='string',
              help='connection port')
    self.__parser.add_option('--updaterun',
              dest='updaterun',
              default=False,
              action='store_true',
              help='specify this flag, if update on run is perfomed')
  def __checkArguments(self, options, remainder):
    if len(remainder) == 0:
      print 'Specify file or directory name. Use -h to get more help'
      sys.exit(-1)
    if options.sqliteDB:
      if options.oracleDB:
        print 'Only one database has to be specified. Use -h to get more help'
        sys.exit(-1)
    else:
      if (options.oracleDB is None):
        print 'Database not specified. Use -h to get more help'
        sys.exit(-1)
      else:
        if (not is_cx_Oracle):
          print 'ERROR!!!: cx_Oracle module not installed'
          sys.exit(-1)
        if not options.username:
          print 'Username not specified. Use -h to get more help'
          sys.exit(-1)
        if not options.password:
          print 'Password not specified. Use -h to get more help'
          sys.exit(-1)
      
  def parse(self, cmd_args):
    settings = {}
    options, remainder = self.__parser.parse_args(cmd_args)
    self.__checkArguments(options, remainder)
    settings['remainder'] = remainder[0]
    settings['username'] = None
    settings['password'] = None
    settings['hostname'] = None
    settings['schema'] = None
    settings['port'] = None
    settings['updaterun'] = options.updaterun
    if options.sqliteDB:
      settings['dbType'] = 'sqlite:///'
      settings['dbName'] = options.sqliteDB
    elif options.oracleDB:
      settings['dbType'] = 'oracle://'
      settings['dbName'] = options.oracleDB
      settings['username'] = options.username
      settings['password'] = options.password
      settings['schema'] = options.schema
      settings['hostname'] = options.hostname
      settings['port'] = options.port
    return settings
    
if __name__ == '__main__':
  if (not is_sqlalchemy):
    print 'ERROR: not able to import sqlalchemy module'
    sys.exit(-1)
    
  argParser = ArgumentParser()
  settings = argParser.parse(sys.argv[1:])
  
  dbService = DBServiceStrips(dbType=settings['dbType'], host=settings['hostname'], port=settings['port'], user=settings['username'], password=settings['password'], schema=settings['schema'], dbName=settings['dbName'])
#  dbService.createDB()
  fileReader = FileReader()
  
  if os.path.isdir(settings['remainder']): #user wants to insert multiple file data into database
    fileListRetriever = FileListRetriever()
    fileList = fileListRetriever.retrieveList(settings['remainder'])
    deletePerformed = False
    for f in fileList:
      runList = fileReader.read(f)
      #in case of update, deletion of rows from database has to be performed only once
      if (settings['updaterun']) and (not deletePerformed):
        dbService.deleteFromDB(runNumber=runList[0]['run'])
        deletePerformed = True
      dbService.insertToDB(runList)
      print 'Inserted ' + f
  else:
    runList = fileReader.read(settings['remainder'])
    dbService.insertToDB(runList)
    print "run%s" %runList[0]['run']
  print 'runDataToDB_strip.py: DONE Successfully'
  sys.exit(0) 
