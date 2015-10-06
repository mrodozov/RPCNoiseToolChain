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
  print "no sqalchemy!!!"

class DBService(object):
  def __init__(self, dbType='sqlite:///', host=None, port=None, user='', password= '', dbName='runData.db'):
    self.__dbType = dbType
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
    runs = sqlalchemy.Table('RPCSUMMARY', metadata,
    sqlalchemy.Column('run_number', sqlalchemy.Integer,primary_key = True),
#    sqlalchemy.Column('exec_date', sqlalchemy.DateTime(timezone=True)),
    sqlalchemy.Column('region',sqlalchemy.Integer,primary_key = True),
    sqlalchemy.Column('occupancy',sqlalchemy.Integer),
    sqlalchemy.Column('n_digis',sqlalchemy.Float(precision=10)),
    sqlalchemy.Column('n_clusters',sqlalchemy.Float(precision=10)),
    sqlalchemy.Column('cluster_size', sqlalchemy.Float(precision=10)),
    sqlalchemy.Column('bx', sqlalchemy.Float(precision=10)),
    sqlalchemy.Column('bx_rms', sqlalchemy.Float(precision=10)),
    sqlalchemy.Column('eff', sqlalchemy.Float(precision=10)),
    sqlalchemy.Column('n_good',sqlalchemy.Integer),
    sqlalchemy.Column('n_off',sqlalchemy.Integer),
    sqlalchemy.Column('n_noisy_strip',sqlalchemy.Integer),
    sqlalchemy.Column('n_noisy_chamber',sqlalchemy.Integer),
    sqlalchemy.Column('n_part_dead',sqlalchemy.Integer),
    sqlalchemy.Column('n_dead',sqlalchemy.Integer),
    sqlalchemy.Column('n_bad_shape',sqlalchemy.Integer),
    )
    metadata.create_all(self.__engine)
  
  def deleteFromDB(self, runNumber=None):
    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table('RPCSUMMARY', metadata, 
    autoload=True, autoload_with=self.__engine)
    
    #delete all rows with given runnumber from the data base
    connection = self.__engine.connect()
    delete = table.delete().where(table.c.runnumber==runNumber)
    connection.execute(delete)
    connection.close()
            
  def insertToDB(self, dataList):
    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table('RPCSUMMARY', metadata, 
    autoload=True, autoload_with=self.__engine)
    connection = self.__engine.connect()
    for data in dataList:
      ins = table.insert().values(
          run_number=data['run'],
#          exec_date=data['date'],
          region=data['region'],
          occupancy=data['occupancy'],
          n_digis=data['n_digis'],
          n_clusters=data['n_clusters'],
          cluster_size=data['cluster_size'],
          bx=data['bx'],
          bx_rms=data['bx_rms'],
          efficiency=data['efficiency'],
          n_good = data['n_good'],
          n_off = data['n_off'],
          n_noisy_strip = data['n_noisy_strip'],
          n_noisy_chamber = data['n_noisy_chamber'],
          n_part_dead = data['n_part_dead'],
          n_dead = data['n_dead'],
          n_bad_shape = data['n_bad_shape'],
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
      #header of the file should contain only the run number
    self.__headerPattern ='[0-9]+$' #'^[\+\-0-9]+[ \t]+[0-9]+$'
    
    #-1 : placeholder
    self.__runDataPatternOff = """
      ^
      ([0-9]+[ \t]+){2}
      $
      """
    #Contains : roll raw id (int) , number_of_digis (int), number of clusters, clustersize, bx, bx rms, efficiency (5 floats)
    #and status of the roll (int), separated by white spaces
    self.__runDataPattern = """
      ^
      ([\+\-0-9]+[ \t]+){2}
      ([\+\-0-9]+(\.*[0-9]+){0,1}([eE][+-]?\d+)?[ \t]+){6}
      ([\+\-0-9]+[ \t]+){6} 
      [\+\-0-9]+
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
      runData['region'] = data[0]
      runData['occupancy'] = data[1]
      runData['n_digis'] = data[2]
      runData['n_clusters'] = data[3]
      runData['cluster_size'] = data[4]
      runData['bx'] = data[5]
      runData['bx_rms'] = data[6]
      runData['efficiency'] = data[7]
      runData['n_good'] = data[8]
      runData['n_off'] = data[9]
      runData['n_noisy_strip'] = data[10]
      runData['n_noisy_chamber'] = data[11]
      runData['n_part_dead'] = data[12]
      runData['n_dead'] = data[13]
      runData['n_bad_shape'] = data[14]
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
      settings['hostname'] = options.hostname
      settings['port'] = options.port
    return settings

if __name__ == '__main__':
  if (not is_sqlalchemy):
    print 'ERROR: not able to import sqlalchemy module'
    sys.exit(-1)

  argParser = ArgumentParser()
  settings = argParser.parse(sys.argv[1:])


  dbService = DBService(dbType=settings['dbType'], host=settings['hostname'], port=settings['port'], user=settings['username'], password=settings['password'], dbName=settings['dbName'])
  dbService.createDB()
  fileReader = FileReader()
#  fileReader.read("190465_bis.txt")  
#  data = fileReader.read("196430.txt")  
  data = fileReader.read(settings['remainder'])
  dbService.insertToDB(data)


#if __name__ == '__main__':
#  if (not is_sqlalchemy):
#    print 'ERROR: not able to import sqlalchemy module'
#    sys.exit(-1)
#    
#  argParser = ArgumentParser()
#  settings = argParser.parse(sys.argv[1:])
#  
#  dbService = DBService(dbType=settings['dbType'], host=settings['hostname'], port=settings['port'], user=settings['username'], password=settings['password'], dbName=settings['dbName'])
#  dbService.createDB()
#  fileReader = FileReader()
#  
#  if os.path.isdir(settings['remainder']): #user wants to insert multiple file data into database
#    fileListRetriever = FileListRetriever()
#    fileList = fileListRetriever.retrieveList(settings['remainder'])
#    deletePerformed = False
#    for f in fileList:
#      runList = fileReader.read(f)
#      #in case of update, deletion of rows from database has to be performed only once
#      if (settings['updaterun']) and (not deletePerformed):
#        dbService.deleteFromDB(runNumber=runList[0]['run'])
#        deletePerformed = True
#      dbService.insertToDB(runList)
#      print 'Inserted ' + f
#  else:
#    runList = fileReader.read(settings['remainder'])
#    dbService.insertToDB(runList)
#    print "run%s" %runList[0]['run']
#  print 'runDataToDB_strip.py: DONE Successfully'
#  sys.exit(0) 
