#!/usr/bin/python
from ConStrParser import extract
#import xmlrpclib


#added by mrodozov@cern.ch for possible use 

import re, json
from rrapi import RRApi, RRApiError

def connectRR_v3_API(runType, startRun, endRun, runDuration):

  runInfo = []
  print "Accessing run registry.......\n"
  #URL  = "http://runregistry.web.cern.ch/runregistry/"
  URL = "http://localhost:22223/runregistry/"
  api = RRApi(URL, debug = True)
  an_array = api.data('GLOBAL', 'runsummary', 'json', ['number'], {'datasetExists': '= true', 'number': '>= '+ str(startRun) +' AND <= ' + str(endRun),'duration': '> '+str(runDuration),'rpcPresent' : 'true','runClassName': runType})

  #here , the rpcPresent represents the GOOD or BAD flag as true or false. 

  for record in an_array:
    num = record['number']
    runInfo.append(num)
    
  runInfo.sort()

  #for entry in runInfo:
  #print entry # just print the numbers 

  return runInfo

#example use :
#connectRR_v3_API('Collisions12','190100','191200','1200')







def connectRR(runType, startRun, endRun, runDuration):# Connect to run registry and get run numbers with given conditions
  runInfo = []
  print "Accessing run registry.......\n"
  #### API v2
#  server = xmlrpclib.ServerProxy("http://cms-service-runregistry-api.web.cern.ch/cms-service-runregistry-api/xmlrpc");
  server = xmlrpclib.ServerProxy("http://localhost:40010/cms-service-runregistry-api/xmlrpc")
  #### API v3
#  server = xmlrpclib.ServerProxy("http://runregistry.web.cern.ch/runregistry/")
#  server = xmlrpclib.ServerProxy("http://localhost:40010/runregistry.web.cern.ch/runregistry/")

  select_good = "{groupName} = '%s' and  {runNumber} >= %s and {runNumber} <= %s and {duration} >= %s" %(runType, startRun,endRun,runDuration)
  
  runData_good = server.DataExporter.export('RUN','GLOBAL','xml_all',select_good)
#  print runData_good
  for run in runData_good.split("\n"):
    if "<NUMBER>" in run:
      runInfo.append(run[8:-9])
      print run[8:-9]
  '''    
  select_bad = "{groupName} = '%s' and  {runNumber} >= %s and {runNumber} <= %s and {duration} >= %s and {cmpRpc} = \'BAD\'" %(runType, startRun,endRun,runDuration)
  runData_bad = server.DataExporter.export('RUN','GLOBAL','xml_all',select_bad)
#  print runData_bad

  for run in runData_bad.split("\n"):
    if "<NUMBER>" in run:
      runInfo.append(run[8:-9])
      print run[8:-9]
  '''
  runInfo.sort()
  return runInfo


def addHtml(runInfo):
  exitcode = subprocess.call("grep run%s index.html" %runInfo, shell=True)
  if exitcode == 0: # Already registered in noise tool web page
    return "Registered"
  elif exitcode == 1:  #Not found
    return "New"
  else :
    return "ERROR"
  

def runNoise(runInfo): # Submit each run to noise tool
  runOK = runInfo
  exitcode = 0

  # Check this run is already analyzed or not
  try:
    dirList = os.listdir("./run%s/" %runInfo);
    if not dirList:     # Directory is empty, run noise tool with this run number.
      exitcode = subprocess.call("./submitrun.sh %s" %runInfo, shell=True)
  except OSError:       # No dirctory exists. run noise tool with this run number.
    exitcode = subprocess.call("./submitrun.sh %s" %runInfo, shell=True)

  if exitcode == -1:    # Something is wrong with noise tool submittion.
    print "ERROR!!!!!!!!!! Run %s is not going to be analyzed. Refused by noise tool script." %runInfo
    return "ERROR"
  elif exitcode == -2:  # This run is analyzed more than once. Inform this to the shifter.
    print "NOTICE!!!!!!!!!! Run %s is already analyzed, refused to be re-run. Skip submition step of noise tool." %runInfo
    return None
  elif exitcode == -3:  # Inform this to the shifter.
    print "NOTICE!!!!!!!!!! exitcode from noise tool is -3"
  elif exitcode == -4:  # Inform this to the shifter.
    print "NOTICE!!!!!!!!!! exitcode from noise tool is -4"

  # Noise analysis results are now finished. Get DB files
  new = os.path.exists("./run%s/database_new.txt" %runInfo)
  full = os.path.exists("./run%s/database_full.txt" %runInfo)
  if new == False or full == False:
    exitcode_db = subprocess.call("./DBFileTranslationApp/translate_efficiency_and_DB_files.sh %s" %runInfo, shell=True)
    if exitcode_db != 0:     # Something is wrong with DBfile creation.
      print "ERROR!!!!!!!!!! Run %s is not able to make database_* files. Refused by DBFileTranslationApp." %runInfo
      return "ERROR"

  # Add this run to web site.
  if addHtml(runInfo) == "New":
    subprocess.call("./addhtml.sh %s" %runInfo, shell=True)
  elif addHtml(runInfo) == "Registered":
    print "NOTICE!!!!!!!!!! Run %s is already registred at web page. This will be skipped." %runInfo
    return None
  elif addHtml(runInfo) == "ERROR":
    print "ERROR!!!!!!!!!! Something is wrong with grep option in index.html for run%s" %runInfo
    return "ERROR"

  if exitcode == -3 or exitcode == -4:
    return "SKIP"

  return runOK


def contDB(runOK,opt):
  if opt is 1:  #cms_orcoff_prep
    '''
    exitcode = subprocess.call("python geoTable.py -o cms_orcoff_prep -u CMS_COND_RPC_NOISE -p 8B1M410RM1N0RC3SS4T GeometryTableFinal", shell=True)
    if exitcode != 0:
      print "ERROR!!!!!!!!!! geoTable.py doesn't work properly."
      return "ERROR"
    '''
    exitcode = subprocess.call("python runDataToDB_roll.py -o cms_orcoff_prep -u CMS_COND_RPC_NOISE -p 8B1M410RM1N0RC3SS4T  run%s/database_new.txt" %runOK, shell=True)
    if exitcode != 0:
      print "ERROR!!!!!!!!!! runDataToDB_roll.py with %s doesn't work properly." %runOK
      return "ERROR"
    exitcode = subprocess.call("python runDataToDB_strip.py -o cms_orcoff_prep -u CMS_COND_RPC_NOISE -p 8B1M410RM1N0RC3SS4T run%s/database_full.txt" %runOK, shell=True)
    if exitcode != 0:
      print "ERROR!!!!!!!!!! runDataToDB_strip.py with %s doesn't work properly." %runOK
      return "ERROR"
  elif opt is 2:  #OMDS
    acc = 'oracle://cms_omds_lb/CMS_RPC_COND' #'oracle://cms_orcon_prod/CMS_RPC_COND'
    auth = '/nfshome0/popcondev/conddb_taskWriters/RPC/authentication.xml'  #'/nfshome0/popcondev/conddb/authentication.xml'
    exitcode = extract(acc,auth)
    if exitcode is not None:
      conStr = exitcode['connectionstring']
      user = exitcode['user']
      passwd = conStr.split("/")[1].split("@")[0]
      schema = exitcode['schema']
      print acc
      #print user, schema
      # For Geometry_table insertion
      '''
      exitcode = subprocess.call("python geoTable.py -o cms_orcon_prod -u %s -p %s GeometryTableFinal" %(user,passwd), shell=True)
      if exitcode != 0:
        print "ERROR!!!!!!!!!! geoTable.py doesn't work properly."
        return "ERROR"
      '''
      exitcode = subprocess.call("python runDataToDB_roll.py -o cms_orcon_prod -u %s -p %s -S %s run%s/database_new.txt" %(user,passwd,schema,runOK), shell=True)
      if exitcode != 0:
        print "ERROR!!!!!!!!!! runDataToDB_roll.py with %s doesn't work properly." %runOK
        return "ERROR"
      exitcode = subprocess.call("python runDataToDB_strip.py -o cms_orcon_prod -u %s -p %s -S %s run%s/database_full.txt" %(user,passwd,schema,runOK), shell=True)
      if exitcode != 0:
        print "ERROR!!!!!!!!!! runDataToDB_strip.py with %s doesn't work properly." %runOK
        return "ERROR"
    else:
      print "Not able to connect to ORCON."
      return "ERROR"
  else: # sqlite3
    exitcode = subprocess.call("python runDataToDB_roll.py -s sqliteDB.db run%s/database_new.txt" %runOK, shell=True)
    if exitcode != 0:
      print "ERROR!!!!!!!!!! runDataToDB_roll.py with %s doesn't work properly." %runOK
      return "ERROR"
    exitcodeFull = subprocess.call("python runDataToDB_strip.py -s sqliteDB.db run%s/database_full.txt" %runOK, shell=True)
    if exitcodeFull != 0:
      print "ERROR!!!!!!!!!! runDataToDB_strip.py with %s doesn't work properly." %runOK
      return "ERROR"
    

def summary(runOK,runNot,runErr,runInit):
  print "\nNOT analyzed runs:"
  for idx in range(0,len(runNot)):
    print runNot[idx]
  print "\nRuns with ERROR:"
  for idx in range(0,len(runErr)):
    print runErr[idx]
  print "\nRuns end properly:"
  for idx in range(0,len(runOK)):
    print runOK[idx]

  fres = open("result_noiseDataChain.txt","w")
  fres.write("NOT analyzed runs:\n")
  for idx in range(0,len(runNot)):
    fres.write(str(runNot[idx]))
    fres.write("\n")
  fres.write("\nRuns with ERROR:\n")
  for idx in range(0,len(runErr)):
    fres.write(str(runErr[idx]))
    fres.write("\n")
  fres.write("\nRuns end properly:\n")
  for idx in range(0,len(runOK)):
    fres.write(str(runOK[idx]))
    fres.write("\n")
  
  fres.write("\n")
  last = []   # the last run number
  runNot.sort()
  runErr.sort()
  runOK.sort()
  if len(runNot) is not 0:
    last.append(runNot[-1])
  if len(runErr) is not 0:
    last.append(runErr[-1])
  if len(runOK) is not 0:
    last.append(runOK[-1])
  last.sort()
  fres.write("The latest analyzed run number is\n")
  if len(last) is not 0:
    fres.write(str(last[-1]))
  else:
    fres.write(str(runInit))
  
  fres.close()
  sys.exit(0)

#############################
########## M A I N ##########
#############################
import sys,os,string,subprocess
sys.path.append("/nfshome0/mmaggi/PYTHON")
try:
  import xmlrpclib
except:
  print "Please use lxplus"
  sys.exit(-1)

try:
  import cx_Oracle
except:
  print "cx_Oracle fail"
  sys.exit(-1)


########## Get arguments
runOptions = []

try:
  int(sys.argv[1])
except:
  print "\nUsage:\npython %s [Option] [Run Type] [Start run] [End run] [Run duration]" %sys.argv[0]
  print "Option:"
  print "1: Get run numbers from Run Registry site, run noise tool with them, connect to test DB."
  print "   Need start run type, run number, end run number, run duration in unit of sec(>=1200)."
  print "2: Use the run list in 'run_list_to_db.txt', connect to OMDS."
  print "3: Use the run list in 'run_list_to_db.txt', connect to test DB."
  print "4: Use the run list in 'run_list_to_db.txt', test with sqlite3."
  print "======= run_list_to_db.txt should contain 1 run number per 1 line. =======\n"
  sys.exit(-1)

if len(sys.argv) == 6:
  try:
    int(sys.argv[3])
    int(sys.argv[4])
    int(sys.argv[5])
  except:
    print "\nUsage:\npython %s [Option] [Run Type] [Start run] [End run] [Run duration]" %sys.argv[0]
    print "Option:"
    print "1: Get run numbers from Run Registry site, run noise tool with them, connect to test DB."
    print "   Need start run type, run number, end run number, run duration in unit of sec(>=1200)."
    print "2: Use the run list in 'run_list_to_db.txt', connect to OMDS."
    print "3: Use the run list in 'run_list_to_db.txt', connect to test DB."
    print "4: Use the run list in 'run_list_to_db.txt', test with sqlite3."
    print "======= run_list_to_db.txt should contain 1 run number per 1 line. =======\n"
    sys.exit(-1)
  runOptions.append(int(sys.argv[1]))
  runOptions.append(sys.argv[2])
  runOptions.append(int(sys.argv[3]))
  runOptions.append(int(sys.argv[4]))
  runOptions.append(int(sys.argv[5]))
elif int(sys.argv[1]) == 2 or int(sys.argv[1]) == 3 or int(sys.argv[1]) == 4 or int(sys.argv[1]) == 5:
  runOptions.append(int(sys.argv[1]))
else:
  print "\nUsage:\npython %s [Option] [Run Type] [Start run] [End run] [Run duration]" %sys.argv[0]
  print "Option:"
  print "1: Get run numbers from Run Registry site, run noise tool with them, connect to test DB."
  print "   Need start run type, run number, end run number, run duration in unit of sec(>=1200)."
  print "2: Use the run list in 'run_list_to_db.txt', connect to OMDS."
  print "3: Use the run list in 'run_list_to_db.txt', connect to test DB."
  print "4: Use the run list in 'run_list_to_db.txt', test with sqlite3."
  print "======= run_list_to_db.txt should contain 1 run number per 1 line. =======\n"
  sys.exit(-1)

runOK = []
runNot = []
runErr = []
os.environ["PYTHONPATH"]="/nfshome0/mmaggi/PYTHON"
if runOptions[0] == 55:  # Just get runnumbers from RR
  frunnum = open("run_list_to_db.txt","w")
  runInfo = connectRR_v3_API(runOptions[1], runOptions[2],runOptions[3],runOptions[4])
  for idx in range(0,len(runInfo)):
    frunnum.write(runInfo[idx])
    frunnum.write("\n")
  frunnum.close()
elif runOptions[0] == 1:
  runInfo = connectRR_v3_API(runOptions[1],runOptions[2],runOptions[3],runOptions[4])
  for idx in range(0,len(runInfo)):
    run = runNoise(runInfo[idx])
    if run is None:
      runNot.append(runInfo[idx])
    elif run is "ERROR":
      runErr.append(runInfo[idx])
    elif run is "SKIP": #exitcode -3, -4 case: do not insert data into DB
        runOK.append(run)
    else:
      db = contDB(run,2)
      if db is "ERROR":
        runErr.append(runInfo[idx])
      else:
        runOK.append(run)
else:
  try:
    fin = open("./run_list_to_db.txt","r")
  except:
    print "ERROR!!!!!!!!!! Chosen option needs 'run_list_to_db.txt' file, but it doesn't exist."
    sys.exit(-1)

  runInfo = []
  for line in fin:
    runInfo.append(line.split("\n")[0])

  if runOptions[0] == 2:
    for idx in range(0,len(runInfo)):
      run = runNoise(runInfo[idx])
#      run = runInfo[idx]
      if run is None:
        runNot.append(runInfo[idx])
      elif run is "ERROR":
        runErr.append(runInfo[idx])
      elif run is "SKIP": #exitcode -3, -4 case: do not insert data into DB
        runOK.append(run)
      else:
        print "Data will be inserted into ORCON"  #option 2 is omds
        db = contDB(run,2)
        if db is "ERROR":
          runErr.append(runInfo[idx])
        else:
          runOK.append(run)
  elif runOptions[0] == 3:
    for idx in range(0,len(runInfo)):
      run = runNoise(runInfo[idx])
      if run is None:
        runNot.append(runInfo[idx])
      elif run is "ERROR":
        runErr.append(runInfo[idx])
      elif run is "SKIP": #exitcode -3, -4 case: do not insert data into DB
          runOK.append(run)
      else:
        print "Data will be inserted into test DB"  #option is 1 test DB
        db = contDB(run,1)
        if db is "ERROR":
          runErr.append(runInfo[idx])
        else:
          runOK.append(run)
  elif runOptions[0] == 4:
    for idx in range(0,len(runInfo)):
      run = runNoise(runInfo[idx])
      if run is None:
        runNot.append(runInfo[idx])
      elif run is "ERROR":
        runErr.append(runInfo[idx])
      elif run is "SKIP": #exitcode -3, -4 case: do not insert data into DB
          runOK.append(run)
      else:
        print "Data will be inserted into sqlite3"
        db = contDB(run,2)
        if db is "ERROR":
          runErr.append(runInfo[idx])
        else:
          runOK.append(run)
  else:
    print "ERROR!!!!!!!!!! Please choose program options in 1,2,3,4 only."
    sys.exit(-1)

########## Finally done!
if runOptions[0] != 55:  # Just get runnumbers from RR
  if runOptions[0] == 1:
    summary(runOK,runNot,runErr,runOptions[2])
  else:
    summary(runOK,runNot,runErr,runInfo[-1])

