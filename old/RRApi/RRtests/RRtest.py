#!/usr/bin/python
import xmlrpclib
runInfo = []
runType = 'Commissioning12'
startRun = '190645'
endRun = '203002'
runDuration = '10'
rpcPresent = 'true'

server = xmlrpclib.ServerProxy("http://localhost:40010/cms-service-runregistry-api/xmlrpc") # returns connection refused error 111
#server = xmlrpclib.ServerProxy("http://cms-service-runregistry-api.web.cern.ch/cms-service-runregistry-api/xmlrpc"); #prompts forever
#select_good = "{groupName} = '%s' and  {runNumber} >= %s and {runNumber} <= %s and {duration} >= %s and {cmpRpc} = \'GOOD\'" %(runType, startRun,endRun,runDuration)

select_good = "{runNumber} >= %s and {runNumber} <= %s and {groupName} = '%s' and {duration} >= %s" %(startRun ,endRun ,runType ,runDuration)
print select_good
runData_good = server.DataExporter.export('RUN','GLOBAL','xml_all',select_good)

for run in runData_good.split("\n"):
    if "<NUMBER>" in run:
        runInfo.append(run[8:-9])
#        print run[8:-9]
runInfo.sort()
for entry in runInfo:
    print entry
'''        
print "--------------------------SEPARATION LINE---------------------------------"
'''
#print runData_good
