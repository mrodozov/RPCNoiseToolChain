# application to carry the chain
# creates the objects required, hook them up and follows a logic
#

from RequirementsManager import EnvHandler
from RunlistManager import RunlistManager
import os

'''
eHandler = EnvHandler('resources/ListOfTunnels.json','resources/process.json' ,'resources/variables.json', False)

tunnels = eHandler.listOfTunnels
processes = eHandler.listOfProcesses
evars = eHandler.listOfEnvVars

eHandler.checkListOfTunnels()
eHandler.checkListOfProcesses()
'''

'''
#some ReqManager tests
for p in processes:
    print p.name

for ev in evars:
    for k, v in ev.items():
        os.environ[k] = v
        print os.environ[k]

print os.environ["USER"]
'''
'''
RR_URL = "runregistry.web.cern.ch/runregistry/"
runlist = 'resources/runlist.json'

runManager = RunlistManager(runlist)
jsonlist = runManager.runlist

for k in jsonlist:
    print jsonlist.get(k)

updated = runManager.updateRun("190002", "status", "finished")

print updated

for k in jsonlist:
    print jsonlist.get(k)

updated = runManager.updateRun("190300","status","finished")

print updated

listofkeys = runManager.runlist.keys()

for k in listofkeys:
    print k

shotlist = runManager.getListOfRunsToProcess()
print shotlist

'''

#print cosmicList

