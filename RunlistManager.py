#runlist ideas - single file, JSON, keeps track

from RRService import RRService
import json, simplejson, sys

class RunlistManager:

    '''
    run list manager manages
    1. file with runs
    2. new runs using the RRApi
    3. changes
    '''

    runlist = {} #runlist is dictionary with runs, and their corresponding status. the status is
    RRConnector = RRService()
    readyToUse = False

    def __init__(self,runlist=None):
        '''
        :param runlist: run list is file with json description of runs analyzed.
        :return: none
        '''
        self.toProcessQueue = None
        self.processedRunsQueue = None
        runlistLoaded = False
        if runlist is not None:
            self.loadRunlistFile(runlist)
            runlistLoaded = True
        print runlist, ' is loaded ?: ', runlistLoaded

    def __del__(self):
        #do some shit
        return None

    def getFirstLastRuns(self):
        listOfFirstAndLast = []

        if self.runlist is not None:
            runnums = self.runlist.keys()
            runnums.sort()
            listOfFirstAndLast.append(runnums[0])
            listOfFirstAndLast.append(runnums[-1])
        return listOfFirstAndLast

    def updateRun(self,run,key,value):
        '''
        :param run: run number
        :param key: key in run object
        :param value: new value
        :return: success on change
        '''
        success = False
        if run in self.runlist:
            self.runlist[run][key] = value
            success = True
        return success

    def loadRunlistFile(self,runlist=None):
        '''
        :param runlist: run list is file with json description of runs analyzed.
        :return: success of loading
        '''
        retval = False
        if runlist is not None:
            try:
                with open(runlist) as data_file:
                    data = json.load(data_file)
                    for k, v in data.items():
                        self.runlist[k] = v
                    retval = True
                    data_file.close()
            except Exception, e:
                print e.message
        return retval

    def updateRunlistFile(self,runlistFile,oldFile=None):
        '''
        writes the runlist file using the merged runlist object. careful with this method ! it writes the file with runs, so if the runlist is emptied, it may wipe out the history !
        :param runlistFile: runlist file
        :param oldFile: previous version of the file, backup
        :return: success
        '''
        retval = False
        #TODO - implement protection, like copying the file opened with another name and then writing. UPDATE - implement archive, not protection

        try:
            with open(runlistFile,"w") as runFile:
                runFile.write(json.dumps(self.runlist, indent=1, sort_keys=True))
                runFile.close()
                retval = True
        except Exception, e:
            print e.message
        return retval

    def getFullRunlist(self,runTypesFilter,RRURL):

        '''
        :return: merged runlist, from file AND run registry.
        '''
        latestRun =  self.runlist.keys()
        latestRun.sort()
        latestRun = latestRun[-1]

        '''
        try:
            listofRuns = self.RRConnector.getRunRange(runTypesFilter,latestRun,1,RRURL)
            if listofRuns is not  None:
                 for run in listofRuns:
                     self.runlist[str(run['duration'])] = {'Type': run['runClassName'], 'status': 'new', 'duration': str(run['duration'])}
        except Exception, e:
            print e.message
        '''

        return self.runlist

    def getListOfRunsToProcess(self):

        '''
        get only the runs to be processed
        :return: return only runs to be processed from all, as dictionary of run{}
        '''
        shortlist = {}
        for run in self.runlist:
            if self.runlist[run]['status'] != 'finished':
                shortlist[run] = self.runlist[run]
        return shortlist

    def putRunsOnProcessQueue(self, runlist = None):
        try:
            for r in runlist:
                self.toProcessQueue.put(r)
        except Exception as e:
            print e.message


    #def getSortedListOfRuns(self):


if __name__ == "__main__":
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

    shortlist = runManager.getListOfRunsToProcess()
    print shortlist

    longlist = runManager.getFullRunlist('Collissions15 OR Cosmics15 OR Commissioning15', 'somehttp')
    runManager.runlist['250001'] = {"status": "new",   "duration": "118", "Type": "Collisions15"}

    print "\n", "\n"
    longlist = runManager.getFullRunlist('Collissions15 OR Cosmics15 OR Commissioning15','somehttp')
    print json.dumps(longlist, indent=1, sort_keys=True)

    runManager.updateRunlistFile('resources/runlist.json')

    listfl = runManager.getFirstLastRuns()

    print listfl


    #runManager.getFullRunlist()

    #k , update is working

