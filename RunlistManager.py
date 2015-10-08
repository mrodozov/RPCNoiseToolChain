#runlist ideas - single file, JSON, keeps track

from RRService import RRService
import json
import time
import socks
from threading import Thread, Lock
import Queue

class RunlistManager(Thread):

    '''
    run list manager manages
    1. file with runs
    2. new runs using the RRApi
    3. changes
    '''

    # TODO - use Lock to acquire and release the runlist dictionary object since there are two threads that may modify it 1. Update after new runs from the RRApi 2. Update after run has been processed

    def __init__(self, runlist=None):
        '''
        :param runlist: run list is file with json description of runs analyzed.
        :return: none
        '''
        super(RunlistManager, self).__init__()
        self.runlist = {}
        self.rr_connector = None
        self.toProcessQueue = None
        self.processedRunsQueue = None
        self.reportQueue = None
        self.processed_runs_thread = Thread(target=self.handleProcessedRuns)
        self.check_run_registry = Thread(target=self.checkRRforNewRuns)
        self.stop_event = None
        self.loadRunlistFile(runlist)

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
        if run in self.runlist.keys():
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
            with open(runlist) as data_file:
                try:
                    data = json.load(data_file)
                except Exception, e:
                    print e.message
                for k, v in data.items():
                    self.runlist[k] = v
                retval = True
                data_file.close()
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


        with open(runlistFile,"w") as runFile:
            runFile.write(json.dumps(self.runlist, indent=1, sort_keys=True))
            runFile.close()
            retval = True

        return retval

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
            r_list = self.getListOfRunsToProcess()
            for k in r_list.keys():
                self.toProcessQueue.put({k:r_list[k]})
        except Exception as e:
            print e.message

    def sortRunlist(self, runlist = None):
        runlist = []
        # change execution order
        return runlist

    def handleProcessedRuns(self):
        while True:
            run = self.processedRunsQueue.get()
            # check status and update the run
            rnum = None
            for r in run.keys():
                rnum = r
            run_details = run[rnum].get()
            print 'Run ', rnum, ' in results'
            print run_details.keys()
            run_results = run_details['results']
            run_warnings = run_details['warnings']
            run_logs = run_details['logs']
            #print run_results
            # check the status
            for k in run_results:
                if run_results[k][0] == 'Failed':
                    self.reportQueue.put({rnum:{"warnings":run_warnings, "logs":run_logs}})
                    break


            self.processedRunsQueue.task_done()

            if self.stop_event.is_set() and self.processedRunsQueue.empty():
                # do some finishing stuff
                print 'Processed runs handled'
                break

    def checkRRforNewRuns(self):
        while True:
            # get the last run number from the new runs
            # if new runs arrived,
            last_run = None
            self.runlist.keys().sort()
            if self.runlist.keys(): last_run = last[0]
            new_runs = self.rr_connector.getRunRangeWithLumiInfo()
            # mark as submitted and when uploading runlist don't get those not finished

            time.sleep(5)

            print 'RR checked'

            if self.stop_event.is_set() and self.processedRunsQueue.empty():
                print 'RR exits'
                break


    def run(self):
        self.check_run_registry.start()
        self.processed_runs_thread.start()
        self.check_run_registry.join()
        self.processed_runs_thread.join()

    #def getSortedListOfRuns(self):


if __name__ == "__main__":

    runsToProcessQueue = Queue.Queue()
    processedRunsQueue = Queue.Queue()
    reportsQueue = Queue.Queue()

    socks.setdefaultproxy(socks.PROXY_TYPE_SOCKS5, '127.0.0.1', 1080)
    rr_obj = RRService(use_proxy=True)
    rlistMngr = RunlistManager('resources/runlist.json')
    rlistMngr.toProcessQueue = runsToProcessQueue
    rlistMngr.processedRunsQueue = processedRunsQueue
    rlistMngr.reportQueue = reportsQueue
    rlistMngr.rr_connector = rr_obj


    #rlist = rr_obj.getRunRange('Collisions15', '257000', '600')
    #print rlist
    #rlist = rr_obj.getRunlistForLongRuns(258000)
    #print rlist
    #lumis = rr_obj.getRunsLumiSectionsInfo(lastRun=257000)
    #print lumis
    res = rr_obj.getRunRangeWithLumiInfo('Collisions15', '257000', '600')
    print res
    last = res.keys()
    res.keys().sort()
    print last[-1:]
