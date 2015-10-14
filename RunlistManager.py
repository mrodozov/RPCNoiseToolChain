#runlist ideas - single file, JSON, keeps track

from RRService import RRService , RRApiError
import json
import time
import socks
from threading import Thread, Lock, Event
import Queue

# TODO - Figure it how to synch changes from the remote runlist. Another thread in runlist is required to 1. Keep time of last modification on the remote rl, and verify it. if changed, get, remote rl file

class RunlistManager(Thread):

    '''
    run list manager manages
    1. file with runs
    2. new runs using the RRApi
    3. changes in the remote location runlist (resubmit queries)
    '''

    def __init__(self, runlist=None):


        super(RunlistManager, self).__init__()
        self.runlist = {} #lock when modify the list
        self.rr_connector = None
        self.toProcessQueue = None
        self.processedRunsQueue = None
        self.reportQueue = None
        self.check_completed_runs = Thread(target=self.handleProcessedRuns)
        self.check_run_registry = Thread(target=self.checkRRforNewRuns)
        self.stop_event = None
        self.suspendRRcheck = Event()
        self.suspendProcessedRunsHandler = Event()
        self.loadRunlistFile(runlist)
        self.runlistLock = Lock()

    def __del__(self):
        # make sure clear all the queues, stop threads and delete them
        return None

    def getFirstLastRuns(self):
        if self.runlist is not None:
            runnums = self.runlist.keys()
            runnums.sort()
        return [runnums[0], runnums[-1]]

    def updateRun(self,run,key,value):
        if run in self.runlist.keys():
            with self.runlistLock:
                self.runlist[run][key] = value

    def addruns(self, runs):
        with self.runlistLock:
            self.runlist.update(runs)

    def updateRunlist(self):
        pass

    def loadRunlistFile(self, runlist=None):
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

        #TODO - implement protection, like copying the file opened with another name and then writing. UPDATE - implement archive, not protection

        with open(runlistFile,"w") as runFile:
            runFile.write(json.dumps(self.runlist, indent=1, sort_keys=True))
            runFile.close()
            retval = True

        return retval

    def getListOfRunsToProcess(self):

        shortlist = {}
        with self.runlistLock:
            for run in self.runlist:
                stat = self.runlist[run]['status']
                if stat != 'finished' and stat != 'submitted' and stat != 'Failed':
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
        sorted_runlist = {}
        # change execution order
        sorted_runlist = runlist # implement new first (Coll, Cosm, Comm), failed second (Coll, Cosm, Comm), and toresubmit last
        return sorted_runlist

    def handleProcessedRuns(self):
        while True:
            run = self.processedRunsQueue.get()
            # check status and update the run
            rnum = None
            run_details = {}
            run_results = None
            for r in run.keys():
                rnum = r
            run_details = run[rnum]
            print 'Run ', rnum, ' in results'
            run_status = 'finished'
            try:
                print run_details.keys()
                run_results = run_details['results']

                for k in run_results:
                    run_status = 'finished'
                    if run_results[k][0] == 'Failed':
                        run_status = 'Failed'
                        self.reportQueue.put(run)
                        break
            except KeyError, e:
                e.message
            #update runlist
            self.updateRun(rnum, 'status', run_status)
            self.processedRunsQueue.task_done()

            if self.stop_event.is_set() and self.processedRunsQueue.empty():
                # do some finishing stuff
                print 'Processed runs handled'
                break

    def checkRRforNewRuns(self):
        init_sleep_time = 10
        current_sleep_time = 0
        while True:
            print 'before wait'
            self.suspendRRcheck.wait()
            print 'after wait'
            # get the last run number from runlist, first lock it with Lock
            self.runlistLock.acquire()
            runlist_runs = self.runlist.keys()
            self.runlistLock.release()
            runlist_runs.sort()
            last_run = runlist_runs[-1]
            print 'last run is: ', last_run
            year = time.strftime('%y')
            new_runs = {}
            try:
                new_runs = self.rr_connector.getRunRangeWithLumiInfo('Collisions'+year+' OR Cosmics'+year+' OR Commissioning'+year, last_run, 300)
            except RRApiError, e:
                print e.message, e.code#
            if new_runs : current_sleep_time = init_sleep_time # reset when new runs arrived
            current_sleep_time += init_sleep_time # increments every time with one minute if RR does not return new runs
            # if new runs are available, put them on the runlist
            self.addruns(new_runs)
            # get runs to process
            list_to_process = self.getListOfRunsToProcess()
            # order them and put on queue
            ordered_list_to_process = self.sortRunlist(list_to_process)
            # mark as submitted and when uploading runlist don't get those not finished

            for r in ordered_list_to_process.keys():

                self.updateRun(r, 'status', 'submitted')
                self.toProcessQueue.put({r: self.runlist[r]})

            #if new_runs : print ordered_list_to_process.keys()
            print self.runlist

            print 'go to sleep for', current_sleep_time , 'seconds'
            print 'RR checked last run ', last_run
            time.sleep(current_sleep_time)

            if self.stop_event.is_set() and self.processedRunsQueue.empty():
                print 'RR exits'
                break


    def run(self):
        self.check_run_registry.start()
        self.check_completed_runs.start()
        self.check_run_registry.join()
        self.check_completed_runs.join()

    #def getSortedListOfRuns(self):

def moveQueueEntries(inputQueue = None, outputQueue = None):
    while True:
        input = inputQueue.get()
        sumdict = input.copy()
        rnum = [r for r in input.keys()][0]
        sumdict[rnum]['results'] = {'test':['finished'], 'sectest': ['Failed']}
        sumdict[rnum]['warnings'] = {'test':['finished'], 'sectest': ['Failed']}
        sumdict[rnum]['logs'] = {'test':['finished'], 'sectest': ['Failed']}
        outputQueue.put(sumdict)
        inputQueue.task_done()

if __name__ == "__main__":

    runsToProcessQueue = Queue.Queue()
    processedRunsQueue = Queue.Queue()
    reportsQueue = Queue.Queue()
    stop_rlistmngr = Event()  # set this to kill the loop and exit
    mq = Thread(target=moveQueueEntries, args=(runsToProcessQueue, processedRunsQueue))

    socks.setdefaultproxy(socks.PROXY_TYPE_SOCKS5, '127.0.0.1', 1080)
    rr_obj = RRService(use_proxy=True)
    rlistFile = 'resources/runlist.json'
    rlistMngr = RunlistManager(rlistFile)
    rlistMngr.toProcessQueue = runsToProcessQueue
    rlistMngr.processedRunsQueue = processedRunsQueue
    rlistMngr.reportQueue = reportsQueue
    rlistMngr.rr_connector = rr_obj
    rlistMngr.stop_event = stop_rlistmngr
    rlistMngr.check_run_registry.start()

    delay = 10
    while delay != 0:
        time.sleep(1)
        print 'Start in: ', delay
        delay = delay - 1
    rlistMngr.suspendRRcheck.set()
    mq.start()

    # postpone processed queue
    delay = 10
    while delay != 0:
        time.sleep(1)
        print 'wait for  in: ',delay
        delay = delay - 1

    rlistMngr.check_completed_runs.start()



