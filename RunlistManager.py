#runlist ideas - single file, JSON, keeps track

from RRService import RRService , RRApiError
import json
import time
import socks
from threading import Thread, Lock, Event, RLock
import Queue
import copy
from DBService import DBService
import datetime
import os.path
import shutil
# TODO - Figure it how to synch changes from the remote runlist. Another thread in runlist is required to 1. Keep time of last modification on the remote rl, and verify it. if changed, get, remote rl file
# TODO - Synch the remote runlist only if toProcess and processed queues are empty ?

class RunlistManager(Thread):

    '''
    run list manager manages
    1. file with runs
    2. new runs using the RRApi
    3. changes in the remote location runlist (resubmit queries)
    '''

    def __init__(self, runlist_file=None):

        super(RunlistManager, self).__init__()
        self.runlist = {} #lock when modifying the list
        self.rr_connector = None
        self.toProcessQueue = None
        self.processedRunsQueue = None
        self.reportQueue = None
        self.check_completed_runs = Thread(target=self.handleProcessedRuns)
        self.check_run_registry = Thread(target=self.checkRRforNewRuns)
        self.check_remote_runlist = Thread(target=self.synchronizeRemoteRunlistFile)
        self.stop_event = None
        self.suspendRRcheck = Event()
        self.suspendProcessedRunsHandler = Event()
        self.runlist_file = runlist_file
        self.loadRunlistFile(runlist_file)
        self.runlistLock = RLock()
        self.runlist_sftp_client = None

    def __del__(self):
        # make sure clear all the queues, stop threads and delete them
        return None

    def set_current_runlist_file(self, current_runlist_file=None):
        self.runlist_file = current_runlist_file

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

    def updateRunlistFile(self, runlistFile=None):
        if not runlistFile: runlistFile = self.runlist_file
        ts = str(datetime.datetime.now().replace(microsecond=0))
        timest = str.split(ts)[0].replace('-','_')+'_'+str.split(ts)[1].replace(':','_')
        filename = os.path.splitext(runlistFile)[0]
        extension = os.path.splitext(runlistFile)[1]
        backup = filename+'_'+timest+extension
        fileonly = str(runlistFile).rsplit('/',1)[-1]
        folder = str(runlistFile).replace(fileonly,'')
        for f in os.listdir( folder ) :
            if f.endswith(extension) and f.find(fileonly.replace(extension,'')) is not -1 and f != fileonly:
                #print folder+f
                os.remove(folder+f)
        retval = False
        with self.runlistLock:
            shutil.copyfile(runlistFile, backup)
            with open(runlistFile, "w") as runFile:
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

    def sortRunlist(self, runlist = None):
        sorted_runlist = {}
        # change execution order
        sorted_runlist = runlist # implement new first (Coll, Cosm, Comm), failed second (Coll, Cosm, Comm), and toresubmit last
        return sorted_runlist

    def handleProcessedRuns(self):
        while True:
            #self.suspendProcessedRunsHandler.wait() # block in case it needs to wait until network srvc is restarted
            run = self.processedRunsQueue.get()
            # check status and update the run
            rnum = None
            run_details = {}
            run_results = None
            for r in run.keys():
                rnum = r
            run_details = run[rnum]
            #print 'Run ', rnum, ' in results'
            run_status = 'Failed'
            try:
                #print run_details.keys()
                run_results = run_details['results']
                for k in run_results:
                    run_status = 'finished'
                    if run_results[k][0] == 'Failed':
                        run_status = 'Failed'
                        #self.updateRun(rnum, 'status', run_status)
                        run[rnum]['status'] = run_status
                        self.reportQueue.put(run)
                        break
            except KeyError, e:
                e.message
            #update runlist
            self.updateRun(rnum, 'status', run_status)
            self.processedRunsQueue.task_done()

            #update runlist when the processed queue is empty -
            if self.processedRunsQueue.empty() and self.toProcessQueue.empty():
                self.synchronizeRemoteRunlistFile()

            if self.stop_event.is_set() and self.processedRunsQueue.empty():
                # do some finishing stuff
                print 'Processed runs handled'
                break

    def checkRRforNewRuns(self):
        init_sleep_time = 10
        current_sleep_time = 0
        while True:
            #print 'before wait'
            self.suspendRRcheck.wait()
            #print 'after wait'
            # get the last run number from runlist, first lock it with Lock
            runlist_runs = None
            with self.runlistLock:
                runlist_runs = self.runlist.keys()
                runlist_runs.sort()
            last_run = runlist_runs[-1]
            #print 'last run is: ', last_run
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
            #print self.runlist

            print 'go to sleep for', current_sleep_time , 'seconds'
            print 'RR checked last run ', last_run
            time.sleep(current_sleep_time)

            if self.stop_event.is_set() and self.processedRunsQueue.empty():
                print 'RR exits'
                break

    def synchronizeRemoteRunlistFile(self):

        with self.runlistLock:
            try:
                remote_dir = self.runlist_sftp_client.getcwd()
                remote_runlist = json.loads(self.runlist_sftp_client.file(remote_dir+'/runlist.json').read())
                list_to_resub = [r for r in remote_runlist.keys() if remote_runlist[r]['status'] == 'toresub']
                for r in list_to_resub:
                    self.updateRun(r, 'status', 'submitted')
                self.updateRunlistFile()
                #upload the local on remote like this now
                self.runlist_sftp_client.put(self.runlist_file, remote_dir+'/runlist.json')
                #now change the local run keys to 'toresub'. this way runs goes to be processed
                for r in list_to_resub:
                    self.updateRun(r, 'status', 'toresub')
                #self.updateRunlistFile()
                # test this

            except Exception, e:
                print e.message


    def run(self):
        self.check_run_registry.start()
        self.check_completed_runs.start()
        self.check_remote_runlist.start()
        self.check_run_registry.join()

    #def getSortedListOfRuns(self):

def moveQueueEntries(inputQueue = None, outputQueue = None):
    even = True
    counter = 1
    while True:
        counter += 1
        input = inputQueue.get()
        rnum = None
        for k in input.keys():
            rnum = k
        sumdict = {}
        sumdict[rnum]= copy.deepcopy(input[rnum])
        result = ['finished']
        if counter % 2 == 0: result = ['Failed']
        sumdict[rnum]['results'] = {'test':['finished'], 'sectest': result}
        sumdict[rnum]['warnings'] = {'test':['finished'], 'sectest': result}
        sumdict[rnum]['logs'] = {'test':['finished'], 'sectest': result}
        outputQueue.put(sumdict)
        inputQueue.task_done()

def getReportQueueEntries(reportQueue = None):
    while True:
        record = reportQueue.get()
        print 'from report thread: ', record.keys()
        print 'post a report: '
        for k in record.keys():
            print k, record[k]
        reportQueue.task_done()

if __name__ == "__main__":


    runsToProcessQueue = Queue.Queue()
    processedRunsQueue = Queue.Queue()
    reportsQueue = Queue.Queue()
    stop_rlistmngr = Event()  # set this to kill the loop and exit
    mq = Thread(target=moveQueueEntries, args=(runsToProcessQueue, processedRunsQueue))
    rq = Thread(target=getReportQueueEntries, args=(reportsQueue,))

    socks.setdefaultproxy(socks.PROXY_TYPE_SOCKS5, '127.0.0.1', 1080)

    rr_obj = RRService(use_proxy=True)
    rlistFile = 'resources/runlist.json'
    rlistMngr = RunlistManager(rlistFile)
    rlistMngr.toProcessQueue = runsToProcessQueue
    rlistMngr.processedRunsQueue = processedRunsQueue
    rlistMngr.reportQueue = reportsQueue
    rlistMngr.rr_connector = rr_obj
    rlistMngr.stop_event = stop_rlistmngr
    rlistMngr.suspendRRcheck.set()
    rlistMngr.check_run_registry.start()

    delay = 10
    while delay != 0:
        time.sleep(1)
        print 'Start in: ', delay
        delay = delay - 1
    mq.start()

    # postpone processed queue
    delay = 10
    while delay != 0:
        time.sleep(1)
        print 'wait for  in: ',delay
        delay = delay - 1

    rlistMngr.check_completed_runs.start()
    rq.start()






