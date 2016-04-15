#runlist ideas - single file, JSON, keeps track

from RRService import RRService
import json
import time
import socks
from threading import Thread, Event, RLock
import Queue
import copy
import datetime
import os.path
import shutil
import paramiko

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
        self.stop_event = None
        self.suspendRRcheck = None
        self.suspendProcessedRunsHandler = None
        self.runlist_file = runlist_file
        self.loadRunlistFile(runlist_file)
        self.runlistLock = RLock()
        self.ssh_service = None
        self.runlist_remote_dir = None

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

            rnum = run.keys()[0]
            run_details = run[rnum].get()
            #print 'run details ', run_details
            run_status = 'finished' # to remove, its for a test
            try:
                # print run_details.keys()
                run_results = run_details['results']
                for i in run_results.keys():
                    if run_results[i][0] == 'Failed':
                        print i, run_results[i][0]
                        run_status = 'Failed'
                        self.reportQueue.put({rnum: run_details})
            except Exception, e:
                print e.message, ', deiba'
            #update runlist
            self.updateRun(rnum, 'status', run_status)
            self.processedRunsQueue.task_done()

            #update runlist when the processed queue is empty -
            if self.processedRunsQueue.empty() and self.toProcessQueue.empty():
                self.synchronizeRemoteRunlistFile()
                print 'runlist synched'

            if self.stop_event.is_set() and self.processedRunsQueue.empty():
                # do some finishing stuff
                print 'Processed runs handled'
                break

    def checkRRforNewRuns(self):
        init_sleep_time = 10
        current_sleep_time = 0
        while True:
            self.suspendRRcheck.wait()

            # get the last run number from runlist, first lock it with Lock
            runlist_runs = None
            with self.runlistLock:
                runlist_runs = self.runlist.keys()
                runlist_runs.sort()
            last_run = runlist_runs[-1]
            print 'last run is: ', last_run
            year = time.strftime('%y')
            #year = '15' # TODO - remove this
            new_runs = {}
            try:
                new_runs = self.rr_connector.getRunRangeWithLumiInfo('Collisions'+year+' OR Cosmics'+year+' OR Commissioning'+year+'', last_run, 300)
            except Exception, e:
                print e.message, ' RR check failed at ', datetime.datetime.now().replace(microsecond=0)
                # try to set semaphore that calls the env handler to reestablish the channel to RR
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

                #print 'before status change run', r, ordered_list_to_process[r]
                rn_copy = copy.deepcopy(r)
                rd_copy = copy.deepcopy(self.runlist[r])
                self.toProcessQueue.put({rn_copy: rd_copy})

                self.updateRun(r, 'status', 'submitted')

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

                # if the connection to transport has been closed, there is no way to be reestablished
                # (aaaand - should be)
                # so its better the rlist mngr keeps a reference to the service itself
                # (some service, any service that is already established for the manager)

                runlist_sftp_client = paramiko.SFTPClient.from_transport(self.ssh_service.get_transport())
                remote_runlist = json.loads(runlist_sftp_client.file(self.runlist_remote_dir+'runlist.json').read())
                list_to_resub = [r for r in remote_runlist.keys() if remote_runlist[r]['status'] == 'toresub']
                for r in list_to_resub:
                    self.updateRun(r, 'status', 'submitted')
                self.updateRunlistFile()
                #upload the local on remote like this now
                runlist_sftp_client.put(self.runlist_file, self.runlist_remote_dir+'/runlist.json')
                runlist_sftp_client.get_channel().close()
                #now change the local run keys to 'toresub'. this way runs goes to be processed
                for r in list_to_resub:
                    self.updateRun(r, 'status', 'toresub')
                #self.updateRunlistFile()
                # test this

            except Exception, e:
                print e.message # this exception is not printed

    def run(self):
        self.check_run_registry.start()
        self.check_completed_runs.start()
        self.check_run_registry.join()
        self.check_completed_runs.join()

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
    suspendRunRegistry = Event()
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
    rlistMngr.suspendRRcheck = suspendRunRegistry
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






