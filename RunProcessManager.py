__author__ = 'rodozov'

from CommandClasses import *
from Chain import Chain, EventsHandler
from Event import SimpleEvent
from RunlistManager import RunlistManager
import multiprocessing as mp
from multiprocessing.pool import ThreadPool
import Queue
import time
from threading import Thread
import copy

'''


'''

class RunProcessPool(Thread):

    def __init__(self, runs_to_process_queue=None, processed_runs_queue=None, sequence_handler_object=None, options=None):

        super(RunProcessPool, self).__init__()
        self.options = options
        self.pool = mp.Pool(mp.cpu_count(),maxtasksperchild=1)
        self.toprocess = runs_to_process_queue
        self.processed_runs_q = processed_runs_queue
        self.sequence_handler = sequence_handler_object
        self.stop_process_event = None
        self.runsProcessThread = Thread
        self.runChainProcessFunction = None

    def processRuns(self, functoapply):

        while True:

            r_copy = self.toprocess.get()
            results_folder = self.options['result_folder']
            run_status = None
            for k in r_copy:
                run_status = r_copy[k]['status']
            print k, run_status
            sequence = self.sequence_handler.getSequenceForName(run_status)
            #print 'sequence ', sequence
            runChainArgs = {'rundetails':r_copy, 'commands': sequence, 'result_folder': results_folder}
            result = self.pool.apply_async(functoapply, (runChainArgs, ))
            self.processed_runs_q.put({k: result})
            self.toprocess.task_done() # remove the run from the toprocess queue

            if self.stop_process_event.is_set() and self.toprocess.empty():
                self.pool.close()
                self.pool.join()
                print 'Exiting run proccess mngr'
                break

    def formatRunResult(self, rnum, result_output):
        result = {}
        result[rnum] = result_output
        return

    def run(self):
        self.processRuns(self.runChainProcessFunction)

    def runForestRun(self):
        ''' For the lolz ! :) '''
        self.start()

def processSingleRunChain(args=None):

    '''
    Function to run single runChain object
    Setup the run chain object with the args
    '''
    e_handler = EventsHandler()
    runchain = Chain(e_handler)
    e_handler.addObserver(runchain)

    run_num = None
    for k in args['rundetails'].keys():
        run_num = k

    print 'process is ', mp.current_process().name , ' for run ', run_num

    runchain.commands = args['commands']
    #print 'commands', runchain.commands
    rfolder = args['result_folder']

    initialEvent = SimpleEvent('init', True, {'run':run_num, 'result_folder':rfolder})
    runchain.startChainWithEvent(initialEvent)

    return runchain.getResult()


if __name__ == "__main__":

    print os.getpid()

    #os.environ['LD_LIBRARY_PATH'] = '/opt/offline/slc6_amd64_gcc491/cms/cmssw/CMSSW_7_3_6/external/slc6_amd64_gcc491/bin/root'  # important

    optionsObject = None
    with open('resources/options_object.txt', 'r') as optobj: optionsObject = json.loads(optobj.read())
    os.environ['LD_LIBRARY_PATH'] = optionsObject['paths']['cms_online_nt_machine']['root_path']
    print os.environ['LD_LIBRARY_PATH']
    #with open('resources/passwd') as pfile: passwd = pfile.readline()
    p = ''

    connections_dict = {}
    connections_dict.update({'webserver':optionsObject['webserver_remote']})
    connections_dict.update({'lxplus':optionsObject['lxplus_archive_remote']})
    connections_dict['webserver']['ssh_credentials']['password'] = p
    connections_dict['lxplus']['ssh_credentials']['password'] = p

    print connections_dict

    sshTransport = SSHTransportService(connections_dict)
    db_obj = DBService(dbType='oracle+cx_oracle://',host= 'localhost',port= '1521',user= 'CMS_COND_RPC_NOISE',password= 'j6XFEznqH9f92WUf',schema= 'CMS_RPC_COND',dbName= 'cms_orcoff_prep')
    

    #print alist
    ssh_one = sshTransport.connections_dict['webserver']
    ssh_two = sshTransport.connections_dict['lxplus']
    print ssh_one
    print ssh_two

    runsToProcessQueue = Queue.Queue()
    processedRunsQueue = Queue.Queue()
    sequence_handler = CommandSequenceHandler('resources/SequenceDictionaries.json', 'resources/options_object.txt')
    rpmngr = RunProcessPool(runsToProcessQueue, processedRunsQueue, sequence_handler, {'result_folder':'/rpctdata/CAF/'})

    rlistMngr = RunlistManager('resources/runlist.json')
    rlistMngr.toProcessQueue = runsToProcessQueue

    stop = mp.Event()
    #stop.set()
    rpmngr.stop_process_event = stop
    rpmngr.runChainProcessFunction = processSingleRunChain


    runsToProcessQueue.put({'269615':rlistMngr.runlist['269615']})
    

    rpmngr.start()



