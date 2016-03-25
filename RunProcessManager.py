__author__ = 'rodozov'

from CommandClasses import *
from Chain import Chain
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
        self.pool = ThreadPool(mp.cpu_count())
        self.toprocess = runs_to_process_queue
        self.processed_runs_q = processed_runs_queue
        self.sequence_handler = sequence_handler_object
        self.stop_process_event = None
        self.runsProcessThread = Thread
        self.runChainProcessFunction = None

    def processRuns(self, functoapply):

        while True:

            r_copy = self.toprocess.get()
            #r_copy = copy.deepcopy(run)
            results_folder = self.options['result_folder']
            run_status = None
            for k in r_copy:
                run_status = r_copy[k]['status']
            print k, run_status
            #print 'r copy', r_copy
            sequence = self.sequence_handler.getSequenceForName(run_status)
            print 'sequence ', sequence
            runChainArgs = {'rundetails':r_copy, 'commands': sequence, 'result_folder': results_folder}
            result = self.pool.apply_async(self.runChainProcessFunction, (runChainArgs, ))
            #r = result.get()
            #r_copy['results'] = {'overall':['result1,result2']}
            #r_copy[k]['status'] = 'finished'
            #result = r_copy
            self.processed_runs_q.put({k:result})
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

    runchain = Chain()
    run_num = None
    for k in args['rundetails'].keys():
        run_num = k

    print 'process is ', mp.current_process().name , ' for run ', run_num

    runchain.commands = args['commands']
    print 'commands', runchain.commands
    rfolder = args['result_folder']

    initialEvent = SimpleEvent('init', True, {'run':run_num, 'result_folder':rfolder})
    runchain.startChainWithEvent(initialEvent)

    return runchain.getResult()


if __name__ == "__main__":

    os.environ['LD_LIBRARY_PATH'] = '/home/rodozov/Programs/ROOT/INSTALL/lib/root'  # important
    optionsObject = None
    with open('resources/options_object.txt', 'r') as optobj:
        optionsObject = json.loads(optobj.read())

    p = 'BAKsho__4321'

    connections_dict = {}
    connections_dict.update({'webserver':optionsObject['webserver_remote']})
    connections_dict.update({'lxplus':optionsObject['lxplus_archive_remote']})
    connections_dict['webserver']['ssh_credentials']['password'] = p
    connections_dict['lxplus']['ssh_credentials']['password'] = p

    print connections_dict

    sshTransport = SSHTransportService(connections_dict)
    db_obj = DBService('oracle://','localhost','1521','rodozov','tralala','','RPC')

    #print alist
    ssh_one = sshTransport.connections_dict['webserver']
    ssh_two = sshTransport.connections_dict['lxplus']
    print ssh_one
    print ssh_two

    runsToProcessQueue = Queue.Queue()
    processedRunsQueue = Queue.Queue()
    sequence_handler = CommandSequenceHandler('resources/SequenceDictionaries.json', 'resources/options_object.txt')
    rpmngr = RunProcessPool(runsToProcessQueue, processedRunsQueue, sequence_handler, {'result_folder':'results/'})

    rlistMngr = RunlistManager('resources/runlist.json')
    rlistMngr.toProcessQueue = runsToProcessQueue

    stop = mp.Event()
    stop.set()
    rpmngr.stop_process_event = stop
    rpmngr.runChainProcessFunction = processSingleRunChain

    runsToProcessQueue.put({'263743':rlistMngr.runlist['263743']})
    runsToProcessQueue.put({'263744':rlistMngr.runlist['263744']})
    runsToProcessQueue.put({'263745':rlistMngr.runlist['263745']})
    runsToProcessQueue.put({'263752':rlistMngr.runlist['263752']})
    runsToProcessQueue.put({'263757':rlistMngr.runlist['263757']})

    rpmngr.start()



