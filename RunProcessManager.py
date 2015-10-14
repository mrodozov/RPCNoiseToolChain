__author__ = 'rodozov'

from CommandClasses import *
from Chain import Chain
from Event import SimpleEvent
from RunlistManager import RunlistManager
import multiprocessing as mp
import Queue
import time
from threading import Thread

'''


'''

class RunProcessPool(Thread):

    def __init__(self, runs_to_process_queue=None, processed_runs_queue=None, sequence_handler_object=None, options=None):

        super(RunProcessPool, self).__init__()
        self.options = options
        self.pool = mp.Pool()
        self.toprocess = runs_to_process_queue
        self.processed_runs_q = processed_runs_queue
        self.sequence_handler = sequence_handler_object
        self.stop_process_event = None
        self.runsProcessThread = Thread
        self.runChainProcessFunction = None

    def processRuns(self, functoapply):

        while True:

            run = self.toprocess.get()
            results_folder = self.options['result_folder']
            run_status = None
            for k in run:
                run_status = run[k]['status']
            print run_status
            sequence = self.sequence_handler.getSequenceForName(run_status)
            runChainArgs = {'rundetails':run, 'commands':sequence, 'result_folder': results_folder}
            result = self.pool.apply_async(functoapply, (runChainArgs, ))

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
    rfolder = args['result_folder']

    initialEvent = SimpleEvent('init', True, {'run':run_num, 'result_folder':rfolder})
    runchain.startChainWithEvent(initialEvent)

    return runchain.getResult()


if __name__ == "__main__":

    os.environ['LD_LIBRARY_PATH'] = '/home/rodozov/Programs/ROOT/INSTALL/lib/root'  # important
    runsToProcessQueue = Queue.Queue()
    processedRunsQueue = Queue.Queue()
    sequence_handler = CommandSequenceHandler('resources/SequenceDictionaries.json', 'resources/options_object.txt')
    rpmngr = RunProcessPool(runsToProcessQueue, processedRunsQueue, sequence_handler, {'result_folder':'results/'})

    rlistMngr = RunlistManager('resources/runlist.json')
    rlistMngr.toProcessQueue = runsToProcessQueue
    arun = rlistMngr.runlist['220796']
    print arun
    stop = mp.Event()
    stop.set()
    rpmngr.stop_process_event = stop
    runsToProcessQueue.put({'220796':arun})
    rpmngr.processRuns(processSingleRunChain)





