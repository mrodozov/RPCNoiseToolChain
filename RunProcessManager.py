__author__ = 'rodozov'

from CommandClasses import *
from Chain import Chain
from RPCMap import RPCMap
from Event import SimpleEvent
from RunlistManager import RunlistManager
import multiprocessing as mp
import Queue
import time
import json
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
        self.loopTime = None


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
            r = result.get()
            for rec in r['results']:
                if r['results'][rec][0] == 'Failed':
                    print rec
                    print r['warnings'][rec]
                    print r['logs'][rec]

            #format result, put it in the result queue. or just put it on the result queue as it is with its runnum

            self.toprocess.task_done() # remove the run from the toprocess queue

    def loopUntilSignalIsSentOrTimeElapsed(self, seconds=10, signal=None):
        secs = 0
        secs = seconds
        while True:
            print 'Stop after: ', secs
            secs = secs - 1
            time.sleep(1)
            if secs == 0:
                signal.set()
                print 'Breaking on time elapsed !'
                break
            if signal.is_set():
                print 'Breaking on signal to stop !'
                break

    def run(self):

        self.loopUntilSignalIsSentOrTimeElapsed(self.loopTime, self.stop_process_event)


    def runForestRun(self):
        '''
        For the lolz ! :)
        '''
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
    rpmngr = RunProcessPool(runsToProcessQueue,processedRunsQueue,sequence_handler,{'result_folder':'results/'})

    rlistMngr = RunlistManager('resources/runlist.json')
    rlistMngr.toProcessQueue = runsToProcessQueue
    arun = rlistMngr.runlist['220796']
    print arun
    runsToProcessQueue.put({'220796':arun})
    #rpmngr.processRuns(processSingleRunChain)

    rpmngrFirst = RunProcessPool()
    rpmngrSecond = RunProcessPool()
    stop_event = mp.Event()
    rpmngrFirst.loopTime = 10
    rpmngrFirst.stop_process_event = stop_event
    rpmngrSecond.loopTime = 5
    rpmngrSecond.stop_process_event = stop_event
    rpmngrFirst.runForestRun()
    rpmngrSecond.runForestRun()

    print 'run after start of both'

    rpmngrFirst.join()
    rpmngrSecond.join()



'''

import multiprocessing
import time

def releaseTheCracken(e, delay):
    i = delay
    while True:
        print 'Waiting for: ', i
        time.sleep(1)
        i = i - 1
        if i == 0:
            e.set()
            break

def wait_for_event(e):
    """Wait for the event to be set before doing anything"""
    print 'wait_for_event: starting'
    e.wait()
    print 'wait_for_event: e.is_set()->', e.is_set()

def wait_for_event_timeout(e, t):
    """Wait t seconds and then timeout"""
    print 'wait_for_event_timeout: starting'
    e.wait(t)
    print 'wait_for_event_timeout: e.is_set()->', e.is_set()


if __name__ == '__main__':
    e = multiprocessing.Event()
    w1 = threading.Thread(name='block',
                                 target=wait_for_event,
                                 args=(e,))
    w1.start()

    w2 = threading.Thread(name='non-block',
                                 target=wait_for_event_timeout,
                                 args=(e, 2))
    w2.start()

    w3 = threading.Thread(name='cracken',
                                 target=releaseTheCracken,
                                 args=(e,10))


    print 'main: waiting before calling Event.set()'
    time.sleep(3)
    #w3.start()
    print 'main: event is set'

'''




