'''
Its a class to connect CERN runregistry api
'''

from thirdPartyAPI.rrapi import RRApi, RRApiError
import socks
import time

class RRService:

    def __init__(self, RRURL = 'http://runregistry.web.cern.ch/runregistry', use_proxy = False):
        self.use_proxy = use_proxy
        self.rr_obj = RRApi(RRURL, debug = False, use_proxy=self.use_proxy)

    def __del__(self):
        pass

    def getRunRange(self, runType=None, lastRun=None, runDuration=None):
        '''
        :param runType: Collisions, Cosmics & Commissioning or combinations
        :param lastRun: start run number
        :param runDuration:  duration in seconds
        :return: list of runs
        '''
        runlist = {}

        try:
            #print "Accessing run registry.......\n"
            an_array = self.rr_obj.data('GLOBAL', 'runsummary', 'json', ['number', 'duration', 'runClassName'],
                                        {'datasetExists': '= true', 'number': '> '+ str(lastRun), 'duration': '> '+str(runDuration),
                                         'rpcPresent' : 'true', 'runClassName': runType}, order=['number asc'])
            if an_array:
                for description in an_array:
                    rundescription = {}
                    rundescription['Type'] = str(description['runClassName'])
                    rundescription['duration'] = str(description['duration'])
                    rundescription['status'] = 'new'
                    runlist[str(description['number'])] = rundescription

        except RRApiError, e:
            print e.message

        return runlist

    def getRunsLumiSectionsInfo(self, goodLSconditions = 'rpcReady', lastRun=None):

        result = {}
        #year = time.strftime("%y")
        array = self.rr_obj.data( 'GLOBAL', 'runlumis', 'json', ['runNumber','sectionFrom','sectionTo', goodLSconditions],{'runNumber': '> '+str(lastRun) } )
        for r in array:
            rnum = r['runNumber']
            if not str(rnum) in result: result[str(rnum)] = {'good':0, 'all':0}
            result[str(rnum)]['all'] = r['sectionTo']
            if r['rpcReady'] == True: result[str(rnum)]['good'] += r['sectionTo'] - r['sectionFrom'] + 1

        return result

    def getRunRangeWithLumiInfo(self, runType=None, lastRun=None, runDuration=None):
        lr = lastRun
        result = self.getRunRange(runType, lr, runDuration)
        lInfo = self.getRunsLumiSectionsInfo(lastRun=lr)
        for run in result.keys():
            result[run]['good'] = lInfo[run]['good']
            result[run]['all'] = lInfo[run]['all']
        return result

    '''
    methods with predefined run settings, most used cases
    '''

    def getRunlistForLongRuns(self, lastRun=None):
        year = time.strftime("%y")
        runlist = self.getRunRange('Collisions'+year+' OR Cosmics'+year+' OR Commissioning'+year, lastRun, 600)
        return runlist

if __name__ == "__main__":

    socks.setdefaultproxy(socks.PROXY_TYPE_SOCKS5, '127.0.0.1', 1080)
    rr_obj = RRService(use_proxy=True)
    #rlist = rr_obj.getRunRange('Collisions15', '257000', '600')
    #print rlist
    #rlist = rr_obj.getRunlistForLongRuns(258000)
    #print rlist
    #lumis = rr_obj.getRunsLumiSectionsInfo(lastRun=257000)
    #print lumis
    res = rr_obj.getRunRangeWithLumiInfo('Collisions15', '257000', '600')
    print res