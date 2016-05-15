'''
Its a class to connect CERN runregistry api
'''

from thirdPartyAPI.rrapi import RRApi
import socks
import time
import datetime

#TODO - adjust to the new RR API https://twiki.cern.ch/twiki/bin/viewauth/CMS/DqmRrApi

class RRService:

    def __init__(self, RRURL = 'http://runregistry.web.cern.ch/runregistry/', debug = False, use_proxy = False):
        self.use_proxy = use_proxy
        self.rr_obj = RRApi(RRURL, debug = debug , use_proxy=self.use_proxy)
        #self.rr_obj = RRApi(RRURL, debug = debug)

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
        an_array = []

        try:
            #print "Accessing run registry.......\n"
            an_array = self.rr_obj.data('GLOBAL', 'runsummary', 'json', ['number', 'duration', 'runClassName'],
                                        {'datasetExists': '= true', 'number': '> '+ str(lastRun), 'duration': '> '+str(runDuration),
                                         'rpcPresent' : 'true', 'runClassName': runType}, order=['number asc'])
        except Exception, e:
            print e.message, ' from getRunRange at ', datetime.datetime.now().replace(microsecond=0)
            raise

        for description in an_array:
            rundescription = {}
            rundescription['Type'] = str(description['runClassName'])
            rundescription['duration'] = str(description['duration'])
            rundescription['status'] = 'new'
            runlist[str(description['number'])] = rundescription

        return runlist

    def getRunsLumiSectionsInfo(self, goodLSconditions = 'rpcReady', lastRun=None):

        result = {}
        #year = time.strftime("%y")
        try:
            array = self.rr_obj.data( 'GLOBAL', 'runlumis', 'json', ['runNumber','sectionFrom','sectionTo', goodLSconditions],{'runNumber': '> '+str(lastRun)})
        except Exception, e:
            print e.message, ' from getRunsLumiSectionsInfo at ', datetime.datetime.now().replace(microsecond=0)
            raise

        for r in array:
            rnum = r['runNumber']
            if not str(rnum) in result: result[str(rnum)] = {'lumisections': {'good':0, 'all': 0}}
            result[str(rnum)]['lumisections']['all'] = r['sectionTo']
            if r['rpcReady'] == True: result[str(rnum)]['lumisections']['good'] += r['sectionTo'] - r['sectionFrom'] + 1

        return result

    def getRunRangeWithLumiInfo(self, runType=None, lastRun=None, runDuration=None):
        lr = lastRun
        result = {}
        try:
            result = self.getRunRange(runType, lr, runDuration)
            lInfo = self.getRunsLumiSectionsInfo(lastRun=lr)
            for run in result.keys():
                result[run]['lumisections'] = lInfo[run]['lumisections']
                result[run]['lumisections']['all'] = lInfo[run]['lumisections']['all']
        except Exception, e:
            print e.message, ' from getRunRangeWithLumiInfo '
            raise
        return result

    def getRangeWithStartAndDuration(runType, lastRun, runDuration, RRUrl):

        runInfo = []
        startRun = 0
        endRun = 0
        print "Accessing run registry.......\n"
        api = RRApi(RRUrl, debug = True)
        an_array = api.data('GLOBAL', 'runsummary', 'json', ['number','startTime', 'duration'], {'datasetExists': '= true', 'number': '> '+ str(lastRun),
        'duration': '> '+str(runDuration), 'rpcPresent' : 'true' , 'runClassName': runType})
        for k in an_array:
            print k['number'], k['startTime'], k['duration']


    '''
    methods with predefined run settings, most used cases
    '''

    def getRunlistForLongRuns(self, lastRun=None):
        year = time.strftime("%y")
        #year = '15'
        runlist = self.getRunRange('Collisions'+year+' OR Cosmics'+year+' OR Commissioning'+year, lastRun, 600)
        return runlist


if __name__ == "__main__":

    #socks.setdefaultproxy(socks.PROXY_TYPE_SOCKS5, '127.0.0.1', 1080)
    rr_obj = RRService('http://localhost:22223/runregistry/',False,False)
    #rr_obj.getRangeWithStartAndDuration('Cosmics11 OR Cosmics12 or Cosmics13 or Cosmics14 or Cosmics15', 160077, 600, 'http://runregistry.web.cern.ch/runregistry/')
    result = rr_obj.getRunRangeWithLumiInfo('Cosmics16 OR Commissioning16 OR Collisions16', '268456 and < 269556', 300);
    rlist = result.keys()
    rlist.sort()
    print rlist
    print len(rlist)
