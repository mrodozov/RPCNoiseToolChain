import re,json,sys,getopt
from rrapi import RRApi, RRApiError

    
def getRunDurationAsLumiSections(runNumber):

    URL = "http://localhost:22223/runregistry/"
    api = RRApi(URL, debug = True)
    #time_array = api.data('GLOBAL','runsummary','json',['duration'],{'number': '> '+str(runNumber - 1)+' AND '+'< '+str(runNumber + 1)})
    time_array = api.data('GLOBAL','runsummary','json',['duration'],{'number': '= '+str(runNumber)})
    seconds = 0

    for i in time_array:
        seconds = i['duration']
    LStime = 23.31
    #print seconds
    print int(seconds/LStime),' lumi sections for run ',sys.argv[1]
    return int(seconds/LStime)

print len(sys.argv),' arguments'

if(len(sys.argv) < 3):
    print 'Too few arguments, please pass the runnumber and the file where the result should be written, exiting'
    exit(1)

else:

    Lsections = str(getRunDurationAsLumiSections(int(sys.argv[1])))
    fileToWrite = sys.argv[2]
    File = open(fileToWrite,'w')
    File.write(Lsections+'\n')

