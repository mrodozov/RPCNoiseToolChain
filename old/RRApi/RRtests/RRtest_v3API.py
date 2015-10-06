
#!/usr/bin/python

import re,json,sys
from rrapi import RRApi, RRApiError

URL = "http://localhost:22223/runregistry/"

def connectRR_v3_API(runType, startRun, endRun, runDuration):
    runInfo = []
    api = RRApi(URL, debug = True)    
    an_array = api.data('GLOBAL', 'runsummary', 'json', ['number'], {'datasetExists': '= true', 'number': '>= '+ startRun +' AND <= ' + endRun,'duration': '> '+runDuration,'rpcPresent' : 'true','runClassName' : runType})
    for i in an_array:
        num = i['number']
        runInfo.append(num)

    runInfo.sort()
    return runInfo

if (len(sys.argv) < 3):
    print 'arguments are less than 3'
    exit(1)


def inspect_run_goodnes(input_array,percent_good_LS):

    if (len(input_array) == 0):
        print 'The RunList is empty !'
        sys.exit()

    good_runs = []
    bad_runs = []
    return_list = []
    apiObject = RRApi(URL, debug = True)
    firstRun = input_array[0]
    lastRun = input_array[len(input_array)-1]
    #    print 'first run is ', firstRun
    #    print 'last run is ', lastRun
    filtered_array = apiObject.data('GLOBAL','runlumis','json',['runNumber','sectionFrom','sectionTo','rpcReady'],{'runNumber': '>= '+ str(firstRun) +' AND <= ' + str(lastRun)})
    current_runNum = 0
    iterator = 0
    good_LS = 0
    bad_LS = 0
    
    #current_runNum = filtered_array[0]['runNumber'] #initial run number is the beginning of the list

    #print current_runNum # checking the runnum
    
    for i in input_array:
        if i in input_array:
            #print 'the run is ',i
            for j in filtered_array:
                if int(j['runNumber']) == i:
                    #print 'values are',j
                    if j['rpcReady'] == True:
                        good_LS += int(j['sectionTo']) - int(j['sectionFrom']) + 1
                        #print i,'  ',good_LS,'   ',j['rpcReady']
                    else:
                        bad_LS += int(j['sectionTo']) - int(j['sectionFrom']) + 1
                        #print  i,'  ',bad_LS,'   ',j['rpcReady']
            if bad_LS == 0:
                bad_LS = 1
            if float(good_LS/bad_LS)*100 >= percent_good_LS:
                good_runs.append(i)
                #print 'GOOD ',i, ' ' , good_LS, ' ' , bad_LS
            else:
                bad_runs.append(i)
                #print 'BAD ',i, ' ' , good_LS, ' ' , bad_LS

            good_LS = 0
            bad_LS = 0



            #print 'Good runs: ',good_runs
            #print 'Bad runs: ',bad_runs
            
    return_list.append(good_runs)
    return_list.append(bad_runs)
            
    return return_list

                                                                            
arr = connectRR_v3_API(sys.argv[1],sys.argv[2],sys.argv[3],'1200')

result_ar = inspect_run_goodnes(arr,99)

for i in result_ar:
    print 'Next '
    for j in i:
        print j
    
