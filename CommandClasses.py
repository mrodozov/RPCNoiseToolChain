__author__ = 'rodozov'

'''
Abstract command class and different implementations, representing
independent implementations of command objects.
'''

import subprocess
import os
import time
import re
import json
import paramiko
from DBService import DBService
from RPCMap import RPCMap
from Event import SimpleEvent
from Chain import Chain

# TODO - See if there is need to format HTML for any reason,
# TODO - Wrap the processTask with exception, to continue after the execution even if command crashes. for example db upload crash should not prevent creating files and moving them on another location
#

class Command(object):

    def __init__(self, name=None, args=None):
        self.stout = None
        self.sterr = None
        self.name = name
        self.log = {}
        self.results = {}
        self.warnings = []
        self.args = args # static options, from a file
        self.options = {} # dynamic options, passed from
        self.exitcode = None

    def __del__(self):
        pass

    def getArgumentsFromObject(self, argsObject):
        self.args = argsObject[self.name]

    def checkRequirements(self):
        # generic method, always check self.args and self.options - the static and the dynamic resources required. just check if not empty
        retval = False
        if self.args and self.options and self.options != 'Failed':
            retval = True
        else:
            self.results = 'Failed'
            self.log = 'Failed'
            self.warnings.append('Requirements missing')
            retval = False
        return retval

    def checkResult(self):
        # generic, checks if the result (the event message) no empty
        if self.results:
            return True
        else:
            self.results = 'Failed'
            self.warnings.append('Result is empty')
            return False

    def processTask(self):
        raise NotImplementedError

    def execute(self):

        if not self.checkRequirements():
            return False
        if not self.processTask():
            return False
        if not self.checkResult():
            return False
        return True

    def getStdErr(self):
        raise NotImplementedError

    def getLog(self):
        raise NotImplementedError

class GetListOfFiles(Command):

    def getTowerNameFromFileName(self, filename):
        parts = filename.split('_')
        return parts[1] + '_' + parts[2]

    def processTask(self):
        complete = False
        files_per_tower_more_than_one = False
        missing_towers_files = []
        total_files_n = 0
        filespath = self.args['filesfolder']
        runnum = self.options['run']
        rfolder = self.options['result_folder']
        towers = self.args['towers_list']
        # print runnum
        files = [f for f in os.listdir(filespath) if f.endswith('.root') and f.find(runnum) is not -1]
        # print towers
        shortlist = []
        total_files_n = len(files)
        if len(files) > 18:
            files_per_tower_more_than_one = True
            towerMap = {}
            for t in towers:
                towerMap[t] = {'file': None, 'size': 0}
            for f in files:
                tf = self.getTowerNameFromFileName(f)
                # print tf
                statinfo = os.stat(filespath + '/' + f)
                fsize = statinfo.st_size
                # print f, fsize
                if towerMap[tf]['file'] is None or fsize > towerMap[tf]['size']:
                    towerMap[tf] = {'file': f, 'size': fsize}
            for k in towerMap.keys():
                shortlist.append(towerMap[k]['file'])
            #print towerMap
            files = shortlist
        # print files
        self.results['rootfiles'] = [filespath + f for f in files]
        self.results['run'] = runnum
        self.results['result_folder'] = 'results'
        #format the output
        towerslist = self.args['towers_list']
        for t in towerslist:
            match = False
            for f in files:
                if f.find(t) is not -1:
                    match = True
                    break
            if not match:
                missing_towers_files.append(t)
        #print filespath
        if missing_towers_files:
            self.log['missing_files'] = missing_towers_files
            self.warnings.append('missing towers files')
        if files_per_tower_more_than_one:
            self.log['files_for_run'] = total_files_n
            self.warnings.append('multiple files per tower')
        if len(files) is 0:
            self.log['missing_files'] = 'no files for this run, skip'
            self.results = 'Failed'
            self.warnings.append('root files missing, check the source')
        if len(files) > 0:
            self.log['files_for_run'] = total_files_n
            complete = True
        #print self.results['rootfiles']
        return complete


class CheckIfFilesAreCorrupted(Command):
    def processTask(self):

        complete = False
        goodresult = []
        corrupted_files = {}
        #if self.args is not None:
        #    self.args = static_opts[self.name]['args']
        #    self.options = static_opts[static_opts[self.name]['source']]['results']
        executable = self.args
        for file in self.options['rootfiles']:
            #print file
            childp = subprocess.Popen(executable + ' ' + file, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, close_fds=True)
            self.stout, self.sterr = childp.communicate()
            self.exitcode = childp.returncode
            overall = {'sterr': self.sterr, 'stout': self.stout, 'exitcode': self.exitcode}

            if self.exitcode == 0 and self.sterr is None:
                complete = True
                goodresult.append(file)
            else:
                corrupted_files[file] = overall

        self.log = {'good_files':goodresult, 'corrupted_files': corrupted_files}
        if corrupted_files: self.warnings.append('corrupted files found')
        self.results['rootfiles'] = goodresult

        self.results['run'] = self.options['run']
        self.results['result_folder'] = self.options['result_folder']

        #print self.results['result_folder']

        if not complete:
            print complete
            self.results = 'Failed'
            self.warnings.append('all files corrupted')
            #print 'exit code', self.exitcode
        self.results = self.options # ??????????? #TODO - fix this lousy bug !

        return complete


class NoiseToolMainExe(Command):
    '''
    Noise tool main executable command
    If the file check is passed, it process the files (one by one ? really ?)
    '''

    def processTask(self):

        complete = False
        #print self.options
        results = {'masked':[],'dead':[],'tomask':[],'tounmask':[],'rootfiles':[],'totalroot':'','run':self.options['run']}
        filesToProcess = [f for f in self.options['rootfiles']]
        #print filesToProcess # for debug


        '''
        create the result dir if doesn't exist yet

        '''

        if not self.options['result_folder'].endswith('/'): self.options['result_folder'] += '/'
        res_folder = self.options['result_folder'] + 'run' + self.options['run'] + '/'
        #print res_folder
        #print 'nexe results in ', self.options['result_folder']
        if not os.path.isdir(res_folder):
            try:
                os.mkdir(res_folder)
            except Exception as e:
                print e.message
        executable = self.args[0]
        arguments = self.args[1] + ' ' + self.args[2] + ' ' + res_folder
        '''
        for f in filesToProcess:
            # print executable, f, arguments
            childp = subprocess.Popen(executable + ' ' + f + ' ' + arguments, shell=True, stdout=subprocess.PIPE,
                                      stderr=subprocess.STDOUT, close_fds=True)
            current_stdout, current_stderr = childp.communicate()
            current_excode = childp.returncode
            current_stdout = ''
            current_excode = 0
            current_stderr = None
            #if at least one file has finished
            if current_excode == 0 and current_stderr is None:
                complete = True
            self.log[f] = {'complete': complete,'err': current_stderr,'out':current_stdout,'exitcode':current_excode}
                # so far, and thanks for all the fish
        '''
        complete = True
        if not complete:
            results = 'Failed'
            self.warnings.append('no properly processed files')
        else:
            # list the results dir and collect the results
            results['masked'] = [f for f in os.listdir(res_folder) if f.find('Masked_') is not -1 and f.find('All') is -1 ]
            results['dead'] = [f for f in os.listdir(res_folder) if f.find('Dead') is not -1 and f.find('All') is -1]
            results['tounmask'] = [f for f in os.listdir(res_folder) if f.find('ToUnmask') is not -1 and f.find('All') is -1]
            results['tomask'] = [f for f in os.listdir(res_folder) if f.find('ToMask') is not -1 and f.find('All') is -1]
            results['rootfiles'] = [f for f in os.listdir(res_folder) if f.find('Noise_') is not -1 and f.endswith('.root')]
            childp = subprocess.Popen('hadd -f ' + res_folder+'total.root ' + res_folder + 'Noise_*', shell=True, stdout=subprocess.PIPE,
                                      stderr=subprocess.STDOUT, close_fds=True)
            current_stdout, current_stderr = childp.communicate()
            urrent_excode = childp.returncode
            if  os.path.isfile(res_folder + 'total.root'):
                results['totalroot'] = res_folder+'total.root'
            else:
                results = 'Failed'

        self.results = results
        self.results['result_folder'] = self.options['result_folder']
        # TODO - improve the options again, this is redundant here.

        return complete


class DBInputPrepare(Command):
    '''

    '''

    def mergeInputFilesByName(self, fileDir, outputFileName, substringToSearch, orderingList, exploder):

        listOfFiles = [f for f in os.listdir(fileDir) if
                       f.endswith('.txt') and f.find(substringToSearch) is not -1 and f.find('All') is -1]
        # print listOfFiles
        # check if files exists
        if not listOfFiles:
            return False
        with open(fileDir + outputFileName, 'w') as outputFile:
            if listOfFiles:
                for f in listOfFiles:
                    with open(fileDir + f, 'r') as data_file:
                        lines = data_file.readlines()
                        for l in lines:
                            listElements = l.split(exploder)
                            if len(listElements) > 2:
                                # print l, f
                                argstowrite = [listElements[a] for a in orderingList]
                                outputFile.write(' '.join(argstowrite) + '\n')
        return True

    def processTask(self):
        complete = False
        results = {}

        executable = self.args[0]
        resourcesDir = self.args[1]
        resultsDir = self.options['result_folder']
        rootFile = self.options['totalroot']
        rnum = self.options['run']
        resultsDir += 'run' + rnum + '/'
        fileToSearch = self.args[2:6]
        areaFile = resourcesDir + self.args[6]
        rawids = resourcesDir + self.args[7]
        inputrolls = resourcesDir + self.args[8]
        print resultsDir
        for f in fileToSearch:
            listOfOrderedArgs = [6, 3]
            if f == 'ToMask':
                listOfOrderedArgs = [6, 3, 9, 14]
            if f == 'ToUnmask':
                listOfOrderedArgs = [6, 3, 15]
            self.mergeInputFilesByName(resultsDir, 'All' + f + '.txt', f, listOfOrderedArgs, ' ')

        file_to_check = rootFile + ' ' + resultsDir + 'AllMasked.txt ' + resultsDir + 'AllDead.txt ' + resultsDir + 'AllToMask.txt ' + resultsDir + 'AllToUnmask.txt ' + areaFile + ' ' + rawids + ' ' + inputrolls
        arguments = rootFile + ' ' + resultsDir + 'database_new.txt ' + resultsDir + 'database_full.txt ' + resultsDir + 'AllMasked.txt ' + resultsDir + 'AllDead.txt ' + resultsDir + 'AllToMask.txt ' + resultsDir + 'AllToUnmask.txt ' + areaFile + ' ' + rawids + ' ' + resultsDir + 'error_in_translation ' + inputrolls

        fnf_list = [fnf for fnf in file_to_check.split(' ') if not os.path.isfile(fnf)]
        if fnf_list:
            # file is missing, write log and abort
            self.log = 'Files missing: ' + ' '.join(fnf_list) + ', Abort'
            self.warnings.append('required file missing, requirements not completed')
            self.results = 'Failed'
        else:
            childp = subprocess.Popen(executable + ' ' + arguments, shell=True, stdout=subprocess.PIPE,
                                      stderr=subprocess.STDOUT, close_fds=True)
            current_stdout, current_stderr = childp.communicate()
            current_excode = childp.returncode
            if current_excode == 0:
                complete = True
                self.log = 'Completed'
                # print self.log
                self.results = {'strips_file': resultsDir + 'database_full.txt', 'rolls_file': resultsDir + 'database_new.txt','run':self.options['run'], 'result_folder':self.options['result_folder']}
                fileList = [self.results['strips_file'], self.results['rolls_file']]
                for finlist in fileList:
                    existingData = None
                    with open(finlist, 'r') as df:
                        existingData = df.read()
                    with open(finlist, 'w') as data_file:
                        timestamp = int(time.time())
                        #print self.options.keys()
                        data_file.write(self.options['run'] + ' ' + str(timestamp) + '\n')
                        data_file.write(existingData)

            #print current_stdout, current_stderr, current_excode

        #self.results['result_folder'] = self.options['result_folder']
        return complete

class DBFilesContentCheck(Command):
    # TODO - pass the patterns from the options, read them from a file maybe

    def contentCheck(self, file_content, file_type='rolls'):
        contentMeta = {'correct': True, 'errors': []}
        headerPattern = '^[0-9]+[ \t]+[0-9]+$'
        runDataPatternNon_Roll = """
        ^
        ([0-9]+[ \t]+){1}
        [a-zA-Z0-9_\+\-]+[ \t]+
        (-99[ \t]+){5}
        -99[ \t]*
        $
        """
        runDataPattern_Roll = """
        ^
        ([0-9]+[ \t]+){1}
        [a-zA-Z0-9_\+\-]+[ \t]+
        ([0-9]+[ \t]+){4}
        [0-9]+(\.*[0-9]+){0,1}([eE][+-]?\d+)?[ \t]+
        [0-9]+(\.*[0-9]+){0,1}([eE][+-]?\d+)?[ \t]*
        $
        """
        runDataPatternNon_Strips = """
        ^
        ([0-9]+[ \t]+){2}
        ([-]?\d+[ \t]+)+
        (0|-99)[ \t]*
        $
        """
        runDataPattern_Strips = """
        ^
        ([0-9]+[ \t]+){5}
        [0-9]+(\.*[0-9]+){0,1}([eE][+-]?\d+)?[ \t]*
        $
        """
        pattrn = runDataPattern_Roll
        pattrnNon = runDataPatternNon_Roll
        if file_type is 'strips':
            pattrn = runDataPattern_Strips
            pattrnNon = runDataPatternNon_Strips
        if not re.match(headerPattern, file_content[0]):
            contentMeta['correct'] = False
            contentMeta['errors'].append('File header (first line of the file) is incorrect')
        for run in file_content[1:]:
            if not re.match(pattrn, run, re.VERBOSE) or not re.match(pattrnNon, run, re.VERBOSE):
                # print run
                contentMeta['correct'] = False
                contentMeta['errors'].append('run data ' + run + ' is incorrect')
        return contentMeta

    def processTask(self):
        complete = True
        strips_file = self.options['strips_file']
        rolls_file = self.options['rolls_file']
        cont_files = [strips_file, rolls_file]
        checkResults = {}
        llog = {}
        self.results['filenames'] = {}

        for f in cont_files:
            with open(f, 'r') as data_file:
                lines = data_file.read().strip().split('\n')
                filename = 'strips'
                if f.find('_new.txt') is not -1:
                    filename = 'rolls'
                filecheck = self.contentCheck(lines, filename)
                # TODO - remove this, its for test only ! checkResults[f] = filecheck['correct']
                self.results['filenames'][filename] = f
                checkResults[f] = True
                llog[f] = filecheck['errors']
                if not filecheck['correct']:
                    complete = False

        self.results['filescheck'] = checkResults
        self.results['run'] = self.options['run']
        self.results['result_folder'] = self.options['result_folder']
        #self.log = filecheck['errors']
        return True

class DBDataUpload(Command):

    def processTask(self):
        complete = False
        # files from results, table names and schemas from options
        #dbService = DBService(dbType=self.args['dbType'], host=self.args['hostname'], port=self.args['port'],
        #                      user=self.args['username'], password=self.args['password'], schema=self.args['schema'],
        #                      dbName=self.args['dbName'])
        for rec in self.args['connectionDetails']:
            dataFile = ''.join([f for f in self.options['filescheck'] if f.find(rec['file']) is not -1])
            print dataFile
            #data = self.getDBDataFromFile(dataFile)
            #completed = dbService.insertToDB(data, rec['name'], rec['schm'], rec['argsList'])
            #catch the error, push it to the log
            completed = True # TODO - to remove
            self.results[dataFile] = completed
            self.log[dataFile] = completed
            complete = completed
            if not completed:
                self.results = 'Failed'
                self.warnings.append('file failed to be inserted')
                break
        self.results['run'] = self.options['run']
        return complete

    def getDBDataFromFile(self, fileName):

        dataList = []
        with open(fileName, 'r') as data_file:
            fileContent = data_file.readlines()
            runID = fileContent[0].split()[0]
            for line in fileContent[1:5]:
                listToIns = line.split()
                listToIns.insert(0, runID)
                dataList.append(listToIns)

        return dataList


class OutputFilesFormat(Command):
    '''
    format all outputs into single file. just compactify all the data into single json file, so it could be used later and to be the only remaining thing
    in the folder apart from the root files
    '''

    def processTask(self):
        complete = False
        #print static_opts
        rpcMapFile = self.args[0]
        rawmapfile = self.args[1]
        results_folder = self.options['result_folder']
        results_folder += 'run' + self.options['run'] + '/'
        rolls_json_file = results_folder + self.args[2]
        strips_json_file = results_folder + self.args[3]
        allToUnmaskFile = results_folder + self.args[4]
        allToMaskFile = results_folder + self.args[5]

        detailedFile = self.options['filenames']['strips']
        rollsFile = self.options['filenames']['rolls']
        self.log = {'strips_file': None, 'rolls_file': None}
        self.results['json_products'] = []
        rnum = self.options['run']

        # first print
        print detailedFile, rollsFile
        rollObject = {}
        stripsobject = {}
        with open(rollsFile, 'r') as rolls_data:
            masked = {}
            dead = {}
            toMask = {}
            toUnmask = {}
            rate = {}
            for rollrecord in rolls_data.read().splitlines():
                listofargs = [rec for rec in rollrecord.split()]
                # print listofargs
                # print rollrecord
                if len(listofargs) < 8 or int(listofargs[2]) == (-99): continue
                # print ' '.join(listofargs)
                rollname = listofargs[1]
                deadstrips = listofargs[2]
                maskedstrips = listofargs[3]
                tounmaskstrips = listofargs[4]
                tomaskstrips = listofargs[5]
                rateofroll = listofargs[6]
                rateofrollcmsquare = listofargs[7]
                if int(deadstrips) > 0:
                    dead[rollname] = deadstrips
                if int(maskedstrips) > 0:
                    masked[rollname] = maskedstrips
                if int(tomaskstrips) > 0:
                    toMask[rollname] = tomaskstrips
                if int(tounmaskstrips) > 0:
                    toUnmask[rollname] = tounmaskstrips
                rate[rollname] = {'rate': rateofroll, 'ratesquarecm': rateofrollcmsquare}
            rollObject['masked'] = masked
            rollObject['dead'] = dead
            rollObject['tomask'] = toMask
            rollObject['tounmask'] = toUnmask
            rollObject['rate'] = rate

        rpcMap = RPCMap(rpcMapFile, rawmapfile)
        allTUmap = {}
        allTMmap = {}
        allMaskedMap = {}
        allDeadMap = {}
        ratesDict = {}

        with open(allToUnmaskFile, 'r') as tounmaskfile:
            for lines in tounmaskfile.read().splitlines():
                line = [a for a in lines.split()]
                rname = line[0]
                channel_num = int(line[1])
                max_rate = float(line[2])
                chamberMap = rpcMap.chamberToRollMap[rname]

                for roll_record in chamberMap:
                    if channel_num in chamberMap[roll_record]['channels']:
                        channel_index = chamberMap[roll_record]['channels'].index(channel_num)
                        stripnum = chamberMap[roll_record]['strips'][channel_index]
                        if not roll_record in allTUmap:
                            allTUmap[roll_record] = {'channels': [], 'strips': [], 'rates': []}
                        allTUmap[roll_record]['channels'].append(channel_num)
                        allTUmap[roll_record]['strips'].append(stripnum)
                        allTUmap[roll_record]['rates'].append(max_rate)
                        break

        with open(allToMaskFile, 'r') as tomaksfile:
            for lines in tomaksfile.read().splitlines():
                line = [a for a in lines.split()]
                rname = line[0]
                channel_num = int(line[1])
                max_rate = float(line[3])
                time_as_noisy = float(line[2])
                chamberMap = rpcMap.chamberToRollMap[rname]

                for roll_record in chamberMap:
                    if channel_num in chamberMap[roll_record]['channels']:
                        channel_index = chamberMap[roll_record]['channels'].index(channel_num)
                        stripnum = chamberMap[roll_record]['strips'][channel_index]
                        if not roll_record in allTMmap:
                            allTMmap[roll_record] = {'channels': [], 'strips': [], 'times': [], 'rates': []}
                        allTMmap[roll_record]['channels'].append(channel_num)
                        allTMmap[roll_record]['strips'].append(stripnum)
                        allTMmap[roll_record]['times'].append(time_as_noisy)
                        allTMmap[roll_record]['rates'].append(max_rate)
                        break

        with open(detailedFile, 'r') as detailed_data:
            for lines in detailed_data.read().splitlines():
                roll_record = [a for a in lines.split()]
                if len(roll_record) < 6 or int(roll_record[3]) < 0: continue
                rawid = roll_record[0]
                roll_name = rpcMap.rawToRollMap[rawid]
                channel_num = roll_record[1]
                strip_num = roll_record[2]
                is_masked = roll_record[3]
                is_dead = roll_record[4]
                ratepcms = roll_record[5]
                if not roll_name in ratesDict:
                    ratesDict[roll_name] = {'channels': [], 'strips': [], 'rates': []}
                ratesDict[roll_name]['channels'].append(channel_num)
                ratesDict[roll_name]['strips'].append(strip_num)
                ratesDict[roll_name]['rates'].append(ratepcms)
                if int(is_masked) == 1:
                    if not roll_name in allMaskedMap:
                        allMaskedMap[roll_name] = {'channels': [], 'strips': []}
                    allMaskedMap[roll_name]['channels'].append(channel_num)
                    allMaskedMap[roll_name]['strips'].append(strip_num)
                if int(is_dead) == 1:
                    if not roll_name in allDeadMap:
                        allDeadMap[roll_name] = {'channels': [], 'strips': []}
                    allDeadMap[roll_name]['channels'].append(channel_num)
                    allDeadMap[roll_name]['strips'].append(strip_num)

        # merge into single object and write into a file
        detailedFileOutput = {'tomask': allMaskedMap, 'dead': allDeadMap, 'tomask': allTMmap, 'tounmask': allTUmap,
                              'rates': ratesDict}

        with open(rolls_json_file, 'w') as rolls_out_file:
            rolls_out_file.write(json.dumps(rollObject, indent=1, sort_keys=True))

        with open(strips_json_file, 'w') as strips_out_file:

            strips_out_file.write('{\n')
            deptone_keys = sorted(detailedFileOutput.keys())
            for deptone in sorted(detailedFileOutput.keys()):
                strips_out_file.write(json.dumps(deptone))
                strips_out_file.write(':{\n')
                keys = sorted(detailedFileOutput[deptone].keys())
                for key in keys:
                    strips_out_file.write('  ')
                    strips_out_file.write(json.dumps(key))
                    strips_out_file.write(':')
                    strips_out_file.write(json.dumps(detailedFileOutput[deptone][key]))
                    if key is not keys[-1]:
                        strips_out_file.write(',')
                    strips_out_file.write('\n')

                strips_out_file.write('}')
                if deptone is not deptone_keys[-1]:
                    strips_out_file.write(',')
                strips_out_file.write('\n')
            strips_out_file.write('}')

        # check if files has been created and not empty
        with open(strips_json_file, 'r') as strips_file_check:
            if json.loads(strips_file_check.read()) and os.stat(strips_json_file) > 0:
                self.log['strips_file'] = 'Completed!'
                self.results['json_products'].append(self.args[2])
        with open(rolls_json_file, 'r') as rolls_file_check:
            if json.loads(rolls_file_check.read()) and os.stat(rolls_json_file) > 0:
                self.log['rolls_file'] = 'Completed!'
                self.results['json_products'].append(self.args[3])
        complete = True
        self.results['results_folder'] = results_folder
        self.results['run'] = rnum
        for i in self.log:
            if self.log[i] is not 'Completed!':
                self.results = 'Failed'
                self.warnings.append('file(s) production failure')
                complete = False
        self.results['result_folder'] = self.options['result_folder']
        return complete

class WebResourcesFormat(Command):
    '''
    write local run html files and prepare some run markup, write it as result
    '''

    def processTask(self):
        pass

class CopyFilesOnRemoteLocation(Command):
    '''
    sends files to remote locations, using sftp, use file list and destination
    '''

    def __init__(self, name=None, args=None):
        Command.__init__(self, name, args)
        self.ssh_client = None
        self.sftp_client = None
        self.sftp = None
        self.connection_established = False
        self.open_connection()

    def __del__(self):
        self.close_connection()

    def open_connection(self):
        rval = False
        args_check = None
        if self.args and self.args['ssh_credentials']:
            for a in self.args['ssh_credentials'].keys():
                if not self.args['ssh_credentials'][a]:
                    print 'not ok, ', a
                    self.warnings.append('value for ' + a + ' is ' + self.args['ssh_credentials'][a])
                    self.results = 'Failed'
                    args_check = False
                    break
                else:
                    args_check = True
            if args_check:
                transfer_username = self.args['ssh_credentials']['username']
                transfer_password = self.args['ssh_credentials']['password']
                transfer_port = int(self.args['ssh_credentials']['port'])
                remote_host = self.args['ssh_credentials']['rhost']
                self.ssh_client = paramiko.SSHClient()
                self.ssh_client.load_host_keys(os.path.expanduser(os.path.join("~", ".ssh", "known_hosts")))
                try:
                    self.ssh_client.connect(remote_host, transfer_port, username=transfer_username, password=transfer_password)
                    self.sftp_client = self.ssh_client.open_sftp()
                    self.sftp = paramiko.SFTPClient.from_transport(self.ssh_client.get_transport())
                    rval = True
                    self.connection_established = True
                except Exception as exc:
                    print exc.message

        return rval

    def close_connection(self):
        if self.connection_established:
            self.ssh_client.close()
            self.sftp_client.close()
            self.sftp.close()
            self.connection_established = False

    def processTask(self):
        rval = False
        rnum = self.options['run']
        list_of_files = self.options['json_products']
        results_folder = self.options['result_folder'] + 'run' + rnum + '/'
        runfolder = 'run'+rnum
        destination = self.args['destination_root'] + runfolder
        print destination
        if not self.connection_established:

            try:
                self.open_connection()
            except Exception as exc:
                print exc.message
        #self.create_dir_on_remotehost(destination)
        self.results = {}
        self.results['files'] = {}
        if not self.connection_established:
            'Connection failed, stop'
            self.results = 'Failed'
            return False
        for f in list_of_files:
            sent = False
            try:
                self.sftp_client.put(results_folder + f, destination + '/' + f)
                sent = True
                rval = True # at least one file made it
            except IOError as exc:
                errout = "I/O error({0}): {1}".format(exc.errno, exc.strerror)
                self.warnings.append('file transfer failed for ' + f + 'with ' + errout)
                sent = False
                print errout
            self.results['files'][f] = sent
        if not rval:
            self.results='Failed'

        #self.results['result_folder'] = self.options['result_folder']
        return rval

    def create_dir_on_remotehost(self, dirname):
        try:
            self.sftp.chdir(dirname)
        except IOError:
            try:
                self.sftp.mkdir(dirname)
                self.sftp.chdir(dirname)
            except IOError as e:
                print e.message


class OldToNewDataConverter(Command):
    '''
    Converts old resources to new type
    '''

    def __init__(self):
        pass

class NewToOldDataConverter(Command):
    '''
    converts new resources to the old type
    '''

    def __init__(self):
        pass

class GarbageRemoval(Command):
    '''
    Compress or erase all outputs, to be processTaskd after all the outputs are finished
    '''

    def __init__(self):
        pass


class CommandSequenceHandler(object):
    '''
    class to represent sequence of command objects,
    that are given to the chain. every type of command correspond to key,
    where the key is written in a file and for each key given type object is created.
    Thus, using config files we create predefined sequences of commands
    suited for different cases. First and the most used should be 'new run ' case,
    but we could have cases where only certain commands has to be finished - like DB upload,
    ot backups. Such cases requires shorter sequences. Each command object have single
    key that defines its type, while the different configs (files) should define
    order and additional arguments. A config file describes the order, names and arguments
    for each command in given sequence, this class defines the types of command objects for each key.
    '''

    def __init__(self, sequence_file=None, stat_opts_file=None):
        self.sequence_file = sequence_file
        self.sequence_maps = None
        self.statoptsfile = stat_opts_file
        self.statoptsmap = None
        if self.sequence_file:
            try:
                self.setupMaps()
            except Exception as e:
                print e.message

    def setupMaps(self):
        with open(self.sequence_file, 'r') as sequences_file:
            self.sequence_maps = json.load(sequences_file)
        with open(self.statoptsfile, 'r') as statopfile:
            self.statoptsmap = json.load(statopfile)

    def getSequenceForName(self, name=None):
        command_sequence ={}

        try:
            for c in self.sequence_maps[name]:
                msg = c['starton']
                if not msg in command_sequence.keys(): command_sequence[msg] = []
                cmnd = self.getCommandObjectForKey(c['type'])
                cmnd.name = c['name']
                cmnd.args = self.statoptsmap[c['optionskey']]
                command_sequence[msg].append(cmnd)

            return command_sequence
        except Exception as e:
            print e.message

    def getCommandObjectForKey(self, type_key=None):
        obj = None
        if type_key == 'filelist' : obj = GetListOfFiles()
        if type_key == 'filecheck' : obj = CheckIfFilesAreCorrupted()
        if type_key == 'noiseexe' : obj = NoiseToolMainExe()
        if type_key == 'dbinput' : obj = DBInputPrepare()
        if type_key == 'dbfilecheck' : obj = DBFilesContentCheck()
        if type_key == 'dbdataupload' : obj = DBDataUpload()
        if type_key == 'outputfilesprep' : obj = OutputFilesFormat()
        if type_key == 'remotecopy' : obj = CopyFilesOnRemoteLocation()

        return obj

if __name__ == "__main__":

    optionsObject = None
    with open('resources/options_object.txt', 'r') as optobj:
        optionsObject = json.loads(optobj.read())

    optionsObject['run'] = '220796'
    rnum = '220796'

    listFiles = GetListOfFiles(name='filelister', args=optionsObject['filelister'])
    fileIsCorrupted = CheckIfFilesAreCorrupted(name='check', args=optionsObject['check'])
    noiseExe = NoiseToolMainExe(name='noiseexe',args=optionsObject['noiseexe'])

    start_command_on_event_dict = {'initEvent' : [listFiles], listFiles.name: [fileIsCorrupted], fileIsCorrupted.name: [noiseExe]}

    runchain = Chain()
    dyn_opts = {'run':rnum, 'result_folder':'results/'}

    runchain.commands = start_command_on_event_dict
    initialEvent = SimpleEvent('initEvent', True, dyn_opts)
    runchain.startChainWithEvent(initialEvent)


    print noiseExe.args
    print noiseExe.args[2]
    #print noiseExe.options
    print noiseExe.results
