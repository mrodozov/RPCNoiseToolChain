__author__ = 'rodozov'

import smtplib
import email
from threading import Thread
import Queue
from email.mime.text import MIMEText
import time
import json
import paramiko
import SSHTransportService
import getpass

class ReportHandler(Thread):
    
    def __init__(self, results_queue = None, emails_settings = None , mailpass = None, logFile = None):
        super(ReportHandler, self).__init__()
        self.messages = []
        self.mail_service = MailService(emails_settings, mailpass)
        self.log_service = LogHandler()
        self.run_results_queue = results_queue
        self.reports_queue = Queue.Queue()
        self.suspend_signal = None
        self.stop_signal = None # signal to stop
        self.ssh_client = None
        self.ssh_conn_name = None
        self.remote_dir = None
        self.logFile = logFile

    def processResults(self, msg = None):
        # do something with msg, format and pass
        print 'Blabla from report handler: ', msg
        return msg

    def run(self):
        while True:
            #print 'Bla from report mngr run method'
            run_result = self.run_results_queue.get()

            shortLog, fullLog = self.log_service.parseLog(run_result)

            #print 'short log', shortLog
            #print 'full log ', fullLog
            
            rnum = [r for r in shortLog.keys()][0]
            if shortLog:
                try:
                    self.mail_service.sendMail(json.dumps(shortLog), 'Run number ' + rnum + ' failed')
                except Exception as e:
                    print e.message
            
            if fullLog:
                self.log_service.updateLogFile(self.logFile, fullLog)
                sftp_client = paramiko.SFTPClient.from_transport(self.ssh_client.get_transport_for_connection(self.ssh_conn_name))
                try:
                    sftp_client.chdir(self.remote_dir)
                    sftp_client.put(self.logFile, self.remote_dir + '/' + 'ErrorLog.log')
                except IOError, exc:
                    print exc.message
                sftp_client.get_channel().close()

            # have to put some delay it's not receiving tasks in the queue fast enough (for the test, not in general)
            time.sleep(5)
            self.run_results_queue.task_done()
            if self.stop_signal.is_set() and self.run_results_queue.empty():
                #do some finishing shits
                print 'Finishing report mngr'
                self.mail_service.server.quit()
                break

    def runSonicRun(self):
        # for the lolz !
        self.start()

class MailService(object):

    def __init__(self, emails_settings = None, paswd = None):
        # read file with settings
        self.description = None
        with open(emails_settings, 'r') as msettings:
            self.description = json.loads(msettings.read())
        self.server = smtplib.SMTP(self.description['url']+':'+self.description['port'])
        if paswd:
            self.server.ehlo()
            self.server.starttls()
            #print self.description['username'], paswd
            self.server.login(self.description['username'], paswd)


    def sendMail(self, emailText = None, subject = None):

        msg = email.message_from_string(emailText)
        msg['Subject'] = subject
        msg['From'] = self.description['sender']
        msg['To'] = ', '.join( self.description['mail_list'] )

        string_message = MIMEText(msg.as_string())

        try:
            self.server.sendmail(self.description['sender'], self.description['mail_list'], string_message.as_string())
        except smtplib.SMTPException, ex:
            print 'failed to send email, because ', str(ex)

class LogHandler(object):

    def __init__(self):
        pass

    def updateLogFile(self, logFile = None, log = None):

        with open(logFile,'r+') as lfile:
            updatedLog = json.loads(lfile.read())
            lfile.seek(0)
            lfile.truncate()
            updatedLog.update(log)
            lfile.write(json.dumps(updatedLog, indent=1, sort_keys=True))

    def parseLog(self, log):
        rnum = [r for r in log.keys()][0]
        shortLog = {rnum: log[rnum]['results']}
        fullLog = log
        return shortLog, fullLog

if __name__ == "__main__":

    # make a check on server connection (ReportService run loop)
    # save unsaved reports to local file
    
    mpass = getpass.getpass('mail pass: ')
    passwd = getpass.getpass('lxplus pass: ')

    with open('resources/options_object.txt', 'r') as optobj: optionsObject = json.loads(optobj.read())
    with open('resources/mail_settings.json') as mail_settings_file: mail_settings = json.loads(mail_settings_file.read())

    connections_dict = {}
    connections_dict.update({'webserver':optionsObject['webserver_remote']})
    connections_dict['webserver']['ssh_credentials']['password'] = passwd

    remote_destinations = {'webserver': optionsObject['webserver_remote']}
    remote_webserver = optionsObject['webserver_remote']['destination_root']
    
    #transportService = SSHTransportService(remote_destinations)
    reportsQueue = Queue.Queue()
    #stop = Event()
    
    #reportsMngr = ReportHandler(reportsQueue, 'resources/mail_settings.json', mpass, 'resources/ErrorLog.log')
    
    
    with open('resources/mail_settings.json') as mail_settings_file: mail_settings = json.loads(mail_settings_file.read())
    m_service = MailService('resources/mail_settings.json', mpass)
    m_service.sendMail('test the service','test service')
    
