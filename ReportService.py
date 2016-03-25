__author__ = 'rodozov'

import smtplib
import email
from threading import Thread
from email.mime.text import MIMEText
import time

class ReportHandler(Thread):
    
    def __init__(self, msg_queue = None, emails_settings = None):
        super(ReportHandler, self).__init__()
        self.messages = []
        self.mail_service = MailService(emails_settings)
        self.log_service = LogHandler()
        self.message_queue = msg_queue
        self.stop_signal = None # signal to stop

    def sendReport(self):
        pass

    def processMessages(self, msg = None):
        # do something with msg, format and pass
        print 'Blabla from report handler: ', msg
        return msg

    def run(self):
        while True:
            print 'Bla from report mngr run method'
            msg = self.message_queue.get()
            self.processMessages(msg)
            # have to put some delay it's not receiving tasks in the queue fast enough (for the test, not in general)
            time.sleep(5)
            self.message_queue.task_done()
            if self.stop_signal.is_set() and self.message_queue.empty():
                #do some finishing shits
                print 'Finishing report mngr'
                break

    def runSonicRun(self):
        # for the lolz !
        self.start()

    
class MailService(object):

    def __init__(self, emails_settings = None):
        # read file with settings
        with open(emails_settings, 'r') as msettings:
            description = msettings.read()



    def sendMail(self, sender = None,emailText = None, subject = None, emails = None):
        msg = email.message_from_string(emailText)

        msg['Subject'] = subject
        msg['From'] = sender
        msg['To'] = ', '.join( emails )

        string_message = MIMEText(msg.as_string())

        try:
            smtpObj = smtplib.SMTP('localhost:22224')
            #smtpObj = smtplib.SMTP('cernmx.cern.ch')
            smtpObj.sendmail(sender, emails, string_message.as_string())
        except smtplib.SMTPException, ex:
            print 'failed to send email, because ', str(ex)

class LogHandler(object):

    def __init__(self):
        self.log = None

    def parseLog(self, log):
        #
        result = {}
        return result

    def shortReport(self):
        pass

    def longReport(self):
        pass

    def publishOnWeb(self):
        #publish logs on web
        pass

    def notifySubscribers(self):
        # notify based on subscription key, where each key is found in the log and it marks an specific event
        pass
