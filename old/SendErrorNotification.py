'''
Author : Mircho Rodozov
mrodozov@cern.ch
'''

import smtplib,email,sys, getopt
from email.mime.text import MIMEText

def sendMail(self, emailText, subject, emails):
    msg = email.message_from_string(emailText)

    sender = 'mrodozov@cern.ch'

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


if(len(sys.argv) < 3):
    print 'Too few arguments, please pass the files with emails, the subject and the message as arguments '
    exit(1)
            

theMessage = sys.argv[3]
theSubject = sys.argv[2]

arrayOfEmails = []
fp = open(sys.argv[1],'r')
arrayOfEmails = fp.readlines()
fp.close()

sendMail(theMessage,theSubject,arrayOfEmails);
