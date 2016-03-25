import smtplib,email,sys,getopt

def sendMail(self, emailText, subject, emails):
    msg = email.message_from_string(emailText)

    sender = 'mrodozov@cern.ch'

    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = ', '.join( emails )
    
    string_message = msg.as_string()
    
    try:
        smtpObj = smtplib.SMTP('localhost:22224')
        smtpObj.sendmail(sender, emails, string_message)
    except smtplib.SMTPException, ex:
        self._logger.info('failed to send email, because %s' % str(ex))

'''
if(len(sys.argv) < 3):
    print 'Too few arguments, please pass the files with emails, the subject and the message as arguments '
    exit(1)
            

theMessage = sys.argv[3]
theSubject = sys.argv[2]

arrayOfEmails = []
fp = open(sys.argv[1],'r')
arrayOfEmails = fp.readlines()
fp.close()

sendMail('',theMessage,theSubject,arrayOfEmails);
'''
