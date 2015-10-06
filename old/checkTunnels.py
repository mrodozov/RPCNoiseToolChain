import subprocess,os

def checkTunnel(tunnelString):
    p = subprocess.Popen("ps -ef | grep ssh | grep '"+tunnelString+"' | grep -v grep",shell = True,stdout = subprocess.PIPE)
    out,err = p.communicate()
    tunnelRestarted = False
    
    if not out:
        #try to restart the tunnel
        print 'restarting tunnel ',tunnelString
        try:
            tunopen = subprocess.Popen("ssh -f -N -L "+tunnelString,shell = True,close_fds=True)
            out, err = tunopen.communicate()
            if err is None:
                tunnelRestarted = True
        except ValueError:
            print 'restart failed'
            #send email with exception
            
    if err is not None:
        print err
        #send email with err 
    if tunnelRestarted:
        print 'tunnel ', tunnelString,' restarted'

lxplusTunnel = "20222:lxplus.cern.ch:22 cmsusr"
RRtunnel = "22223:runregistry.web.cern.ch:80 cmsusr"
SMTPtunnel = "22224:cernmx.cern.ch:25 cmsusr"


checkTunnel(lxplusTunnel)
checkTunnel(RRtunnel)
checkTunnel(SMTPtunnel)

