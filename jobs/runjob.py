#!/usr/bin/env python27
from configobj import ConfigObj,flatten_errors,ConfigObjError
import os
import subprocess
import sys
import logging
from logging.handlers import TimedRotatingFileHandler
import time
import re
import argparse
import getpass
from mailtools import SMTPMailer
from mailtools import ThreadedMailer
from validate import Validator
from configobj import Section
#use local version of jobstep
#sys.path.insert(0, '/home/bharath/analytics_scripts/includes/python_includes/')
import rtrbi.jobstep

def traversejob(jobfile, stepfrom, stepto, show, Logger):
    
    logInvokeUser(jobfile, stepfrom, stepto, show, Logger)
    jobname = getjobname(jobfile)
    logFileDir, logFileName = getlogfilename(jobfile)
    configspecfilelocation = '/home/' + getpass.getuser() + '/analytics_scripts/jobs/job.spec'
    try:
        job = ConfigObj(jobfile, configspec=configspecfilelocation,file_error=True)
    except (ConfigObjError, IOError), e:
        if Logger is not None:
            Logger.critical( 'Check path. File does not exist. Could not read "%s"' % jobfile)
            raise    
    validator = Validator()
    valoutput = job.validate(validator)
    
    if valoutput != True:
        for (section_list, key, _) in flatten_errors(job, valoutput):
            if key is not None:
                Logger.critical( 'The "%s" key in the section "%s" failed validation' % (key, ', '.join(section_list)))
            else:
                Logger.critical( 'The following section was missing:%s ' % ', '.join(section_list))
        raise

    global mailto
    mailto = unicode(job["mailto"])
        
    jobmaxsteps = len(job.keys())
    if stepto == None:
        stepto = jobmaxsteps

    jobsteps = {}
    stepnumber = 0
    
    for (step, value) in job.iteritems():    
        if isinstance(value, Section):
            stepnumber += 1
            jobsteps[stepnumber] = rtrbi.jobstep.jobstep(jobname, stepnumber, step, job[step]['target'], job[step]['command'], logFileName)
            stepid, stepname, steptarget = jobsteps[stepnumber].getstep()
            if stepnumber >= stepfrom and stepnumber <= stepto:
                if show == False:
                    Logger.info(jobsteps[stepnumber].logstep())
                    runprocess(jobsteps[stepnumber].getcommand(), Logger, jobname, stepid, stepname)
                else:    
                    print 'Step Number: ' + str(stepid)
                    print 'Step Name: ' + str(stepname)
                    print 'Step Target: ' + str(steptarget)
                    print 'Step Command: ' + str(jobsteps[stepnumber].getcommand()) + '\n'

def logInvokeUser(jobfile, stepfrom, stepto, show, Logger):
    sudo_user_cmd="who am i | awk '{print $1}'"
    user = str(subprocess.check_output(sudo_user_cmd, shell=True)).rstrip()
    if user == None:
        user = os.getenv("USER")
    if show == False:    
        Logger.info('User ' + user + ' kicked off ' + jobfile + ' at ' + str(stepfrom))

def getjobname(jobfile):
    pathDir, jobname = os.path.split(getfulljobpath(jobfile))
    return jobname

def getfulljobpath(jobfile):
    pathDir, jobname = os.path.split(jobfile)
    if pathDir == '':
        pathDir = '/home/' + getpass.getuser() + '/analytics_scripts/jobs'
    return pathDir+'/'+jobname
        
def getlogfilename(jobfile):
    logFileDir = '/home/' + getpass.getuser() + '/joblogs/'
    logFileName = logFileDir + getjobname(jobfile) + '.log'
    return logFileDir, logFileName

def setuplogging(jobfile):
    logFileDir, logFileName = getlogfilename(jobfile)
    
    if not os.path.exists(logFileDir):
        os.makedirs(logFileDir)

    Logger = logging.getLogger('joblogger')
    Logger.setLevel(logging.INFO)
    logfileHandler = TimedRotatingFileHandler(logFileName, when='W1', backupCount=3)
    logfileHandler.setLevel(logging.INFO)    
    logfileHandler.setFormatter(logging.Formatter(fmt='%(asctime)s - %(levelname)s - %(lineno)d - %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
    Logger.addHandler(logfileHandler)
    #Logger.basicConfig(filename=logFileName,level=logging.INFO,format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    return Logger

def setupmailerlogging():
    Logger = logging.getLogger('maillogger')
    Logger.setLevel(logging.ERROR)
    logfileHandler = TimedRotatingFileHandler('/home/' + getpass.getuser() + '/joblogs/mailer.log', when='W1', backupCount=3)
    logfileHandler.setLevel(logging.ERROR)    
    logfileHandler.setFormatter(logging.Formatter(fmt='%(asctime)s - %(levelname)s - %(lineno)d - %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
    Logger.addHandler(logfileHandler)
    #Logger.basicConfig(filename=logFileName,level=logging.INFO,format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    return Logger

def runprocess(cmdString, Logger=None, jobname=None, stepnumber=None, stepname=None):

    myProcess = None
    try:
        starttime = time.time()
        myProcess = subprocess.Popen(cmdString, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    except KeyboardInterrupt as e:
        pass

    except Exception as e:
        # log exceptions
        exc_type, exc_value, exc_traceback = sys.exc_info()
        if Logger is not None:
            Logger.critical(traceback.format_exception(exc_type, exc_value, exc_traceback))
            raise

    finally:
        if myProcess is not None:

            # make sure we're done
            output, error = myProcess.communicate()
            Logger.info('Elapsed time : ' + str(time.time()-starttime) + 'secs')
            Logger.info(output)

            # catch errors if any - silent fail on analyze_statistics not run (assuming this is always set to end of a vsql script)
            if (myProcess.returncode is not None) and (myProcess.returncode != 0): 
                logmsg = "Command '{}' returned non-zero exit status {}".format(cmdString, myProcess.returncode)

                if Logger is not None:
                    Logger.error(logmsg)
                    Logger.error(error)

                mailonfail(jobname, stepnumber, starttime, logmsg+'\n'+error, stepname)    
                sys.exit(myProcess.returncode)

def mailonfail(jobname, stepnumber, starttime, logmsg, stepname):
    Mailer = SMTPMailer('127.0.0.1',logger = setupmailerlogging())
    message = u'Job Failed at Step: ' + stepname + '\n'
    message += u'Command Exec Time: ' + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(starttime)) + '\n'
    message += u''+logmsg
    message += '\n\nRerun as deploy user using command:\n/home/deploy/analytics_scripts/jobs/runjob.py --frm ' + str(stepnumber) + ' ' + jobname + '\n'
    logging.getLogger('mailtools.mailer').addHandler
    #print message
    #print type(message)
    #print mailto
    #print type(mailto)
    Mailer.send_plain(
        u'BarryO@renttherunway.it',
        mailto,
        u'Job Failure : ' + jobname + ' failed at Step ' + str(stepnumber),
        message
    )

if __name__ == '__main__': 
    
    parser = argparse.ArgumentParser(description = 'Executes job files. Logs the process at /home/[user]/joblogs/[jobname].log. Emails out errors.')
    parser.add_argument('jobfile', metavar='JobFile', help='The fully qualified job file name (including path)')
    parser.add_argument('--only', help='Run a single step in the job', type=int)
    parser.add_argument('--frm', help='Run from a certain step in the job to the end; if used with --to, you can run jobs till a certain step; if --to is used without specifying --from, it defaults to 1', type=int)
    parser.add_argument('--to', help='Run until a certain step in the job from the beginning; if used with --from, you can run jobs from a certain step; if --from is used without specifying --to, it defaults to max job steps', type=int)
    parser.add_argument('--show', help='Display the job steps of the job along with the step number; Useful when starting or rerunning a job to confirm the step; Can be used with from,to and only', action='store_true', default=False)
    args = parser.parse_args()
    if args.only != None and ( args.to != None or args.frm != None):
        print 'Cannot use --only with --frm or --to'
        parser.print_help()
    if re.match(r'[_a-zA-Z0-9///~/-]*\.job$',args.jobfile,re.I) == None:    
        print'Filename must end with .job. Include full path or filename in jobs folder.'
        parser.print_help() 

    if args.only != None:
        args.frm = args.only 
        args.to = args.frm
    if args.frm == None:
        args.frm = 1            
    jobfilepath = getfulljobpath(args.jobfile)
    Logger = setuplogging(jobfilepath)
    traversejob(jobfilepath, args.frm, args.to, args.show, Logger)
