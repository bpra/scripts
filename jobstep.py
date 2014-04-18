import sys
import re
import ConfigParser
import subprocess
import getopt

config = ConfigParser.RawConfigParser()
config.read('/etc/analytics/config.ini')
vhost = config.get("vertica","host")
vdb = config.get("vertica","database")
vusername = config.get("vertica","user")
vpassword = config.get("vertica","password")  
myhost = config.get("db5","host")
myusername = config.get("db5","user")
mypassword = config.get("db5","Password")  
mydb =  config.get("db5","db_rtrbi")  

class jobstep:
        def __init__(self,_job,_stepid,_stepname,_target,_command,_logfile):
            self.target = _target
            self.jobname = _job
            self.stepid = _stepid
            self.stepname = _stepname
            if re.search("python",self.target)!= None:
                self.command = self.target+" "+_command
            elif self.target == "perl":
                self.command = "perl "+_command    
            elif self.target == "shell":
                self.command = "sh "+_command
            elif self.target == "vertica":    
                self.command = "vsql"+" -h "+vhost+" -d "+vdb+" -U "+vusername+" -w "+vpassword+" -f "+_command
            elif self.target == "mysql":   
                self.command = "mysql -h "+myhost+" -u "+myusername+" -p"+mypassword+" -D "+mydb+" < "+_command+ " >> " +_logfile
            else:
                self.command = _command    
        def execute(self):
            return subprocess.Popen(self.command,shell = True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        def logstep(self):
            return "jobname: "+self.jobname+", stepId: "+str(self.stepid)+", command: "+self.command
        def getcommand(self):
            return self.command
        def getstep(self):    
            return self.stepid,self.stepname,self.target
        def getjobname(self):
            return self.jobname

             


