#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# 外壳shell程序，执行失败可以发邮件/短信通知相关人员

import b2


import os,sys
import subprocess
import time
#发邮件/短信的间隔，避免轰炸
INTERVAL=15

CMDNAME='/tmp/'+os.path.basename(sys.argv[1])+'.log'
shellcmd='source /root/.bash_profile; cd '+os.getcwd()+';'
for cmd in sys.argv[1:]:
    shellcmd=shellcmd+' '+cmd

b2.log(shellcmd)
#执行真正的程序
try:
    f=open(CMDNAME,'w')
    ret=subprocess.check_output(shellcmd,stderr=f,shell=True)
    if os.path.exists(CMDNAME+'.err') and (time.time()-os.path.getmtime(CMDNAME+'.err')<=INTERVAL*60):
        ferr=open(CMDNAME+'.err','r')
        try:
            lines=ferr.read()
        finally:
            ferr.close() 
        
        b2.log("""'[CLOSE][警告告警-WARNING][runcmd.sh]['+sys.argv[1]+'][ERROR!]',lines,'') """)
        b2.SendMail('[CLOSE][警告告警-WARNING][runcmd.sh]['+sys.argv[1]+'][ERROR!]',lines,'')
except subprocess.CalledProcessError as e:
    #失败记下日志
    f=open(CMDNAME,'r')
    try:
        lines=f.read()
    finally:
        f.close()
    b2.log(lines)
    
    if (os.path.exists(CMDNAME+'.err') == False) or (time.time()-os.path.getmtime(CMDNAME+'.err')>INTERVAL*60):
        b2.log("""'[OPEN][警告告警-WARNING][runcmd.sh]['+sys.argv[1]+'][ERROR!]',lines,'') """)
        b2.SendMail('[OPEN][警告告警-WARNING][runcmd.sh]['+sys.argv[1]+'][ERROR!]',lines,'')
        
        ferr=open(CMDNAME+'.err','w')
        try:
            ferr.write(lines)
        finally:
            ferr.close()        

