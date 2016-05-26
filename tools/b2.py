#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
#工具包

1.发邮件
def SendMail(to_addr,subject=None,txt=None,files=None):
参数输入：      
       #to_addr 收件人地址，分号隔开
       #subject 邮件标题 会自动添加发送IP，如果有'[严重警告-CRITICAL]'字样会自动转发给外部服务商电话提醒
       #txt     邮件内容 自动转换为html格式
       #files   文件名，逗号隔开,如果是jpg,gif，png格式，自动显示      
返回：无 
       
2.数据库查询
def fetchdb(DBstr,SQLcmd)
输入：
     DBstr   数据库连接串
     SQLcmd  SQL指令
返回：记录集合
       
3.发短信
def SMS(to_phone,msg=None):
参数输入：      
       #to_phone 短信接收人号码，分号隔开
       #msg     短信内容 会自动添加发送IP
返回：无

4.日志
log('日志内容随便写')
按info级别写日志
当天日志写到/tmp/runcmd.log目录下，往日归档到/tmp/runcmd.log.*

5.字符串转html格式
def txt2html(txt):
说明：把字符串的回车/换行/空格/tab换成html的格式
输入：
    txt plain txt字符串
返回： 含html标签的字符串  

6.警告日志
def alarmlog(cmd,status,value):
说明：把警告的状态记录到数据库LOGQUERY.MONITOR表
输入：
    cmd      字符串，监控命令标识    
    status   整数，0是成功，非零是异常
    value    字符串，20个字符
返回： 无  
"""
import BDini

import os
def IP():   # 取本机IP
    ip=os.popen("/sbin/ifconfig | grep 'inet addr' | awk '{print $2}'").read()
    ip=ip[ip.find(':')+1:ip.find('\n')]
    return ip

def txt2html(txt):
    txt=txt+'\n'
    txt=txt.replace('\r\n','</br><br>')
    txt=txt.replace('\n','</br><br>')
    txt=txt.replace('\r','</br><br>')
    txt=txt.expandtabs()
    txt=txt.replace(' ','&nbsp;')
    txt='<br>'+txt+'</br>' 
    return txt

def SendMail(subject=None,txt=None,files=""):
    """to_addr 收件人地址，逗号隔开
       subject 邮件标题
       txt     邮件内容
       files   文件名，逗号隔开,如果是jpg,gif，png格式，自动显示"""
    
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    from email.mime.base import MIMEBase
    from email import encoders  
    import random
    
    to_addr=BDini.to_addr
    flist=files.split(',')
    msg = MIMEMultipart()
    msg['Subject'] =  subject+'['+IP()+']' 
    mailmsg='<html><body>'+txt2html(txt)

        
    i=random.randint(1,100000)
    for fname in flist:     
        if '.jpg' in fname.lower() or '.gif' in fname.lower() or '.png' in fname.lower():
            mailmsg=mailmsg+'<p><img src="cid:'+str(i)+'"></p>'
            # 添加附件就是加上一个MIMEBase，从本地读取一个图片:
            with open(fname, 'rb') as f:
                # 设置附件的MIME和文件名，这里是png类型:
                mime = MIMEBase('image', 'jpg', filename=fname)
                # 加上必要的头信息:
                mime.add_header('Content-ID', '<'+str(i)+'>')
                mime.add_header('X-Attachment-Id', str(i))
                # 把附件的内容读进来:
                mime.set_payload(f.read())
                # 用Base64编码:
                encoders.encode_base64(mime)
                f.close()
                 # 添加到MIMEMultipart:
                msg.attach(mime) 
            i=i+1
        else:
            if len(fname.strip()) >0:
                with open(fname, 'rb') as f:
                    mime = MIMEBase('application', 'octet-stream')
                    mime.set_payload(f.read())
                    encoders.encode_base64(mime) 
                    mime.add_header('Content-Disposition', 'attachment; filename="%s"' %fname) 
                    msg.attach(mime)            
            
    mailmsg=mailmsg+'</body></html>'
    msg['To'] = to_addr
    msg.attach(MIMEText(mailmsg, 'html', 'utf-8'))   

        
    import smtplib
    server = smtplib.SMTP(BDini.smtp_server) # SMTP协议默认端口是25
    server.set_debuglevel(1)
    server.starttls()
    server.login(BDini.from_addr, BDini.user_pass)
    server.sendmail(BDini.from_addr, to_addr.split(','), msg.as_string())
    server.quit()        



import time
import logging
import logging.handlers
import sys
def log(msg):
    log_format = '[%(asctime)s]%(message)s'
    log_filename='/tmp/'+os.path.basename(sys.argv[0])+'.log.'+time.strftime('%Y%m%d')
    logging.basicConfig(filename=log_filename,format=log_format,datefmt='%Y-%m-%d %H:%M:%S %p',level=logging.DEBUG)
    logging.info(msg)
    


 
    