#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
import shutil
import subprocess
import commands
from datetime import datetime
import smtplib
from email.mime.text import MIMEText
from email import charset
from boto3 import Session

DEBUG = 0
LOG_DIR = "/var/lib/pgsql/data/pg_log/"
LOGNAME_HEADER = "dblog-"
S3_CMD = "aws s3 "
S3_HEADER = "s3://"
S3_BUCKET = "db-log"
SUCCESS = 0
ERROR = -1
ON = 1
OFF = 0
SMTP_HOST = "email-smtp.us-west-2.amazonaws.com"
SMTP_PORT = "587"
SMTP_USER = "xxxxxxxxxxxxxxxxx"
SMTP_PSWD = "xxxxxxxxxxxxxxxxxxxx"
MAIL_FROM = "dblog-s3-transfer@xxxxxx.info"
MAIL_TO = ['xxxxxxx@xxxxxx.co.jp', 'xxxxxxxxxxxxx@xxxxxx.co.jp']
SUBJECT = "DBログS3転送実行通知メール"

### 引数チェック  ################
def arg_check():
   args = len(sys.argv)
   arg_check = 'OK'
   if args > 2:
      arg_check = 'NG:1'
   elif args == 2:
      if sys.argv[1] == "-d":
         debug_flag = ON
      else:
         arg_check = 'NG:2'
   elif args == 1:
       debug_flag = OFF
   else:
       arg_check = 'NG:3'
   if arg_check == 'OK':
      if debug_flag == ON:
         print("debug_flag = " + str(debug_flag))
      ret = 0
   else:
      print("引数指定エラー:python DBlogToS3.py [-d] [error-code]:" + arg_check)
      ret = -1
   return ret, debug_flag

####  Log Transfer #############
def log_transfer( date, debug_flag ):
   print("\n--- Log Transfer to S3 ------------------------")

   logfind_cmd = "ls -ltr " + LOG_DIR + " | tail -n 1 |awk '{print $9}'"
   print("ls cmd =", logfind_cmd)
   log_fname = commands.getoutput( logfind_cmd )
   log_path = LOG_DIR + log_fname
   print("log_fname =", log_fname)

   tmpf_name = LOGNAME_HEADER + date + '.txt'
   tmp_log = LOG_DIR + tmpf_name

   if debug_flag == OFF:
      shutil.copy2( log_path, tmp_log )

   s3_path = S3_HEADER + S3_BUCKET + '/'
   s3_cp_cmd = S3_CMD + 'cp' + ' ' + tmp_log + ' ' + s3_path
   print("S3_CP_CMD = " + s3_cp_cmd)
   if debug_flag == OFF:
      cp_ret = subprocess.call( s3_cp_cmd, shell=True )
   else:
      cp_ret = SUCCESS
   if cp_ret == SUCCESS:
      tr_ret = SUCCESS
      # TMP LOG DELETE
      if debug_flag == OFF:
         rm_ret = os.remove( tmp_log )
         if rm_ret == SUCCESS:
            print("RM_TMPF_CMD completed!")
         else:
            print("RM_TMPF_CMD failed!")
   else:
      tr_ret = ERROR

   return tr_ret, tmpf_name

#### S3 Old Log Delete  #############
def s3_delete( date, debug_flag ):
   print("\n--- S3 Old Log Delete -------------------------")

   del_log = LOGNAME_HEADER + date + '.txt'

   chk_ret = s3_exist_check( S3_BUCKET, del_log )
   print("chk_ret=" + chk_ret)
   if chk_ret == "NOT FOUND":
      del_ret = "NOT FOUND"
   elif chk_ret == "FOUND":
      s3_path = S3_HEADER + S3_BUCKET + '/' + del_log
      s3_del_cmd = S3_CMD + 'rm ' + s3_path
      print("S3_RM_CMD = " + s3_del_cmd)
      if debug_flag == OFF:
         del_ret = subprocess.call( s3_del_cmd, shell=True )
      else:
         del_ret = SUCCESS

   return del_ret, del_log

###  S3 Log Exist Check  #################
def s3_exist_check( bucketname, s3_logf ):
   s3client = Session().client('s3')
   contents = s3client.list_objects(Prefix=s3_logf,Bucket=bucketname).get("Contents")
   if contents:
      for content in contents:
         if content.get( "Key" ) == s3_logf:
            return "FOUND"
   return "NOT FOUND"

####  Mail Routine  ###################
def mail_send(date,tr_status,tr_fname,del_status,del_fname,debug_flag):

   cset = 'utf8'
   tr_info = '転送ログ:' + tr_fname + ' (転送ステータス:' + tr_status + ')'
   del_info = '削除ログ:' + del_fname + ' (削除ステータス:' + del_status + ')'
   s3_info = 'S3ログフォルダ:' + S3_HEADER + S3_BUCKET
   aws_info = 'AWSアカウント:sou-nanboya'
   body = '[実行日時]\r\n' + date + '\r\n\r\n' + '[転送情報]\r\n' + tr_info + '\r\n[削除情報]\r\n' + del_info + '\r\n\r\n[S3情報]\r\n' + s3_info + '\r\n' + aws_info

   msg = MIMEText( body, "plain", cset)
   msg.replace_header("Content-Transfer-Encoding", "base64")
   msg['Subject'] = SUBJECT
   msg['From'] = MAIL_FROM
   msg['To'] = ", ".join(MAIL_TO)
   #msg['Cc'] = CC_TO

   nego_combo = ("starttls", SMTP_PORT)
   smtpclient = smtplib.SMTP(SMTP_HOST, nego_combo[1], timeout=10)
   smtpclient.ehlo()
   smtpclient.starttls()
   smtpclient.ehlo()
   if debug_flag != OFF:
      smtpclient.set_debuglevel(2)

   smtpclient.login(SMTP_USER, SMTP_PSWD)
   #toaddrs = MAIL_TO + ',' + CC_TO
   #smtpclient.sendmail( MAIL_FROM, [toaddrs], msg.as_string() )
   smtpclient.sendmail( MAIL_FROM, MAIL_TO, msg.as_string() )
   smtpclient.quit()

####  Main Routine  #################

if __name__ == '__main__':

   #### Arg check ####################
   ret = arg_check()
   if ret[0] == ERROR:
       exit()
   else:
       debug_flag = ret[1]

   #### Output Log Header ############
   print("=========================================")
   date = datetime.now().strftime("%Y%m%d-%H%M")
   print("[[CMD Execute Date = " + date + ']]')

   tr_date = datetime.now().strftime("%Y%m%d")
   print("\n[Transfer Target Date = " + tr_date + ']')

   #### Log Transfer ############
   tr_ret = log_transfer( tr_date, debug_flag )
   tr_fname = tr_ret[1]
   if tr_ret[0] == SUCCESS:
      tr_status = "成功"
      print("Transfer log(" + tr_fname +") to S3 Success!")
   elif tr_ret[0] == ERROR:
      tr_status = "失敗"
      print("Transfer log(" + tr_fname +") to S3 Error!")

   #### Old S3 Log Delete ############
   del_date = commands.getoutput('date +%Y%m%d --date "90days ago"')
   print("\n[DEL Target Date = " + del_date + ']')
   del_ret = s3_delete( del_date, debug_flag )
   del_fname = del_ret[1]
   if del_ret[0] == SUCCESS:
      del_status = "成功"
      print("S3 RM log(" + del_fname + ") CMD Success!")
   elif del_ret[0] == "NOT FOUND":
      del_status = "該当なし"
      print("S3 RM log(" + del_fname + ") CMD Not Executed!")
   else:
      del_status = "失敗"
      print("S3 RM log(" + del_fname + ") CMD Failed!")

   # Print Delimiter To OutputLog
   print("\n")

   #### Send Mail ############
   mail_send( date, tr_status, tr_fname, del_status, del_fname, debug_flag )
