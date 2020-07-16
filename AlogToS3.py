#!/usr/bin/env python
# -*- coding: utf-8 -*-

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
LOG_DIR = "/var/log/httpd/"
ALOG = "alog.1"
ELOG = "elog.1"
ALOG_PATH = LOG_DIR + ALOG
ELOG_PATH = LOG_DIR + ELOG
S3_CMD = "aws s3 "
S3_PATH_HEADER = "s3://"
S3_BUCKET = "xx-apache-log"
ALOG_FOLDER = "access-log/"
ELOG_FOLDER = "error-log/"
S3_ALOG_FOLDER_PATH = S3_PATH_HEADER + S3_BUCKET + '/' + ALOG_FOLDER
S3_ELOG_FOLDER_PATH = S3_PATH_HEADER + S3_BUCKET + '/' + ELOG_FOLDER
SUCCESS = 0
OFF = 0
SMTP_HOST = "email-smtp.us-west-2.amazonaws.com"
SMTP_PORT = "587"
SMTP_USER = "xxxxxxxxxxx"
SMTP_PSWD = "xxxxxxxxxxxxxxxxxxx"
MAIL_FROM = "apachelog-s3-transfer@test.info"
MAIL_TO = ['xxxx@xxxx.co.jp', 'xxxxxx@xxxxx.co.jp']
SUBJECT = "apacheログS3転送実行通知メール"

####  Log Transfer #############
def log_transfer( target_log, date ):

   ans = os.path.exists( target_log )

   if ans == False:
      tr_ret = 1
      tmpf_name = '対象なし'
      return tr_ret, tmpf_name
   
   ret = logtype( target_log )
   s3_tr_folder = ret[0]
   tr_fname_header = ret[1]

   tmpf_name = tr_fname_header + date + '.log'
   tmp_log = LOG_DIR + tmpf_name
   shutil.move(target_log, tmp_log)

   cp_s3_cmd = S3_CMD + 'cp' + ' ' + tmp_log + ' ' + s3_tr_folder
   print("S3_CP_CMD = " + cp_s3_cmd)
   if DEBUG == OFF:
      cp_ret = subprocess.call( cp_s3_cmd, shell=True )
   else:
      cp_ret = SUCCESS
   if cp_ret == SUCCESS:
      tr_ret = SUCCESS
      # TMP LOG DELETE
      if DEBUG == OFF:
         rm_ret = os.remove(tmp_log)
         if rm_ret == SUCCESS:
            print("RM_TMPF_CMD completed!")
         else:
            print("RM_TMPF_CMD failed!")
   else:
      tr_ret = -1

   return tr_ret, tmpf_name

####  Delete Old S3 log  ##############
def s3_delete( target_log, date ):

   ret = logtype( target_log )
   s3_del_folder = ret[0]
   del_fname_header = ret[1]
   logf_name = del_fname_header + date + '.log' 

   chk_ret = s3_exist_check( S3_BUCKET, logf_name )
   print("chk_ret=" + chk_ret )

   if chk_ret == "NOT FOUND":
      del_ret = "NOT FOUND"
   elif chk_ret == "FOUND":
      del_s3_cmd = S3_CMD + 'rm' + ' ' + s3_del_folder + logf_name
      print("S3_RM_CMD = " + del_s3_cmd)
      if DEBUG == OFF:
         del_ret = subprocess.call( del_s3_cmd, shell=True )
      else:
         del_ret = SUCCESS

   return del_ret, logf_name

###  S3 Log Exist Check  #################
def s3_exist_check( bucketname, s3_logf ):
   s3client = Session().client('s3')
   contents = s3client.list_objects(Prefix=s3_logf,Bucket=bucketname).get("Contents")
   if contents:
      for content in contents:
         if content.get( "Key" ) == s3_logf:
            return "FOUND"
   return "NOT FOUND"

#### Output Log Type ###################
def logtype( target_log ):

   # Transfer
   if target_log == ALOG_PATH:
      print("\n--- Transfer AccessLog To S3 ---------------------------------")
      s3_folder = S3_ALOG_FOLDER_PATH
      fname_header = 'access-'
   elif target_log == ELOG_PATH:
      print("\n--- Transfer ErrorLog To S3 ----------------------------------")
      s3_folder = S3_ELOG_FOLDER_PATH
      fname_header = 'error-'
   # Delete
   elif target_log == ALOG:
      print("\n--- S3 Old AccessLog Delete ----------------------------------")
      s3_folder = S3_ALOG_FOLDER_PATH
      fname_header = 'access-'
   elif target_log == ELOG:
      print("\n--- S3 Old ErrorLog Delete -----------------------------------")
      s3_folder = S3_ELOG_FOLDER_PATH
      fname_header = 'error-'

   return s3_folder, fname_header

####  Mail Routine  ###################
def mail_send(date,tr_alog_status,tr_alog_fname,tr_elog_status,tr_elog_fname,del_alog_status,del_alog_fname,del_elog_status,del_elog_fname):

   cset = 'utf8'  
   tr_alog_info = '転送アクセスログ:' + tr_alog_fname + ' (転送ステータス:' + tr_alog_status + ')'
   tr_elog_info = '転送エラーログ  :' + tr_elog_fname + ' (転送ステータス:' + tr_elog_status + ')'
   del_alog_info = '削除アクセスログ:' + del_alog_fname + ' (削除ステータス:' + del_alog_status + ')'
   del_elog_info = '削除エラーログ  :' + del_elog_fname + ' (削除ステータス:' + del_elog_status + ')'
   s3_alog_info = 'S3アクセスログフォルダ:' + S3_ALOG_FOLDER_PATH
   s3_elog_info = 'S3エラーログフォルダ  :' + S3_ELOG_FOLDER_PATH
   aws_info = 'AWSアカウント:sou-nanboya'
   body = '[実行日時]\r\n' + date + '\r\n\r\n' + '[転送情報]\r\n' + tr_alog_info +  '\r\n' + tr_elog_info + '\r\n[削除情報]\r\n' + del_alog_info + '\r\n' + del_elog_info + '\r\n\r\n[S3情報]\r\n' + s3_alog_info + '\r\n' + s3_elog_info + '\r\n' + aws_info

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
   if DEBUG != OFF:
      smtpclient.set_debuglevel(2)

   smtpclient.login(SMTP_USER, SMTP_PSWD)
   #toaddrs = MAIL_TO + ',' + CC_TO
   #smtpclient.sendmail( MAIL_FROM, [toaddrs], msg.as_string() )
   smtpclient.sendmail( MAIL_FROM, MAIL_TO, msg.as_string() )
   smtpclient.quit()

if __name__ == '__main__':

   ####  Output Log Header  ####################
   print("==============================================")
   date = datetime.now().strftime("%Y%m%d-%H%M")
   print("[[ CMD Execute Date = " + date + ']]')

   tr_date = datetime.now().strftime("%Y%m%d")
   print("\n[Transfer Target Date = " + tr_date + ']')

   ####  AccessLog Transfer  ####################
   tr_ret = log_transfer( ALOG_PATH, tr_date )
   tr_alog_fname = tr_ret[1]
   if tr_ret[0] == SUCCESS:
      tr_alog_status = "成功"
      print("AccessLog(" + tr_alog_fname + ") Transfer S3 CMD Success!")
   elif tr_ret[0] == 1:
      tr_alog_status = "転送対象なし"
      print("\nAccessLog(" + ALOG + ") Not Exist, Not Transfer")
   else:
      tr_alog_status = "失敗"
      print("AccessLog(" + tr_alog_fname + ") Transfer S3 CMD Failed!")

   ####  ErrorLog Transfer  #####################
   tr_ret = log_transfer( ELOG_PATH, tr_date )
   tr_elog_fname = tr_ret[1]
   if tr_ret[0] == SUCCESS:
      tr_elog_status = "成功"
      print("ErrorLog(" + tr_elog_fname + ") Transfer S3 CMD Success!")
   elif tr_ret[0] == 1:
      tr_elog_status = "転送対象なし"
      print("\nErrorLog(" + ELOG + ") Not Exist, Not Transfer")
   else:
      tr_elog_status = "失敗"
      print("ErrorLog(" + tr_elog_fname + ") Transfer S3 CMD Failed!")

   ####  Old S3 Log Delete  #####################
   del_date = commands.getoutput('date +%Y%m%d --date "90days ago"')
   print("\n[DEL Target Date = " + del_date + "]")

   # AccessLog Delete
   del_ret = s3_delete( ALOG, del_date )
   del_alog_fname = del_ret[1]
   if del_ret[0] == SUCCESS:
      del_alog_status = "成功"
      print("S3 Old AccessLog(" + del_alog_fname + ") Delete CMD Success!")
   elif del_ret[0] == "NOT FOUND":
      del_alog_status = "該当なし" 
      print("S3 Old AccessLog(" + del_alog_fname + ") Delete CMD Not Executed!")
   else:
      del_alog_status = "失敗"
      print("S3 Old AccessLog(" + del_alog_fname + ") Delete CMD Failed!")
   
   # ErrorLog Delete
   del_ret = s3_delete( ELOG, del_date )
   del_elog_fname = del_ret[1]
   if del_ret[0] == SUCCESS:
      del_elog_status = "成功"
      print("S3 Old ErrorLog(" + del_elog_fname + ") Delete CMD Success!")
   elif del_ret[0] == "NOT FOUND":
      del_elog_status = "該当なし" 
      print("S3 Old AccessLog(" + del_elog_fname + ") Delete CMD Not Executed!")
   else:
      del_elog_status = "失敗"
      print("S3 Old ErrorLog(" + del_elog_fname + ") Delete CMD Failed!")
   
   # Print Delimiter
   print("\n")

   # Mail Send
   mail_send(date,tr_alog_status,tr_alog_fname,tr_elog_status,tr_elog_fname,del_alog_status,del_alog_fname,del_elog_status,del_elog_fname)
