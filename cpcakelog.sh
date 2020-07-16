#!/bin/sh

###################################
#
# Transfer cakelog to S3
#
###################################

S3_ELOG_PATH="s3://sou-pro-cakelog/error-log/"
S3_DLOG_PATH="s3://sou-pro-cakelog/debug-log/"

# Transfer log to S3

DATE=`date +%Y%m%d`
HOST_NAME=`hostname`
WORK_DIR=/var/www/pms/current/logs

ELOG=error.log
DLOG=debug.log

#TMPELOG=${HOST_NAME}_${ELOG}-${DATE}.txt
#TMPDLOG=${HOST_NAME}_${DLOG}-${DATE}.txt

TMPELOG=${ELOG}-${DATE}.txt
TMPDLOG=${DLOG}-${DATE}.txt

cd ${WORK_DIR}

cp -p ${ELOG} ${TMPELOG}
cp -p ${DLOG} ${TMPDLOG}

aws s3 cp ${TMPELOG} ${S3_ELOG_PATH} --profile s3
aws s3 cp ${TMPDLOG} ${S3_DLOG_PATH} --profile s3

rm -f ${TMPELOG}
rm -f ${TMPDLOG}


# Delete old log at S3

RM_DATE=`date +%Y%m%d --date "90days ago"`
OLD_RMELOG=${HOST_NAME}_error.log-${RM_DATE}.txt
OLD_RMDLOG=${HOST_NAME}_debug.log-${RM_DATE}.txt
RMELOG=error.log-${RM_DATE}.txt
RMDLOG=debug.log-${RM_DATE}.txt

aws s3 rm ${S3_ELOG_PATH}${OLD_RMELOG} --profile s3
aws s3 rm ${S3_DLOG_PATH}${OLD_RMDLOG} --profile s3
aws s3 rm ${S3_ELOG_PATH}${RMELOG} --profile s3
aws s3 rm ${S3_DLOG_PATH}${RMDLOG} --profile s3

