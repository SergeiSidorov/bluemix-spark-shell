#!/usr/bin/env bash

# IBM Confidential
# OCO Source Materials
# 5725-Y50
# (C) Copyright IBM Corp. 2015
# The source code for this program is not published or otherwise divested of its trade secrets, irrespective of what has been deposited with the U.S. Copyright Office.

###############################################################################
#
# This script performs the following steps:
# 1. Uploads local files to the cluster host (i.e. '--master').
#    The files it uploads are specified in the following parameters:
#       --files
#       --jars
#       --py-files
#       The application JAR file or python file
#    If you want to use files already on the spark cluster, you can disable
#    the uploading of files by setting operating system environment variables
#    described below. Uploaded files will be placed on the cluster at
#    <TENANT_ID>/data/libs/
# 2. Re-writes paths for files uploaded to the cluster.  The re-written paths
#    are used when calling submit REST API.
# 3. Gets returning spark-submit's submission ID and periodically polls for status
#    of the job using the submission ID.
# 4. When the job is FINISHED, downloads 'stdout' and 'stderr' from the
#    cluster.
#
# Before running this script, operating system variables must be set.
# Optional:
#    SS_APP_MAIN_UPLOAD=<true|false> # Default: 'true' application jar file is uploaded.
#    SS_FILES_UPLOAD=<true|false> # Default: 'true'. '--files' and "--py-files" files are uploaded.
#    SS_JARS_UPLOAD=<true|false> # Default: 'true'. '--jars' files are uploaded.
#    SS_LOG_ENABLE=<true|false> # Default: 'true'. Execution log is created.
#
# VCAP information needs to be made available to this program in the '--vcap'
# parameter.  The VCAP information is obtained from your BlueMix application.
# Here is one way to create a file from your VCAP:
# cat <<EOT > ~/vcap.json
# {
#    "credentials": {
#              "tenant_id": "s12345676",
#              "tenant_id_full": "s123567-890",
#              "cluster_master_url": "https://169.54.219.20",
#              "instance_id": "1ed3b844-f507-44ae-aa1b-9e6f8a79d781",
#              "tenant_secret": "e407f6f4-0c37-4428-bd16-e66caa73bb4b",
#              "plan": "ibm.SparkService.PayGoPersonal"
#          }
#    }
# }
# EOT
#
# Example command to run:
#
# ./spark-submit.sh \
#    --vcap ~/vcap.json \
#    --deploy-mode cluster \
#    --class com.ibm.sparkservice.App \
#    --master https://169.54.219.20 \
#    --jars /path/to/mock-library-1.0.jar,/path/to/mock-utils-1.0.jar \
#    ~/mock-app-1.0.jar
#
#
###############################################################################

invokeCommand="$(basename $0) $@"

# -- User-modifiable variables ------------------------------------------------
# To modify,  set the operating system environment variable to the desired value.

if [ -z $SS_LOG_ENABLE ]; then SS_LOG_ENABLE=true;  fi # Enable detailed logging
if [ -z $SS_APP_MAIN_UPLOAD ]; then SS_APP_MAIN_UPLOAD=true; fi # If true, copy the local application JAR or python file to the spark cluster
if [ -z $SS_JARS_UPLOAD ]; then SS_JARS_UPLOAD=true; fi # If true, copy the local JAR files listed in "--jars" to the spark cluster.
if [ -z $SS_FILES_UPLOAD ]; then SS_FILES_UPLOAD=true; fi # If true, copy the local files listed in "--files" and "--py-files" to the spark cluster.
if [ -z $SS_POLL_INTERVAL ]; then SS_POLL_INTERVAL=10; fi # Number of seconds until script polls spark cluster again.
if [ -z $SS_SPARK_WORK_DIR ];     then SS_SPARK_WORK_DIR="workdir";  fi # Work directory on spark cluster
if [ -z $SS_DEBUG ];              then SS_DEBUG=false;               fi # Detailed debugging

# -- Set working environment variables ----------------------------------------

if [ "${SS_DEBUG}" = "true" ]
then
    set -x
fi

EXECUTION_TIMESTAMP="$(date +'%s%N')"
APP_MAIN=
app_parms=
FILES=
JARS=
PY_FILES=
CLASS=
APP_NAME=
DEPLOY_MODE=
LOG_FILE=spark-submit_${EXECUTION_TIMESTAMP}.log
MASTER=
INSTANCE_ID=
TENANT_ID=
TENANT_SECRET=
submissionId=
declare -a CONF_KEY
declare -a CONF_VAL
confI=0
CHECK_STATUS=false
KILL_JOB=false
PY_APP=false
RETRY=4
VERSION="1.0"

UUID=$(openssl rand -base64 64 | shasum -a 1 | awk '{print $1}')
SERVER_SUB_DIR="${UUID}"

uploadList=" "
# =============================================================================
# -- Functions ----------------------------------------------------------------
# =============================================================================

printUsage()
{
   echo "Usage: "
   echo "     spark-submit.sh --vcap <vcap-file> [options] <app jar | python file> [app arguments] "
   echo "     spark-submit.sh --vcap <vcap-file> --kill [submission ID] --master [cluster_master_url] "
   echo "     spark-submit.sh --vcap <vcap-file> --status [submission ID] --master [cluster_master_url] "
   echo "     spark-submit.sh --help  "
   echo "     spark-submit.sh --version  "
   echo " "
   echo "     vcap-file:                  json format file that contains spark service credentials, "
   echo "                                 including cluster_master_url, tenant_id, instance_id, and tenant_secret"
   echo "     cluster_master_url:         The value of 'cluster_master_url' on the service credentials page"
   echo " "
   echo " options:"
   echo "     --help                      Print out usage information."
   echo "     --version                   Print out the version of spark-submit.sh"
   echo "     --master MASTER_URL         MASTER_URL is the value of 'cluster-master-url' from spark service instance credentials"
   echo "     --deploy-mode DEPLOY_MODE   DEPLOY_MODE must be 'cluster'"
   echo "     --class CLASS_NAME          Your application's main class (for Java / Scala apps)."
   echo "     --name NAME                 A name of your application."
   echo "     --jars JARS                 Comma-separated list of local jars to include on the driver and executor classpaths."
   echo "     --files FILES               Comma-separated list of files to be placed in the working directory of each executor."
   echo "     --conf PROP=VALUE           Arbitrary Spark configuration property."
   echo "     --py-files PY_FILES         Comma-separated list of .zip, .egg, or .py files to place on the PYTHONPATH for Python apps."
   echo " "

   echo "     --kill SUBMISSION_ID        If given, kills the driver specified."
   echo "     --status SUBMISSION_ID      If given, requests the status of the driver specified."
   echo " "
   exit 0
}

printVersion()
{
   echo "  spark-submit.sh  VERSION : '${VERSION}' "
   exit 0
}

logMessage()
{
    if [ "${SS_LOG_ENABLE}" = "true" ]
    then
        echo "$1" >> ${LOG_FILE}
    else
        echo "$1"
    fi
}

logFile()
{
    logMessage "Contents of $1:"
    if [ "${SS_LOG_ENABLE}" = "true" ]
    then
        cat "$1" >> ${LOG_FILE}
    else
        cat "$1"
    fi
}

endScript()
{
    if [ "${SS_LOG_ENABLE}" = "true" ]
    then
        echo "Log file can be found at $LOG_FILE"
    fi
}

base64Encoder()
{
    encoded="`printf $1 | base64`"
    echo "$encoded"
}

get_from_vcap()
{
    local vcapFilePath=$1
    local vcapKey=$2
    echo `grep ${vcapKey}\" ${vcapFilePath} | awk '{print $2}' | sed 's/\"//g' | sed 's/\,//g'`
}

get_hostname_from_url()
{
    local url=$1
    echo $url | sed -n 's/[^:]*:\/\/\([^:]*\)[:]*.*/\1/p'
}

get_http_authentication()
{
    echo "--insecure -u $TENANT_ID:$TENANT_SECRET"
}

get_http_instance_id()
{
    echo "X-Spark-service-instance-id: $INSTANCE_ID"
}

local2server()
{
    local localPath=$1
    local serverPath=$2
    logMessage "local2server command : curl -v -X PUT $(get_http_authentication) -H \"$(get_http_instance_id)\" --data-binary \"@$localPath\" https://$HOSTNAME/tenant/data/$serverPath"
    local result="`curl -v -X PUT $(get_http_authentication) -H \"$(get_http_instance_id)\" --data-binary "@$localPath" https://$HOSTNAME/tenant/data/$serverPath 2>&1`"
    uploadList+="$(fileNameFromPath $localPath) "
    logMessage "local2server result : ${result}"
}

deleteFolderOnServer()
{
    local serverDir=$1
    local encodedKey=$(base64Encoder $TENANT_ID)
    logMessage "deleteFolderOnServer command : curl -X DELETE $(get_http_authentication) -H \"$(get_http_instance_id)\" https://$HOSTNAME/tenant/data/$serverDir"
    local result="`curl  -X DELETE $(get_http_authentication) -H \"$(get_http_instance_id)\" https://$HOSTNAME/tenant/data/$serverDir  2>&1`"
    logMessage "deleteFolderOnServer result : ${result}"
}

local2server_list()
{
    local localFiles=$1
    local files=$2
    OIFS=$IFS
    IFS=","
    localFileArray=($localFiles)
    fileArray=($files)
    IFS=$OIFS

    for ((i=0; i<${#localFileArray[@]}; ++i))
    do
        local2server ${localFileArray[$i]} ${fileArray[$i]}
    done
}

fileNameFromPath()
{
    local path=$1
    local fileName="`echo ${path} | awk 'BEGIN{FS="/"}{print $NF}'`"
    echo "$fileName"
}

fileNameFromPath_list()
{
    local paths=$1
    OIFS=$IFS
    IFS=","
    pathArray=($paths)
    IFS=$OIFS
    local fileNames=
    for ((i=0; i<${#pathArray[@]}; ++i))
    do
        local fileName=$(fileNameFromPath ${pathArray[$i]})
        if [ -z "${fileNames}" ]
        then
            fileNames="$fileName"
        else
            fileNames="$fileNames,$fileName"
        fi
    done
    echo "$fileNames"
}

convert2serverPath()
{
    local fileName=$(fileNameFromPath $1)
    local serverFile="${SERVER_SUB_DIR}/${fileName}"
    echo "$serverFile"
}

convert2serverPath_list()
{
    local localFiles=$1
    OIFS=$IFS
    IFS=","
    localFileArray=($localFiles)
    IFS=$OIFS
    local serverFiles=
    for ((i=0; i<${#localFileArray[@]}; ++i))
    do
        local serverFile=$(convert2serverPath ${localFileArray[$i]})
        if [ -z "${serverFiles}" ]
        then
            serverFiles="$serverFile"
        else
            serverFiles="$serverFiles,$serverFile"
        fi
    done
    echo "$serverFiles"
}

convert2submitPath()
{
    local serverFile=$1
    echo "${PREFIX_SERVER_PATH}/$serverFile"
}

convert2submitPath_list()
{
    local serverFiles=$1
    OIFS=$IFS
    IFS=","
    serverFileArray=($serverFiles)
    IFS=$OIFS
    local submitPaths=
    for ((i=0; i<${#serverFileArray[@]}; ++i))
    do
        local submitPath=$(convert2submitPath ${serverFileArray[$i]})
        if [ -z "${submitPaths}" ]
        then
            submitPaths="$submitPath"
        else
            submitPaths="$submitPaths,$submitPath"
        fi
    done
    echo "$submitPaths"
}

server2local()
{
    local serverPath=$1
    local localPath=$2
    local encodedKey=$(base64Encoder ${TENANT_ID})
    logMessage "server2local command : curl -D \"$localPath.header\" -v -X GET $(get_http_authentication)  -H \"$(get_http_instance_id)\" https://$HOSTNAME/tenant/data/$serverPath"
    local result="`curl -D "${localPath}.header" -v -X GET $(get_http_authentication)  -H \"$(get_http_instance_id)\" https://$HOSTNAME/tenant/data/$serverPath`"
    fileExist="`cat "${localPath}.header" | grep "404 NOT FOUND" | wc -l`"
    if [ "${fileExist}" == 0 ]
    then
        echo "${result}" > $localPath
    fi
    rm -f $localPath.header
    return ${fileExist}
}


terminate_spark()
{
    if [ -n "${submissionId}" ]
    then
        logMessage "WARN: Terminate signal received. Stop spark job: ${submissionId}"
        local result=$(call_kill_REST)
        logMessage "Terminate result : $result"
        local resultStatus=$(call_status_REST)
        driverStatus="`echo ${resultStatus} | sed -n 's/.*\"driverState\" : \"\([^\"]*\)\",.*/\1/p'`"
        echo "Job kill:  ${submissionId} status is $driverStatus"
    fi
    endScript
    exit 1
}

substituteArg()
{
    local arg=$1
    local fileName="`echo ${arg} | sed -n 's/.*file:\/\/\([^\"]*\)\"/\1/p'`"
    local newArg=$arg
    if [ -n "${fileName}" ]
    then
        if [[ "$uploadList" =~ $fileName ]]; then
            newArg="\"file://$SERVER_SUB_DIR/$fileName\""
        fi
   fi
   echo "$newArg"
}

parsing_appArgs()
{
   local argString=$1
   OIFS=$IFS
   IFS=","
   local argArray=($argString)
   IFS=$OIFS
   local resultArgs=
   for ((i=0; i<${#argArray[@]}; ++i))
   do
        local arg=$(substituteArg ${argArray[$i]})
        if [ -z "${resultArgs}" ]
        then
            resultArgs="$arg"
        else
            resultArgs="$resultArgs,$arg"
        fi
    done
    echo "$resultArgs"
}

submit_REST_json()
{
    local appArgs1="$1"
    local appResource="$2"
    local mainClass="$3"
    local sparkJars="$4"
    local sparkFiles="$5"
    local sparkPYFiles="$6"
    local appArgs=$(parsing_appArgs "${appArgs1}")
    local reqJson="{"
    reqJson+=" \"action\" : \"CreateSubmissionRequest\",  "
    if [ "${PY_APP}" = "true" ]
    then
        local appResourceFileName=$(fileNameFromPath $appResource)
        if [ -n "$sparkPYFiles" ]
        then
            local sparkPYFileNames=$(fileNameFromPath_list ${sparkPYFiles})
            if [ -n "$appArgs" ]
            then
                appArgs="\"--primary-py-file\",\"$appResourceFileName\",\"--py-files\",\"${sparkPYFileNames}\",$appArgs"
            else
                appArgs="\"--primary-py-file\",\"$appResourceFileName\",\"--py-files\",\"${sparkPYFileNames}\""
            fi
        else
            if [ -n "$appArgs" ]
            then
                appArgs="\"--primary-py-file\",\"$appResourceFileName\",$appArgs"
            else
                appArgs="\"--primary-py-file\",\"$appResourceFileName\""
            fi
        fi
    fi
    reqJson+=" \"appArgs\" : [ $appArgs ], "
    reqJson+=" \"appResource\" : \"$appResource\","
    reqJson+=" \"clientSparkVersion\" : \"1.6.0\","
    reqJson+=" \"environmentVariables\" : { \"SPARK_SUBMIT_DEPLOY_MODE\" : \"cluster\"}, "
    reqJson+=" \"mainClass\" : \"$mainClass\", "
    reqJson+=" \"sparkProperties\" : { "

    ##### properties: spark.app.name
    reqJson+=" \"spark.app.name\" : \"$APP_NAME\", "

    ##### properties: spark.submit.deployMode
    reqJson+=" \"spark.submit.deployMode\" : \"cluster\", "

    ##### properties: spark.jars - add appResource to jars list if this is java application
    if [ -n "$sparkJars" ]
    then
        if [ "${PY_APP}" = "false" ]
        then
            sparkJars+=",$appResource"
        fi
    else
        if [ "${PY_APP}" = "false" ]
        then
           sparkJars=$appResource
        fi
    fi
    if [ -n "$sparkJars" ]
    then
        reqJson+=" \"spark.jars\" : \"$sparkJars\", "
    fi

    ##### properties: spark.files - add appResource to files list if this is python application
    if [ -n "$sparkFiles" ]
    then
        if [ -n "$sparkPYFiles" ]
        then
            sparkFiles+=",$appResource,$sparkPYFFiles"
        elif [ "${PY_APP}" == "true" ]
        then
            sparkFiles+=",$appResource"
        fi
    else
        if [ -n "$sparkPYFiles" ]
        then
            sparkFiles="$appResource,$sparkPYFiles"
        elif  [ "${PY_APP}" == "true" ]
        then
            sparkFiles="$appResource"
        fi
    fi
    if [ -n "$sparkFiles" ]
    then
        reqJson+=" \"spark.files\" : \"$sparkFiles\", "
    fi

    ##### properties: spark.submit.pyFiles
    if [ -n "$sparkPYFiles" ]
    then
        reqJson+=" \"spark.submit.pyFiles\" : \"$sparkPYFiles\", "
    fi

    for ((i=0; i<${#CONF_KEY[@]}; ++i))
    do
        reqJson+=" \"${CONF_KEY[$i]}\" : \"${CONF_VAL[$i]}\", "
    done

    ##### properties: spark.service.* : all properties specific for spark service
    reqJson+=" \"spark.service.tenant_id\" : \"$TENANT_ID\", "
    reqJson+=" \"spark.service.instance_id\" : \"$INSTANCE_ID\", "
    reqJson+=" \"spark.service.tenant_secret\" : \"$TENANT_SECRET\", "
    reqJson+=" \"spark.service.spark_version\" : \"1.6.0\""

    reqJson+="}"
    reqJson+="}"
    echo $reqJson
}

status_kill_REST_json()
{
    reqJson="{"
    reqJson+=" \"sparkProperties\" : { "
    reqJson+=" \"spark.service.tenant_id\" : \"$TENANT_ID\", "
    reqJson+=" \"spark.service.instance_id\" : \"$INSTANCE_ID\", "
    reqJson+=" \"spark.service.tenant_secret\" : \"$TENANT_SECRET\", "
    reqJson+=" \"spark.service.spark_version\" : \"1.6.0\", "
    reqJson+=" \"spark.service.submission_id\" : \"$submissionId\" "
    reqJson+="}"
    reqJson+="}"
    echo $reqJson
}

call_status_REST()
{
    local requestBody=$(status_kill_REST_json)
    logMessage "call_status_REST command : curl --insecure -i -X GET --data-binary \"${requestBody}\" https://$HOSTNAME/v1/submissions/status/${submissionId}"
    local statusRequest="`curl --insecure -i -X GET --data-binary \"${requestBody}\" https://$HOSTNAME/v1/submissions/status/${submissionId}  2>&1`"
    echo "${statusRequest}"
}

call_kill_REST()
{
    local requestBody=$(status_kill_REST_json)
    logMessage "call_kill_REST  command : curl --insecure -i -X POST --data-binary \"${requestBody}\" https://$HOSTNAME/v1/submissions/kill/${submissionId}"
    local killRequest="`curl --insecure -i -X POST --data-binary \"${requestBody}\" https://$HOSTNAME/v1/submissions/kill/${submissionId}  2>&1`"
    echo "${killRequest}"
}


# =============================================================================
# -- Main ---------------------------------------------------------------------
# =============================================================================

trap terminate_spark SIGINT

# -- Parse command line arguments ---------------------------------------------

while [[ $# > 0 ]]
do
    key="$1"
    case $key in
        --help)
            printUsage
            ;;
        --version)
            printVersion
            ;;
        --master)
            MASTER="$2"
            HOSTNAME=$(get_hostname_from_url $MASTER)
            logMessage "MASTER HOSTNAME: ${HOSTNAME}"
            shift
            shift
            ;;
        --jars)
            JARS="$2"
            shift
            shift
            ;;
        --files)
            FILES="$2"
            shift
            shift
            ;;
        --class)
            CLASS="$2"
            shift
            shift
            ;;
        --conf)
            aconf="$2"
            CONF_KEY[$confI]="`echo ${aconf} | sed -n 's/\([^=].*\)=\(.*\)/\1/p'`"
            CONF_VAL[$confI]="`echo ${aconf} | sed -n 's/\([^=].*\)=\(.*\)/\2/p'`"
            ((confI++))
            shift
            shift
            ;;
        --vcap)
            VCAP_FILE="$2"
            shift
            shift
            ;;
        --status)
            CHECK_STATUS=true
            submissionId="$2"
            shift
            shift
            ;;
        --kill)
            KILL_JOB=true
            submissionId="$2"
            shift
            shift
            ;;
        --name)
            APP_NAME="$2"
            shift
            shift
            ;;
        --py-files)
            PY_FILES="$2"
            PY_APP=true
            shift
            shift
            ;;
        --deploy-mode)
            DEPLOY_MODE="$2"
            shift
            shift
            ;;
        *)
            if [[ "$key" =~ ^--.* ]] &&  [[ -z "${APP_MAIN}" ]]; then
                printUsage
            else
                if [ -z "${APP_MAIN}" ]
                then
                    APP_MAIN="$key"
                    shift
                else
                    if [ -z "${app_parms}" ]
                    then
                        app_parms=" \"${key}\" "
                    else
                        app_parms="$app_parms, \"${key}\" "
                    fi
                    shift
                fi
            fi
            ;;
    esac
done

# -- Initialize log file ------------------------------------------------------

if [ "${SS_LOG_ENABLE}" = "true" ]
then
    rm -f ${LOG_FILE}
    echo "To see the log, in another terminal window run the following command:"
    echo "tail -f ${LOG_FILE}"
    echo "Timestamp: ${EXECUTION_TIMESTAMP}"  >> ${LOG_FILE}
    echo "Date: $(date +'%Y-%m-%d %H:%M:%S')" >> ${LOG_FILE}
    echo "VERSION: ${VERSION}" >> ${LOG_FILE}
    echo "Command invocation: ${invokeCommand}" >> ${LOG_FILE}  # Print how the command was invoked
fi


# -- Check variables ----------------------------------------------------------

if [ -z "${MASTER}" ]
then
    echo "ERROR: '--master' is not specified correctly."
    exit 1
fi

if [ -z "${VCAP_FILE}" ]
then
    echo "ERROR: '--vcap' is not specified correctly."
    exit 1
fi

# -- Pull values from VCAP ----------------------------------------------------

logFile $VCAP_FILE

INSTANCE_ID=$(get_from_vcap $VCAP_FILE  "instance_id")
TENANT_ID=$(get_from_vcap $VCAP_FILE  "tenant_id")
TENANT_SECRET=$(get_from_vcap $VCAP_FILE "tenant_secret")
CLUSTER_MASTER_URL=$(get_from_vcap $VCAP_FILE "cluster_master_url")

if [ -z "${TENANT_ID}" ]
then
    echo "ERROR: 'tenant_id' was not retrieved from --vcap $VCAP_FILE."
    exit 1
fi

if [ -z "${TENANT_SECRET}" ]
then
    echo "ERROR: 'tenant_secret' was not retrieved from --vcap $VCAP_FILE."
    exit 1
fi

if [ -z "${INSTANCE_ID}" ]
then
    echo "ERROR: 'instance_id' was not retrieved from --vcap $VCAP_FILE."
    exit 1
fi


if [ -z "${CLUSTER_MASTER_URL}" ]
then
    echo "ERROR: 'cluster_master_url' was not retrieved from --vcap $VCAP_FILE."
    exit 1
fi

vcap_hostname=$(get_hostname_from_url ${CLUSTER_MASTER_URL})
if [ "${HOSTNAME}" != "${vcap_hostname}" ]
then
    echo "ERROR: The URL specified in '--master ${MASTER}' option does not match with the URL in 'cluster_master_url ${CLUSTER_MASTER_URL}' in '--vcap' $VCAP_FILE."
    exit 1
fi

# -- Handle request for status or cancel  -------------------------------------

if [ "${CHECK_STATUS}" = "true" ]
then
    if [ -n "${submissionId}" ]
    then
        echo $(call_status_REST)
        exit 0
    else
        echo "ERROR: You need to specify submission ID after --status option"
        exit 1
    fi
fi

if [ "${KILL_JOB}" = "true" ]
then
    if [ -n "${submissionId}" ]
    then
        echo $(call_kill_REST)
        exit 0
    else
        echo "ERROR: You need to specify submission ID after --kill option"
        exit 1
    fi
fi

# -- Handle request for submit  -----------------------------------------------

if [ -z "${DEPLOY_MODE}" ] || [ "${DEPLOY_MODE}" != "cluster" ]
then
    echo "ERROR: '--deploy-mode' must be set to 'cluster'."
    exit 1
fi

if [ -z "${APP_MAIN}" ]
then
    echo "ERROR: The main application file is not specified correctly."
    exit 1
fi

if [[ "$APP_MAIN" =~ .*\.py ]]; then
    PY_APP=true
fi

if [ -z "${APP_NAME}" ]
then
    if [ -z "${CLASS}" ]
    then
        APP_NAME=${APP_MAIN}
    else
        APP_NAME=${CLASS}
    fi
fi

# -- Synthesize variables -----------------------------------------------------

if [ -z $PREFIX_SERVER_PATH ]; then PREFIX_SERVER_PATH="/gpfs/fs01/user/$TENANT_ID/data"; fi

# -- Prepare remote path and upload files to the remote path ------------------

posixJars=
if [ "${JARS}" ]
then
    if [ "${SS_JARS_UPLOAD}" = "true" ]
    then
        posixJars=$(convert2serverPath_list ${JARS})
        local2server_list ${JARS} ${posixJars}
        #posixJars=$(convert2submitPath_list ${posixJars})
    else
        posixJars="${JARS}"
    fi
fi

posixFiles=
if [ "${FILES}" ]
then
    if [ "${SS_FILES_UPLOAD}" = "true" ]
    then
        posixFiles=$(convert2serverPath_list ${FILES})
        local2server_list ${FILES} ${posixFiles}
    else
        posixFiles="${FILES}"
    fi
fi

posixPYFiles=
if [ "${PY_FILES}" ]
then
    if [ "${SS_FILES_UPLOAD}" = "true" ]
    then
        posixPYFiles=$(convert2serverPath_list ${PY_FILES})
        local2server_list ${PY_FILES} ${posixPYFiles}
    else
        posixPYFiles="${PY_FILES}"
    fi
fi


if [ "${SS_APP_MAIN_UPLOAD}" = "true" ]
then
    app_server_path=$(convert2serverPath $APP_MAIN)
    local2server ${APP_MAIN} ${app_server_path}
    #app_server_path=$(convert2submitPath ${app_server_path})
else
    app_server_path=${APP_MAIN}
fi


# -- Compose spark-submit command ---------------------------------------------

mainClass=$CLASS
if [ "${PY_APP}" = "true" ]
then
    mainClass="org.apache.spark.deploy.PythonRunner"
fi

requestBody=$(submit_REST_json "${app_parms}" "${app_server_path}" "$mainClass" "$posixJars" "$posixFiles" "$posixPYFiles")

# -- Call spark-submit REST to submit the job to spark cluster ---------------------

logMessage "submit REST call:\'curl --insecure  --data-binary \"${requestBody}\" https://$HOSTNAME/v1/submissions/create \'"
resultSubmit="`curl --insecure -i --data-binary \"${requestBody}\" https://$HOSTNAME/v1/submissions/create 2>&1`"

echo $resultSubmit
logMessage "==== Submit output ============================================================"
logMessage "${resultSubmit}"
logMessage "==============================================================================="


# -- Parse submit job output to find 'submissionId' value ---------------------

submissionId="`echo ${resultSubmit} | sed -n 's/.*\"submissionId\" : \"\([^\"]*\)\",.*/\1/p'`"
logMessage  "Submission ID : ${submissionId}"

if [ -z "${submissionId}" ]
then
    logMessage "ERROR: Problem submitting job. Exit"
    endScript
    exit 1
fi

echo "Job Submitted"

# -- Periodically poll job status ---------------------------------------------

driverStatus="NULL"
jobFinished=false
jobFailed=false
try=1
while [[ "${jobFinished}" == false ]] && [[ "$try" < "$RETRY" ]]
do
    logMessage "==== Checking job status, $try try ==============================="
    resultStatus=$(call_status_REST)
    ((try++))
    driverStatus="`echo ${resultStatus} | sed -n 's/.*\"driverState\" : \"\([^\"]*\)\",.*/\1/p'`"
    logMessage "driverStatus is $driverStatus"
    case $driverStatus in
        FINISHED)
            logMessage "${resultStatus}"
            echo "Job Finished"
            jobFinished=true
            ;;
        RUNNING|SUBMITTED)
            sleep $SS_POLL_INTERVAL
            jobFinished=false
            ;;
        *)
            logMessage "==== Failed Status output ====================================================="
            logMessage "${resultStatus}"
            logMessage "==============================================================================="
            jobFinished=true
            jobFailed=true
            ;;
    esac
done

if [ "$try" == "$RETRY" ]
then
    logMessage "INFO: Checking job status, reached to $((--try)) number of times try"
    logMessage "INFO: The current status of $submissionId is $driverStatus"
    logMessage "INFO: Use --status for your submission status of $submissionId"
fi

# -- Download stdout and stderr files -----------------------------------------

if [ -n "${submissionId}" ]
then
    LOCAL_STDOUT_FILENAME="stdout_${EXECUTION_TIMESTAMP}"
    LOCAL_STDERR_FILENAME="stderr_${EXECUTION_TIMESTAMP}"
    stdout_server_path="${SS_SPARK_WORK_DIR}/${submissionId}/stdout"
    server2local ${stdout_server_path} ${LOCAL_STDOUT_FILENAME}
    if [ "$?" == 0 ]
    then
        echo "Done downloading from ${stdout_server_path} to ${LOCAL_STDOUT_FILENAME}"
    else
        echo "Failed to download from ${stdout_server_path} to ${LOCAL_STDOUT_FILENAME}"
    fi

    stderr_server_path="${SS_SPARK_WORK_DIR}/${submissionId}/stderr"
    server2local ${stderr_server_path} ${LOCAL_STDERR_FILENAME}
        if [ "$?" == 0 ]
    then
        echo "Done downloading from ${stderr_server_path} to ${LOCAL_STDERR_FILENAME}"
    else
        echo "Failed to download from ${stderr_server_path} to ${LOCAL_STDERR_FILENAME}"
    fi
fi

# -- Delete transient files on spark cluster ----------------------------------
if [ "${SS_APP_MAIN_UPLOAD}" = "true" ] || [ "${SS_JARS_UPLOAD}" = "true" ] || [ "${SS_FILES_UPLOAD}" = "true" ]
then
    if [ "${jobFinished}" = "true" ]
    then
        deleteFolderOnServer ${SERVER_SUB_DIR}
    fi
fi

# -- Epilog -------------------------------------------------------------------

endScript

# -- --------------------------------------------------------------------------
