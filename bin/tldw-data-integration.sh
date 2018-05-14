#!/usr/bin/bash

PID_FILE=/var/tmp/tldw-data-integration.pid

RETVAL=0

#HEAP
JVM_OPTS="$JVM_OPTS -Xms4096M"
JVM_OPTS="$JVM_OPTS -Xmx4096M"
#GC tuning options
JVM_OPTS="$JVM_OPTS -XX:+HeapDumpOnOutOfMemoryError"
#GC logging options
JVM_OPTS="$JVM_OPTS -XX:+PrintGCDetails"
JVM_OPTS="$JVM_OPTS -XX:+PrintGCDateStamps"
JVM_OPTS="$JVM_OPTS -XX:+PrintHeapAtGC"
JVM_OPTS="$JVM_OPTS -XX:+UseParNewGC"
JVM_OPTS="$JVM_OPTS -XX:+UseConcMarkSweepGC"
JVM_OPTS="$JVM_OPTS -XX:CMSInitiatingOccupancyFraction=75"
JVM_OPTS="$JVM_OPTS -XX:+UseCMSInitiatingOccupancyOnly"

JVM_OPTS="$JVM_OPTS -Xloggc:log/gc/gc.log"
JVM_OPTS="$JVM_OPTS -XX:+UseGCLogFileRotation"
JVM_OPTS="$JVM_OPTS -XX:NumberOfGCLogFiles=10"
JVM_OPTS="$JVM_OPTS -XX:GCLogFileSize=10M"

#JMX related
JMX_OPTS="-Dcom.sun.management.jmxremote"
JMX_OPTS="$JMX_OPTS -Dcom.sun.management.jmxremote.port=8011"
JMX_OPTS="$JMX_OPTS -Dcom.sun.management.jmxremote.local.only=false"
JMX_OPTS="$JMX_OPTS -Dcom.sun.management.jmxremote.authenticate=false"
JMX_OPTS="$JMX_OPTS -Dcom.sun.management.jmxremote.ssl=false"


current_dir=`dirname $0`
. ${current_dir}/env.sh

start()
{

  pid=`ps aux |grep "[^]]java.*data-integration.*.jar"|  awk '{print $2}'`
  if [ ! -z "$pid" ]
  then
    echo "Found existing service with process id $pid, will not start new"
    exit 1;
  fi
  echo -n $"Starting  data-integration : "
  nohup $JAVA_HOME/bin/java $JVM_OPTS -classpath data-integration-*.jar $JMX_OPTS -Dlogging.config=./config/logback-spring.xml -Djava.ext.dirs=lib/ com.zqykj.tldw.ApplicationServer 2>&1 &
  pid=$!
  RETVAL=$?
  [ $RETVAL -eq 0 ] && echo "$pid" > $PID_FILE
}

stop()
{
  # kill existing process if available
  echo -n $"Stopping data-integration: "
  if test -e "$PID_FILE"; then
    pid=`cat $PID_FILE`
    echo "Kill process $pid from pid file"
    kill -9 $pid
    RETVAL=0
    rm -f $PID_FILE
  else
    pid=`ps aux |grep "[^]]java.*data-integration.*.jar"|  awk '{print $2}'`
    if [ ! -z "$pid" ]
    then
      echo "Kill process $pid found in system"
      kill -9 $pid
      RETVAL=$?
    fi
  fi
}

restart () {
  stop
  sleep 3
  start
}


case "$1" in
  start)
    start
    ;;
  stop)
    stop
    ;;
  restart|reload|force-reload)
    restart
    ;;
  status)
    status $mongod
    RETVAL=$?
    ;;
  *)
    echo "Usage: $0 {start|stop|restart}"
    RETVAL=1
esac

exit $RETVAL
