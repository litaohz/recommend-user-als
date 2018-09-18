#! /bin/bash
source /home/ndir/.bash_profile
date_str=`date  +%Y-%m-%d`
while(true)
do
  echo -e "waiting for today's hdfs file:\t$1"
  file_create_date=`hdfs dfs -ls $1 | tail -n 1 | awk '{if(NF>=6) print $6}'`
  #echo $file_create_date, $date_str
  if [ ${date_str}x = ${file_create_date}x ];
    then
      break
  fi  
  date_str_new=`date  +%Y-%m-%d`
  if [ ${date_str_new} != ${date_str} ]
  then
    exit 1
  fi  
  sleep 66
done
echo -e "find today's hdfs file:\t$1"
exit 0

