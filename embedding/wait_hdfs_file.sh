#! /bin/bash
source /home/ndir/.bash_profile
date_str=`date  +%Y-%m-%d`
while(true)
do
  echo -e "waiting for hdfs file:\t$1"
    hdfs dfs -ls $1
      if [ $? -eq 0 ];
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
                                echo -e "find hdfs file:\t$1"
                                exit 0

