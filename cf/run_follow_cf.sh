#!/bin/bash

#基于评论/关注/关注和评论的协同过滤
/home/ndir/zhaobin/music/wait_hdfs_file_today.sh  music_recommend/event/sns/v1/followbased/cf/pre/_SUCCESS
bash -x run_newitemsimilarity.sh follow_cf.conf
hadoop fs -rmr music_recommend/event/sns/v1/followbased/cf/tmp
