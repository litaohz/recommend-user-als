#!/usr/bin/env bash
source /home/ndir/.bash_profile
source /home/ndir/.bashrc
date_yesterday=`date -d "1 day ago" +%Y-%m-%d`
hadoop fs -mkdir music_recommend/event/frontend/output_biz/ds=$date_yesterday
