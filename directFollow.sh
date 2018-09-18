#!/bin/bash
shopt -s expand_aliases
source /home/ndir/.bash_profile

dateStr=`date +%Y-%m-%d`

Music_VideoRcmdMeta=/db_dump/music_ndir/Music_VideoRcmdMeta
whitelist_user=music_recommend/event/sns/v1/followbased/node2vec/res_line/
output=/user/ndir/music_recommend/feed_video/follow/als_direct_follow_triple.all

hadoop fs -rm -r $output

spark-submit-2 \
--class com.netease.music.recommend.scala.feedflow.follow.Follow2Triple \
--master yarn \
--deploy-mode cluster \
--executor-memory 10g \
--driver-memory 8g \
--name "feed_follow" \
--queue music_feed \
--conf spark.default.parallelism=1000 \
--conf spark.sql.shuffle.partitions=1000 \
--conf spark.rpc.askTimeout=1200s \
FeedFlowSpark-1.0-SNAPSHOT.jar \
-Music_Follow /db_dump/music_ndir/Music_Follow \
-Music_VideoRcmdMeta $Music_VideoRcmdMeta \
-whitelist_user $whitelist_user \
-output $output

#if [ $? -eq 0 ]
#then
#  hadoop fs -cat $output/* | awk -F "\t" '{print $1"_eventRecommendUser\t"$2}' > /data/ddb_input/recommendUser.$dateStr
#  touch /data/ddb_input/recommendUser.$dateStr.success
#fi
