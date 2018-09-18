#!/bin/bash
shopt -s expand_aliases
source /home/ndir/.bash_profile

dateStr=`date +%Y-%m-%d`
input=/user/ndir/music_recommend/feed_video/follow/als_direct_follow_triple.all/data
input_userIdMapping=/user/ndir/music_recommend/feed_video/follow/als_direct_follow_triple.all/userIdMapping
input_itemMapping=/user/ndir/music_recommend/feed_video/follow/als_direct_follow_triple.all/friendMapping
output=/user/ndir/music_recommend/feed_video/follow/als_direct_follow_model.all


hadoop fs -rm -r $output

spark-submit-2 \
--class com.netease.music.recommend.scala.feedflow.follow.AlsFollowData \
--master yarn \
--deploy-mode cluster \
--executor-memory 15g \
--driver-memory 2g \
--name "feed_follow" \
--queue music_feed \
--conf spark.default.parallelism=1000 \
--conf spark.sql.shuffle.partitions=1000 \
FeedFlowSpark-1.0-SNAPSHOT.jar \
-input $input \
-input_userIdMapping $input_userIdMapping \
-input_itemMapping $input_itemMapping \
-output $output

#if [ $? -eq 0 ]
#then
#  hadoop fs -cat $output/* | awk -F "\t" '{print $1"_eventRecommendUser\t"$2}' > /data/ddb_input/recommendUser.$dateStr
#  touch /data/ddb_input/recommendUser.$dateStr.success
#fi
