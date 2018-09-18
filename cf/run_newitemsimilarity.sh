shopt -s expand_aliases
source /home/ndir/.bash_profile
source /home/ndir/zhaobin/env.sh
source $1

date_yesterday=`date -d "1 day ago" +%Y-%m-%d`
/home/ndir/zhaobin/music/wait_hdfs_file.sh $input/_SUCCESS

hadoop fs -rmr $tempDir 
hadoop fs -rmr $output
hadoop jar mahout-examples-new-1.0-SNAPSHOT-job.jar org.apache.mahout.cf.taste.hadoop.similarity.item.ItemSimilarityJob \
-Dmapreduce.map.java.opts="-Xmx4500M -XX:+UseSerialGC" \
-Dmapreduce.map.memory.mb=5000 \
-Dmapreduce.task.timeout=6600000 \
-Ddfs.socket.timeout=6600000 -Dmapred.job.queue.name=music_feed -Dmapred.reduce.tasks=$reduceTasks \
--input $input  --output $output --similarityClassname $similarityClassname --tempDir $tempDir -mp $minPrefsPerUser  -m $maxSimilaritiesPerItem  --maxPrefs  $maxPrefsPerUser -tr $tr --booleanData $booleanData --minSupport $minSupport 


<<!
hdfs dfs -rm -r $filterInput
spark-submit-2 \
--class com.netease.music.recommend.scala.feedflow.userbased.GetMeaningfulPairs \
--master yarn \
--deploy-mode cluster \
--name "GetMeaningfulPairs" \
--queue music_feed \
--num-executors 88 \
../FeedFlowSpark-1.0-SNAPSHOT.jar \
-inputPairs $output \
-simThread 0.01 \
-output $filterInput \


hadoop fs -rmr $filter
hadoop jar /data/music_useraction/itembased/itembased.jar RowItembasedResult -Dmapred.job.queue.name=music_feed  -Dmapred.reduce.tasks=$reduceTasks  -Dmapred.child.java.opts=1024M --input  $filterInput --output $filter 
!
