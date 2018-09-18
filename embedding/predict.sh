#! /bin/bash
source /home/ndir/.bash_profile
source /home/ndir/.bashrc
job_date=$(date "+%Y-%m-%d")
date_7dayago=`date -d "7 day ago" +%Y-%m-%d`
model_path="/home/ndir/litao/model/$job_date/node2vec_model"
echo "now: $job_date"
auth_path="/home/ndir/litao/gensim/data/auth/"
output="/home/ndir/litao/gensim/data/output/predict.txt"
mkdir - p "/home/ndir/litao/gensim/data/output/"
touch  $output
rm $output
rm -rf "$auth_path"
mkdir -p "$auth_path"
hadoop fs -copyToLocal  "music_recommend/event/sns/v1/followbased/dim/auth/*"  $auth_path

echo "begin predict, model path, date $model_path, $job_date"

/home/ndir/litao/code/pkg/bin/python  predict.py $model_path  $auth_path $output > predict.log

echo "predict result: $?"
hadoop fs -rm music_recommend/event/sns/v1/followbased/node2vec/res_line/predict.txt
hadoop fs -put $output music_recommend/event/sns/v1/followbased/node2vec/res_line/
