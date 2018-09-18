#!/bin/bash
source /home/ndir/.bash_profile
source /home/ndir/.bashrc
job_date=$(date "+%Y-%m-%d")

echo $(date)
delete_ds=`date --date='4 days ago' +%Y-%m-%d`
rm -rf /home/ndir/litao/model/'${delete_ds}'
train_data_path='/home/ndir/litao/gensim/data/pre1/'
rm -rf $train_data_path
./wait_hdfs_today_file.sh "music_recommend/event/sns/v1/followbased/cf/pre1/_SUCCESS"
hadoop fs -copyToLocal "music_recommend/event/sns/v1/followbased/cf/pre1" /home/ndir/litao/gensim/data/
model_path='/home/ndir/litao/model/'${job_date}''
mkdir -p $model_path

vector_path='/home/ndir/litao/vec/'${job_date}''
mkdir -p $vector_path

echo "train data : $train_data_path"
echo "model path: $model_path"
echo "vec path $vector_path"
echo "begin to train: ${job_date}"
/home/ndir/litao/code/pkg/bin/python node2vec_train.py $train_data_path $model_path/node2vec_model $vector_path/node_vec_ > train.log

echo " train result: $?"
echo "end time: $(date)"
