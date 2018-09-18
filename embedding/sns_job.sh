#! /bin/bash
source /home/ndir/.bash_profile
source /home/ndir/.bashrc

./train_model.sh
echo "sns-sim-art-train-result: $?"
./predict.sh
echo "sns-sim-art-predict-result: $?"
