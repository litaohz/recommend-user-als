#! /home/ndir/qianwei/anaconda3/bin/python
# coding: utf-8

import gensim
import codecs
from gensim import models
import os
import random
import sys
import multiprocessing

def predict(model_path, predict_path,output_path):
    model = gensim.models.Word2Vec.load(model_path)
    file_list = [predict_path + name for name in os.listdir(predict_path) if name.startswith("part")]
    f = codecs.open(output_path, mode="wb", encoding="utf-8")
    for file in file_list:
        for line in codecs.open(file, encoding="utf-8"):
            puid=line.strip()
            try:
                f.write(puid + '\t' + 'cf2:' +  ','.join(map (lambda x: x[0], model.most_similar([puid], topn=10))) + '\n')
            except: KeyError 
    f.close()
    
if __name__ == "__main__":
    model_path = sys.argv[1]
    predict_path = sys.argv[2]
    output_path = sys.argv[3]
    predict(model_path, predict_path,output_path)
    
