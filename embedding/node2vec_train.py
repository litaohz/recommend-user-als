#!/home/ndir/qianwei/anaconda3/bin/python
# coding: utf-8
#import pdb
import gensim
import codecs
from gensim import models
import os
import random
import sys
import multiprocessing

class MySentences(object):
    def __init__(self, file_name):
        self.dir_name = file_name

    def __iter__(self):
        for line in codecs.open(self.dir_name, encoding="utf-8"):
            l = line.strip().split()
            yield l


class MySentencesDir(object):
    def __init__(self, dir_name):
        self.dir_name = dir_name

    def __iter__(self):
        file_list = [self.dir_name + name for name in os.listdir(self.dir_name) if name.startswith("part")]
        for paths in file_list:
            print(paths)
            for line in codecs.open(paths, encoding="utf-8"):
                l = line.strip().split()
                yield l


class MySentencesDirRandom(object):
    
    def __init__(self, dir_name, choice_num):
        self.dir_name = dir_name
        self.choice_num = choice_num

    def __iter__(self):
        file_list = [self.dir_name + name for name in os.listdir(self.dir_name) if name.startswith("part")]
        file_list = random.sample(file_list, self.choice_num)
        for paths in file_list:
            print(paths)
            for line in codecs.open(paths, encoding="utf-8"):
                l = line.strip().split()
                yield l


def worker(arg_satrt_end, model, file_name_p):
    
    file_name = file_name_p + "_v_" + str(arg_satrt_end[0])
 #   pdb.set_trace()
    f = codecs.open(file_name, mode="wb", encoding="utf-8")
    
    for i in range(arg_satrt_end[0], arg_satrt_end[1]):
        word = model.wv.index2word[i]
        vector = str(list(model[word])).replace("[", "").replace("]", "").replace(" ", "")
        f.write(word + "\t" + vector + "\n")
    
    f.close()


def get_vector_of_word(model, thread_num, file_name_pre):

    vocab_size = model.wv.syn0.shape[0]
  #  pdb.set_trace()
    each_length = int(vocab_size / thread_num) + 1
    
    l = []
    for i in range(thread_num + 10):
        start = i * each_length
        last = min(vocab_size, (i + 1) * each_length)
        print(start, last)
        l.append((start, last))
        if last == vocab_size:
            break
    print(l)
    
    for element in l:
        p = multiprocessing.Process(target = worker, args = (element, model, file_name_pre))
        p.start()






if __name__ == "__main__":
    
    train_data_path = sys.argv[1]
    model_path = sys.argv[2]
    vector_path =  sys.argv[3]
    print(train_data_path)
    print(model_path)
    print(vector_path)

    # 读取句子
    my_sentence = MySentencesDir(train_data_path)

    # 训练模型
    model = gensim.models.Word2Vec(my_sentence, size=100, iter=10, window=5, min_count=2, workers=12)

    # 保存模型
    model.save(model_path)

    # 保存向量
    get_vector_of_word(model, 10, vector_path)
    pass

