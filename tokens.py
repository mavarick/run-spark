#!/usr/bin/env python
#encoding:utf8

import jieba
import jieba.posseg as pseg

jieba.initialize(dictionary="dict.txt")

from pyspark import SparkContext

def tokenize(text):
    docid, body = text.split('\t', 1)
    items = []
    for word,flag in pseg.cut(body):
        items.append('%s/%s'%(word,flag))
    result = "%s\t%s"%(docid, ' '.join(items))
    return result

if __name__ == "__main__":
    sc =SparkContext(appName="Python Tokens")
    #input_file = 'liuxufeng/nlp/doc_text/part-00040'
    input_file = 'liuxufeng/nlp/doc_text/*'
    bodies = sc.textFile(input_file)
    items = bodies.map(tokenize) #.collect()
    #for item in items:
    #    print item.encode("utf8")
    items.saveAsTextFile("liuxufeng/nlp/doc_tokens")
    sc.stop()
