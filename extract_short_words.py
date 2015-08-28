#!/usr/bin/env python
#encoding:utf8

''' this file is mainly do extracting stuff for new short sentences and words
'''

import re
import pdb
from pyspark import SparkConf, SparkContext

def map_extract_short_words(line):
    items = line.strip('\n').split("\t")
    if len(items) < 2:
        return "None"
    doc_id, doc_text = items
    results = extract_short_words(doc_text)
    return "%s\t%s"%(doc_id, '\t'.join(results))

patterns = [ 
    # (pattern, flag, loc)  # comment
    (ur"《([^》]{1,20})》", 'b', 0),  # for book name
    (ur"(\"|\')([\u4E00-\u9FA5]{1,4})\1", 'q', 1), # for quota name
    (ur"(?<=[^\u4E00-\u9FA5])([\u4E00-\u9FA5]{1,5})[^\u4E00-\u9FA5]", 's', 0),  # for short name
    (ur"(?<=[^\u4E00-\u9FA5])([\u4E00-\u9FA5]{1,5})$", 's', 0),                 # tail substring
    (ur"^([\u4E00-\u9FA5]{1,5})[^\u4E00-\u9FA5]", 's', 0),                       # fore substrng
]

def extract_short_words(doc):
    results = []
    for pattern_set in patterns:
        pattern, flag, loc = pattern_set
        matches = re.findall(pattern, doc)
        for item in matches:
            if type(item) in [list, tuple, set]:
                item = item[loc]
            try:
                results.append("/".join([item, flag]))
            except:
                continue
    return results

def main():
    sc = SparkContext(appName="preprocessing")
    #input_file = "liuxufeng/nlp/doc_text/part-00039" # TODO
    input_file = "liuxufeng/nlp/doc_text/*" # TODO
    output_file = "liuxufeng/nlp/doc_short_word"  # TODO

    # handling
    rdd_input = sc.textFile(input_file)
    rdd_words = rdd_input.map(map_extract_short_words) 
    rdd_words.saveAsTextFile(output_file)

def local_test(input_file, *funcs):
    funcs = list(funcs)
    funcs.reverse()
    for line in open(input_file):
        _line = line
        for func in funcs:
            _line = func(_line)
        print _line.encode("utf8")
        

if __name__ == "__main__":
    main()
    #local_test("extract_short_words.dat", map_extract_short_words)

