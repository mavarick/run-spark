#!/usr/bin/env python
#encoding:utf8

import re
import json
from bs4 import BeautifulSoup as BS

from pyspark import SparkConf, SparkContext

def preprocess(line):
    items = line.strip('\n').strip('\r').split("\t")
    # should be i30 fields
    #(docid,boardid,postid,topicid,userid,nickname,userinfo,title,digest,relatekey,dkeys,
    #    _del,source,uip,ptime,lmodify,buloid,modelid,navtopicid,ispic,iscomment,rkeys,
    #    status,flag,other,extra,mtitle,originalflag,author,body) = items
    docid = items[0] 
    doc_body = items[-1]
    #ptime = items[14]
    # 对文档进行简单统计处理
    #print docid
    body_text, content_info = body_filter(doc_body)
    #body_text= body_filter(doc_body)
    #print content_info, len(body_text)

    # items 用来存放文章的信息
    items[-1] = json.dumps(content_info)
    #return items, docid, body_text
    return '\t'.join(items), '\t'.join([docid, body_text])

def body_filter(doc_body):
    # pre filter
    ## 替换多余的 \n, /br为空
    doc_body = re.sub(r"\\n|</br>", "", doc_body)
    ## 去除多余的br
    doc_body = re.sub(r"<br>\s*(<br>)*\s*<br>", "<br>", doc_body)
    ## br 替换成 p
    doc_body = re.sub(r"<br>", "<p>", doc_body)
    ## 去除多余的p
    doc_body = re.sub(r"<p[^>]*>\s*<p[^>]*>", "<p>", doc_body)
    ## div是否替换成p?
    ## table是否进行统计？
    ## ul/ol是否需要统计

    bs = BS(doc_body, "html.parser")
    # 统计图片数量  => img_infos
    img_nodes = bs.findAll("img")
    img_infos = [getattr(item, "attrs") for item in img_nodes]
    img_infos = [t for t in img_infos if t]

    # 统计p信息 => p_info
    p_nodes = bs.findAll("p")
    p_info = {"count": len(p_nodes)}
    p_cnts = []
    for node in p_nodes:
        #print type(node), len(node.get_text())
        p_cnts.append(len(node.get_text()))
    p_info['word_detail'] = p_cnts
    p_info['word_count'] = sum(p_cnts)
    
    # 统计div信息 => div_info 版块的个数
    div_nodes = bs.findAll("div")
    div_info = {"count": len(div_nodes)}
    div_cnts = []
    for node in div_nodes:
        div_cnts.append(len(node.get_text()))
    div_info['word_detail'] = div_cnts
    div_info['word_count'] = sum(div_cnts)

    # 得到文本信息 => text
    body_text = bs.get_text()

    content_info = {
        "img_infos": img_infos,
        "p_info": p_info,
        "div_info": div_info
    }
    return body_text, content_info
    #return bs.get_text()


def main():
    sc = SparkContext(appName="preprocessing")
    input_file = "warehouse/cms_data/channel=cms_0001/*/*"
    #input_file = "warehouse/cms_data/channel=cms_0001/date=2015-08/cms_0001_2015-08-10.dat"
    #input_file = "warehouse/cms_data/channel=cms_0001/date=2015-08/cms_0001_2015-08.dat"
    #input_file = "liuxufeng/nlp/test_data"
    doc_info_file = "liuxufeng/nlp/doc_info"
    doc_text_file = "liuxufeng/nlp/doc_text"
    docs = sc.textFile(input_file)

    total_info = docs.map(preprocess)
    #total_info.persist()
    doc_info = total_info.map(lambda x: x[0])
    doc_text = total_info.map(lambda x: x[1])

    #total_info.saveAsTextFile(doc_info_file)
    doc_info.saveAsTextFile(doc_info_file)
    doc_text.saveAsTextFile(doc_text_file)
    
if __name__ == "__main__":
    main()


