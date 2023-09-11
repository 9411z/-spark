#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@Author: zhoujin10@baidu.com
@Date: 2023-08-23
@Filename: monogatary.py
@Desc: 数据预处理入口文件
"""
import json
import time
import locale
from pyspark import *
import base64
from pyspark.sql import SparkSession
# from pyspark.sql.functions import col
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import argparse
# from filter import filter_emoji
# from tools import count_tokens
# from tools import filter_normalizer_sentence
# from tokenizer import Tokenizer
import mmh3
import sudachipy




def read_data(sc, paths, fields):
    """
    解析输入数据
    :param sc:
    :param paths:
    :param fields:
    :return:
    """

    def parse(line):
        """
            Parse tsv doc
            :param line:
            :return:
        """

        def calculate_minhash(text):
            features = tokenize_text(text)
            hashes = [mmh3.hash(feature.surface()) for feature in features]
            minhash = min(hashes)
            return minhash

        # def tokenize_text(text):
        #     tokenizer = sudachipy.Tokenizer()
        #     tokenizer = sudachipy.Dictionary().create()
        #     tokens = tokenizer.tokenize(text)
        #     # tokenizer = sudachipy.Tokenizer()
        #     # # 分词操作
        #     # tokens = tokenizer.tokenize(text)
        #     return tokens
        def tokenize_text(text):
            tokenizer = sudachipy.Dictionary().create()
            tokens = tokenizer.tokenize(text)
            return tokens
        try:
            row = json.loads(line)
            lang = 'ja'
            category = row["category"]
            url = row["text_url"]
            title = row["title"]
            text = row["text"]
            minhash = calculate_minhash(text)
            print("first minhash:", minhash)

            if minhash == "":
                minhash = 111
                print("second minhash:", minhash)
            time_stamp = row["time_stamp"]
            author = row['author_name']
            article_views = ''
            up_vote = ''
            comment_cnt = row['comment_cnt']
            return [lang, category, url, title, text, time_stamp, author, article_views, up_vote, comment_cnt, minhash]

        except Exception as e:
            # return tuple([None] * 3 + [e.message])
            return None

    # def format(line, fields):
    #     """
    #     ouput format
    #     :param line:
    #     :param fields:
    #     :return:
    #     """
    #     sf = dict(zip(fields, line))
    #     # 计算正文文本的token数
    #     sf['token_num'] = 10
    #     result = json.dumps(sf, ensure_ascii=False).encode('utf-8')
    #     return result.decode('utf-8')
    def format(line, fields):
        """
        ouput format
        :param line:
        :param fields:
        :return:
        """
        sf = dict(zip(fields, line))
        # 计算正文文本的token数
        sf['token_num'] = 10
        return sf

    df = sc.textFile(paths).map(lambda x: parse(x)) \
        .filter(lambda x: x is not None) \
        .map(lambda x: format(x, fields))
    return df

def remove_duplicates(data):
    window_spec = Window.partitionBy("minhash").orderBy("time_stamp")
    data_with_rownum = data.withColumn("rownum", row_number().over(window_spec))
    filtered_data = data_with_rownum.filter(data_with_rownum.rownum == 1)
    return filtered_data
def run(spark, input_path, output_path):
    """
    主函数入口
    :param spark:
    :param input_path:
    :param output_path:
    :return:
    """
    # fields = ["lang", "category", "url", "title", "text", "time_stamp", "author",
    #           "article_views", "up_vote", "comment_cnt"]
    fields = ["lang", "category", "url", "title", "text", "time_stamp", "author",
              "article_views", "up_vote", "comment_cnt", "minhash"]
    data = read_data(spark.sparkContext, "./a.txt", fields)


    print(data.count())
    print("!!!!!!!!!!!!")
    print(data.takeSample(False, 10))
    with open("output.txt", "w", encoding="utf-8") as f:
        for row in data.collect():
            sample = json.dumps(row, ensure_ascii=False)
            f.write(sample + "\n")
    print("===================")
    data = data.toDF()
    data = remove_duplicates(data)
    data = data.drop("minhash")
    # 保存到文件
    data.write.mode("overwrite").json("./de")

    # data.saveAsTextFile(output_path)
# def calculate_minhash(text):
#     features = extract_features(text)
#     hashes = [mmh3.hash(feature) for feature in features]
#     minhash = min(hashes)
#     return minhash
#
# def extract_features(text):
#     tokenizer_obj = tokenizer.Tokenizer()
#     # tokenizer = Tokenizer()
#     words = [token.surface for token in tokenizer_obj.tokenize(text)]
#     return words

if __name__ == "__main__":
    # parser = argparse.ArgumentParser()
    # parser.add_argument("--input_path", type=str)
    # parser.add_argument("--output_path", type=str)
    # args = parser.parse_args()

    spark = SparkSession.builder \
        .appName("test") \
        .config("spark.master", "local") \
        .getOrCreate()
    input_path = "./a.txt"
    out_path = "./b.txt"
    run(spark, input_path, out_path)

    # def calculate_minhash(text):
    #     features = extract_features(text)
    #     hashes = [mmh3.hash(feature) for feature in features]
    #     minhash = min(hashes)
    #     return minhash
    #
    #
    # def extract_features(text):
    #     tokenizer_obj = Tokenizer()
    #     words = [token.surface for token in tokenizer_obj.tokenize(text)]
    #     return words

    # text = "人生は迷路だ\n\n上り坂や下り坂、落とし穴まである\n\n悪戦苦闘して、色々と学びながらゴールを目指している\n\n一本道やわかりやすい道じゃつまらない\n\n本や先輩のアドバイスは迷路を攻略するのに役に立つ\n\n自分の考えで突破するもいい\n\n助けをもらってもいい\n\nでも自分の人生だ\n\n悩んで、喜んで、落ち込んで、はい上がったらいい\n\nその先にはいい人生が待っている\n\n迷路を楽しむんだ"
    #
    # minhash = calculate_minhash(text)
    # print("minhash:", minhash)

    #
    # minhash_val = calculate_minhash(text)
    # print("minhash:", minhash_val)
    # import mmh3
    # from tokenizer import Tokenizer
    # def tokenize_text(text):
    #     tokenizer = sudachipy.tokenizer.Tokenizer()
    #
    #     # tokenizer = sudachipy.Dictionary().create()
    #     tokens = tokenizer.tokenize(text)
    #     print("tokens:", tokens)
    #     return tokens
# def tokenize_text(text):
#     tokenizer = sudachipy.tokenizer.T
#     tokens = tokenizer.tokenize(text)
#     morphemes = [token.surface() for token in tokens]
#     return morphemes

# if __name__ == "__main__":
#     text = "人生は迷路だ\n\n上り坂や下り坂、落とし穴まである\n\n悪戦苦闘して、色々と学びながらゴールを目指している\n\n一本道やわかりやすい道じゃつまらない\n\n本や先輩のアドバイスは迷路を攻略するのに役に立つ\n\n自分の考えで突破するもいい\n\n助けをもらってもいい\n\nでも自分の人生だ\n\n悩んで、喜んで、落ち込んで、はい上がったらいい\n\nその先にはいい人生が待っている\n\n迷路を楽しむんだ"
#
#     morphemes = tokenize_text(text)
#     for morpheme in morphemes:
#         print(morpheme)
#

