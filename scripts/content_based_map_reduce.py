from pyspark import SparkContext, SparkConf
import itertools
import numpy as np
import math
import sys
import logging

logging.basicConfig(level=logging.DEBUG)

def mapper1(line):
    line = line.strip()
    elements = line.split(",")
    user_id = elements[0]
    item_pair = (elements[1], elements[2])
    return user_id, item_pair

def mapper2(input_item):
    user_id, tuple_list = input_item
    result = []

    for tuple_1, tuple_2 in itertools.combinations(tuple_list, r=2):
        movie_id_1, rating_1 = tuple_1
        movie_id_2, rating_2 = tuple_2
        # sort tuple list by movie_id
        # making sure (movie_id_1, movie_id_2) pairs are identical
        if movie_id_1 < movie_id_2:
            key = (movie_id_1, movie_id_2)
            value = (rating_1, rating_2)
        else:
            key = (movie_id_2, movie_id_1)
            value = (rating_2, rating_1)
        result.append((key, value))
    return result


def mapper4(input_item):
    key, value = input_item
    item_x, item_y = zip(*value)
    item_x = np.array(list(map(int, item_x)))
    item_y = np.array(list(map(int, item_y)))
    corelate = np.corrcoef(item_x, item_y)[0, 1]
    if len(item_x) < 2 and len(item_y) < 2:
        return (key, 0)
    elif np.array_equal(item_x, item_y):
        return (key, 0)
    elif math.isnan(corelate):
        return (key, 0)
    else:
        return (key, corelate)

    # Or perform checking after corrcoef:
    # if (math.isnan(np.corrcoef(item_x, item_y)[0, 1])):
    #     print item_x
    #     print item_y
    #     sys.exit(0)


conf = SparkConf().setAll([
    ('spark.executor.memory', '20g'), 
    ('spark.executor.cores', '6'), 
    ('spark.cores.max', '12'), 
    ('spark.driver.memory','40g'),
    ('spark.driver.cores', '6'), 
])

sc = SparkContext(conf=conf)
sc.getConf().getAll()

file_path = 'CHANGEME'
text_file = sc.textFile(file_path)
mapReduce1 = text_file \
            .map(mapper1) \
            .groupByKey() \
            .mapValues(list) \
            .flatMap(mapper2)\
            .groupByKey() \
            .mapValues(list)\
            .map(mapper4)

mapReduce1.saveAsTextFile("../output")
