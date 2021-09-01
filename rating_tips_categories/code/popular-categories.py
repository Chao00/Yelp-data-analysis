#Most popular Restaurants categories
from pyspark import SparkConf, SparkContext
import sys
assert sys.version_info >= (3, 5)
import json
from pyspark.sql import SparkSession, functions, types

def parse_json(line):
    json_obj = json.loads(line)
    return json_obj['categories'].split(', ')

def filter_json(element):
    if element == "Restaurants" or element == "Food":
        return False
    return True

def words_once(word):
    return (word, 1)

def add(x, y):
    return x + y

def get_val(kv):
    return kv[1]

def output_format(kv):
    k, v = kv
    return '%s %i' % (k, v)

def main(inputs, output):
    categories_schema = types.StructType([
    types.StructField('name', types.StringType(), True),
    types.StructField('count', types.IntegerType(), True)
])
    text = sc.textFile(inputs)
    data = text.flatMap(parse_json).filter(filter_json).map(words_once).filter(lambda s: len(s[0]) > 0)
    wordcount = data.reduceByKey(add)
    res = wordcount.sortBy(get_val, ascending=False)
    df = sqlContext.createDataFrame(res, categories_schema)
    df.coalesce(1).write.format('com.databricks.spark.csv').options(header='true').save(output)


if __name__ == '__main__':
    conf = SparkConf().setAppName('popular-categories')
    sc = SparkContext(conf=conf)
    spark = SparkSession.builder.appName('popular-categories').getOrCreate()
    sc.setLogLevel('WARN')
    assert sc.version >= '2.4' 
    inputs = sys.argv[1]
    output = sys.argv[2]