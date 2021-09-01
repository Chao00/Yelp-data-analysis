from pyspark import SparkConf, SparkContext
import sys
assert sys.version_info >= (3, 5)
import json
from pyspark.sql import SparkSession, functions, types

def parse_json(line):
    json_obj = json.loads(line)
    return {'business_id': json_obj['business_id'], 'name': json_obj['name'], 'stars': json_obj['stars'], 'review_count': json_obj['review_count'], 
    'latitude': json_obj['latitude'], 'longitude' : json_obj['longitude'], 'city': json_obj['city']}

def main(inputs, output):
    text = spark.read.json(inputs)
    business = text.select(text.name,text.stars,text.review_count,text.latitude, text.longitude, text.city)
    las_vegas = business.filter(business.city == 'Las Vegas')
    las_vegas.coalesce(1).write.csv(output,mode='overwrite')


if __name__ == '__main__':
    conf = SparkConf().setAppName('las_vegas_filter')
    sc = SparkContext(conf=conf)
    spark = SparkSession.builder.appName('las_vegas_filter').getOrCreate()
    sc.setLogLevel('WARN')
    assert sc.version >= '2.4' 
    inputs = sys.argv[1]
    output = sys.argv[2]