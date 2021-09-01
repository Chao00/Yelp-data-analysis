import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import re
import gzip, os, uuid
import json
from datetime import datetime
from pyspark import SparkConf, SparkContext
from pyspark.sql import Window
from pyspark.sql import SparkSession, functions, types

geojsonSchema = types.StructType([
	types.StructField('type', types.StringType()),
    types.StructField('properties', 
        types.StructType([
            types.StructField('business_id', types.StringType()),
            types.StructField('name', types.StringType()),
            types.StructField('categories', types.StringType()),
            types.StructField('state', types.StringType()),
            types.StructField('city', types.StringType()),
            types.StructField('review_count', types.IntegerType()),
            types.StructField('rank', types.IntegerType()),
            types.StructField('stars', types.DoubleType()),
            ])),
    types.StructField('geometry',
        types.StructType([
            types.StructField('coordinates', types.ArrayType(types.DoubleType())),
            types.StructField('type', types.StringType()),
            ])),
    ])


def to_geojson(row):
	type_item = 'Feature'
	properties = (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7])
	geometry = ([row[8], row[9]], 'Point')
	return (type_item, properties, geometry)

def addkey(val):
    return("features", val)
def addval(val):
    return(val, "FeatureCollection")
def combine_val(paira, pairb):
    return (paira+pairb)


def main(most_review_city, output):
    # main logic starts here
    most_review = spark.read.json(most_review_city)
    
    most_review.createOrReplaceTempView('review_tv')
    geojson = spark.sql('SELECT business_id, name, categories, state, city, review_count, rank, stars, longitude, latitude FROM review_tv')
    
    geojson_rdd = geojson.rdd.map(to_geojson)

    geojson_df = geojson_rdd.toDF(geojsonSchema)

    geojson_df.write.json(output, mode='overwrite')

if __name__ == '__main__':
    most_review_city = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('modify json').getOrCreate()
    assert spark.version >= '2.4' # make sure we have Spark 2.4+
    spark.sparkContext.setLogLevel('WARN')
    main(most_review_city, output)


