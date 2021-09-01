import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import re
import gzip, os, uuid
import json
from datetime import datetime
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions, types

def catering_filter(categories_rdd):
    bid, category = categories_rdd
    categories = str(category).split(", ")
    catering = ["Food", "Restaurants", "Nightlife"]
    for c in categories:
        if c in catering:
            return True

def main(business, output):
    # main logic starts here
    business = spark.read.json(business)
    
    business.createOrReplaceTempView("business_tv")
    id_category = spark.sql('SELECT business_id, categories FROM business_tv')
    category_rdd = id_category.rdd.map(tuple)
    category_rdd = category_rdd.filter(catering_filter)
    
    categorySchema = types.StructType([
        types.StructField('business_id', types.StringType()),
        types.StructField('categories', types.StringType()),
    ])
    category_df = spark.createDataFrame(data=category_rdd, schema = categorySchema)
    category_df.createOrReplaceTempView("category_tv")

    catering = spark.sql("SELECT b.* FROM business_tv b JOIN category_tv c ON (b.business_id = c.business_id)").cache()

    catering.write.json(output, mode='overwrite')
    #result.explain()

if __name__ == '__main__':
    business = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('catering filter').getOrCreate()
    assert spark.version >= '2.4' # make sure we have Spark 2.4+
    spark.sparkContext.setLogLevel('WARN')
    main(business, output)


