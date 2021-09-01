from pyspark.sql import SparkSession, functions, types
import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

def main(inputs, output):

    comments_schema = types.StructType([
        types.StructField('user_id', types.StringType()),
        types.StructField('name', types.StringType()),
        types.StructField('review_count', types.LongType()),
        types.StructField('yelping_since', types.StringType()),
        types.StructField('useful', types.LongType()),
        types.StructField('funny', types.LongType()),
        types.StructField('cool', types.LongType()),
        types.StructField('elite', types.StringType()),
        types.StructField('friends', types.StringType()),
        types.StructField('fans', types.LongType()),
        types.StructField('average_stars', types.FloatType()),
        types.StructField('compliment_hot', types.LongType()),
        types.StructField('compliment_more', types.LongType()),
        types.StructField('compliment_profile', types.LongType()),
        types.StructField('compliment_cute', types.LongType()),
        types.StructField('compliment_list', types.LongType()),
        types.StructField('compliment_note', types.LongType()),
        types.StructField('compliment_plain', types.LongType()),
        types.StructField('compliment_cool', types.LongType()),
        types.StructField('compliment_funny', types.LongType()),
        types.StructField('compliment_writer', types.LongType()),
        types.StructField('compliment_photos', types.LongType()),
    ])

    user_data = spark.read.json(inputs, schema=comments_schema)

    user_data = user_data.select(user_data['user_id'], 
                                 user_data['name'], 
                                 user_data['review_count'], 
                                 user_data['fans'], 
                                 user_data['compliment_funny'], 
                                 user_data['compliment_hot'],
                                 user_data['compliment_cute']) \
                         .orderBy(user_data['review_count'].desc()) \
                         .limit(10)

    user_data.write.csv(output, mode="overwrite", header=True)

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('top ten users').getOrCreate()
    assert spark.version >= '2.4'  # make sure we have Spark 2.4+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)
