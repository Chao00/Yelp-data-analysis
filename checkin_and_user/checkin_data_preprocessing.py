from pyspark.sql import SparkSession, functions, types
import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

def generate_records(row):
    for i in row['date'].split(', '):
      yield (row['business_id'], i)

def main(checkin_input, business_inputs, output):
    checkin_schema = types.StructType([
        types.StructField('business_id', types.StringType()),
        types.StructField('date', types.StringType())
    ])

    business_schema = types.StructType([
    types.StructField('business_id', types.StringType(), False),
    types.StructField('name', types.StringType(), True),
    types.StructField('address', types.StringType(), True),
    types.StructField('city', types.StringType(), True),
    types.StructField('state', types.StringType(), True),
    types.StructField('postal code', types.StringType(), True),
    types.StructField('latitude', types.DoubleType(), True),
    types.StructField('longitude', types.DoubleType(), True),
    types.StructField('stars', types.DoubleType(), True),
    types.StructField('review_count', types.IntegerType(), True),
    types.StructField('is_open', types.IntegerType(), True),
    types.StructField('attributes', types.StringType(), True),
    types.StructField('categories', types.StringType(), True),
    types.StructField('hours', types.StringType(), True)
])

    business = spark.read.json(business_inputs, schema=business_schema)
    checkin = spark.read.json(checkin_input, schema=checkin_schema)

    checkin = checkin.join(business, 'business_id')

    checkin = checkin.withColumn('checkin_count',
        ((functions.length(checkin['date']) + functions.lit(2)) /
        functions.lit(21)).cast('int'))
    checkin.write.csv(output + "/checkin", mode="overwrite", header=True)

    checkin_records_schema = types.StructType([
        types.StructField('business_id', types.StringType()),
        types.StructField('date_pair', types.StringType())
    ])

    checkin_records = spark.createDataFrame(checkin.rdd.flatMap(generate_records),
                                            schema=checkin_records_schema)

    checkin_records = checkin_records.withColumn('date',
                          functions.split(checkin_records['date_pair'], ' ')[0])
    checkin_records = checkin_records.withColumn('time',
                          functions.split(checkin_records['date_pair'], ' ')[1])
    checkin_records = checkin_records.withColumn('year',
                          functions.split(checkin_records['date'], '-')[0])
    checkin_records = checkin_records.withColumn('month',
                          functions.split(checkin_records['date'], '-')[1])
    checkin_records = checkin_records.withColumn('hour',
                          functions.split(checkin_records['time'], ':')[0])
    checkin_records = checkin_records.drop('date_pair')
    checkin_records.write.csv(output + "/checkin_records", mode="overwrite", header=True)

if __name__ == '__main__':
    checkin_input = sys.argv[1]
    business_inputs = sys.argv[2]
    output = sys.argv[3]
    spark = SparkSession.builder.appName('checkin data preprocessing').getOrCreate()
    assert spark.version >= '2.4'  # make sure we have Spark 2.4+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(checkin_input, business_inputs, output)
