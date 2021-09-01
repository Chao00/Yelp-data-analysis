#Most popular business categories
from pyspark import SparkConf, SparkContext
import sys
assert sys.version_info >= (3, 5)
import json
from pyspark.sql import SparkSession, functions, types
import re, string, nltk

def preprocess(x):
    x = re.sub('[^a-z\s]', '', x.lower())                  # get rid of noise
    x = [w for w in x.split() if w not in set(stopwords)]  # remove stopwords
    return ' '.join(x)

def parse_business_json(line):
    json_obj = json.loads(line)
    return {'business_id': json_obj['business_id'], 'name': json_obj['name'], 'stars': json_obj['stars'], 'review_count': json_obj['review_count'], 
    'categories': json_obj['categories'].split(',')}

def parse_tips_json(line):
    json_obj = json.loads(line)
    return {'text': json_obj['text'], 'date': json_obj['date'], 'compliment_count': json_obj['compliment_count'], 'business_id': json_obj['business_id'], 
    'user_id': json_obj['user_id']}

def filter_empty_categories(line):
    json_obj = json.loads(line)
    return json_obj['categories'] is not None  


def main():
    business_schema = types.StructType([
    types.StructField('business_id', types.StringType(), False),
    types.StructField('name', types.StringType(), True),
    types.StructField('stars', types.DoubleType(), True),
    types.StructField('review_count', types.IntegerType(), True),
    types.StructField('categories', types.StringType(), True)
])

    tips_schema = types.StructType([
    types.StructField('text', types.StringType(), True),
    types.StructField('date', types.StringType(), True),
    types.StructField('compliment_count', types.IntegerType(), True),
    types.StructField('business_id', types.StringType(), False),
    types.StructField('user_id', types.StringType(), True)
])

    top_res_schema = types.StructType([
    types.StructField('name', types.StringType(), False),
    types.StructField('categories', types.StringType(), True),
    types.StructField('review_count', types.IntegerType(), True),
    types.StructField('stars', types.DoubleType(), True)
])

    business = sc.textFile('../yelp_dataset/yelp_academic_dataset_business.json')
    business = business.filter(filter_empty_categories).map(parse_business_json)
    tips = sc.textFile('../yelp_dataset/yelp_academic_dataset_tip.json').map(parse_tips_json)
    df_business = spark.createDataFrame(business, business_schema)
    df_tips =  spark.createDataFrame(tips, tips_schema)
    top_ten_res = spark.read.csv('topTenRatedRes.csv', schema=top_res_schema)

    top_ten_res_name_list = top_ten_res.select('name').rdd.flatMap(list).collect()
    top_ten_res_df = df_business.where(df_business['name'].isin(top_ten_res_name_list))
    top_ten_res_business_id = top_ten_res_df.select('business_id').rdd.flatMap(list).collect()
    top_ten_res_tips_df = df_tips.where(df_tips['business_id'].isin(top_ten_res_business_id))
    top_ten_res_tips_text = top_ten_res_tips_df.select('text').rdd.flatMap(list)
    top_ten_res_tips_text = top_ten_res_tips_text.map(preprocess)
    top_ten_res_tips_text.coalesce(1,True).saveAsTextFile("top_ten_res_tips")

    df_earl_of_sandwitch = df_business.where(df_business['name'] == "Earl of Sandwich")
    list_of_business_id = df_earl_of_sandwitch.select('business_id').rdd.flatMap(list)
    df_earl_of_sandwitch_tip = df_tips.where(df_tips['business_id'].isin(list_of_business_id.collect()))
    temp = df_earl_of_sandwitch_tip.select('text').rdd.flatMap(list)
    temp = temp.map(preprocess)
    temp.coalesce(1,True).saveAsTextFile("earl_of_sandwitch_tips")

    # df_restaurant = df_business.where(df_business['name'] == business_name)
    # list_of_business_id = df_restaurant.select('business_id').rdd.flatMap(list)
    # df_restaurant_tip = df_tips.where(df_tips['business_id'].isin(list_of_business_id.collect()))
    # restaurant_tip = df_restaurant_tip.select('text').rdd.flatMap(list)
    # restaurant_tip = restaurant_tip.map(preprocess)
    # restaurant_tip.coalesce(1,True).saveAsTextFile(output)

    df_mcdonalds = df_business.where(df_business['name'] == 'McDonalds')
    list_of_business_id_mcdonalds = df_mcdonalds.select('business_id').rdd.flatMap(list)
    df_mcdonalds_tip = df_tips.where(df_tips['business_id'].isin(list_of_business_id_mcdonalds.collect()))
    mcdonalds_tip = df_mcdonalds_tip.select('text').rdd.flatMap(list)
    mcdonalds_tip = mcdonalds_tip.map(preprocess)
    mcdonalds_tip.coalesce(1,True).saveAsTextFile("mcdonalds_tip")

    df_Little_Miss_BBQ = df_business.where(df_business['name'] == 'Little Miss BBQ')
    list_of_business_id_Little_Miss_BBQ = df_Little_Miss_BBQ.select('business_id').rdd.flatMap(list)
    df_Little_Miss_BBQ_tip = df_tips.where(df_tips['business_id'].isin(list_of_business_id_Little_Miss_BBQ.collect()))
    Little_Miss_BBQ_tip = df_Little_Miss_BBQ_tip.select('text').rdd.flatMap(list)
    Little_Miss_BBQ_tip = Little_Miss_BBQ_tip.map(preprocess)
    Little_Miss_BBQ_tip.coalesce(1,True).saveAsTextFile("Little_Miss_BBQ_tip")


if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit average')
    sc = SparkContext(conf=conf)
    spark = SparkSession.builder.appName('demo').getOrCreate()
    sc.setLogLevel('WARN')
    assert sc.version >= '2.4' 
    stopwords = nltk.corpus.stopwords.words('english')
    punctuation = list(string.punctuation)
    unwanted_words = ['give', 'add', 'says', 'hasnt']
    stopwords = set(stopwords).union(punctuation).union(unwanted_words)
    # business_name = sys.argv[1]
    # output = sys.argv[2]