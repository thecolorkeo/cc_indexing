# Keo Chan
# Insight Practice Coding Challenge W3

import os
import sys
import codecs
import string
import operator
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

conf = SparkConf()
sc = SparkContext(conf=conf)
spark = SparkSession.builder.getOrCreate()

def build_dict(input_path, output_path):
	'''
	Reads documents in path folder
	and collects every unique pair of
	(wordID, docID)
	'''

	files = os.listdir(input_path)
	rdds = [] # list of indices per document

	for filename in files:
		words_seen_in_file = {}
		f = spark.read.text(input_path + '/' + filename) \
			.withColumn("docID", lit(filename))
		f = f.select(
				f.docID,
				f.value
			) \
			.rdd.flatMapValues(lambda line: line.strip('\n').translate(str.maketrans('', '', string.punctuation)).split(" ")) \
			.keyBy(lambda x: x[1]) \
			.mapValues(lambda x: x[0]) \
			.distinct() \
			.groupByKey() \
			.map(lambda x: (x[0], list(x[1])))
		rdds.append(f)

	output =  sc.union(rdds) \
				.groupByKey() \
				.map(lambda x: (x[0], list(x[1])))
	output.coalesce(1).saveAsTextFile(output_path)

def main():
	input_path = sys.argv[1]
	output_path = sys.argv[2]
	build_dict(input_path, output_path)

if __name__ == "__main__":
	main()
