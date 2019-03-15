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

'''
Distributed solution for Insight's indexing
coding challenge using Apache Spark. Reads in
a list of documents and constructs an
inverted index.
'''

conf = SparkConf()
sc = SparkContext(conf=conf)
spark = SparkSession.builder.getOrCreate()

def main():
	try:
		input_path = sys.argv[1]
		output_path = sys.argv[2]
	except:
		print("Invalid input: provide input path then output path")
	build_dict(input_path, output_path)

def build_dict(input_path, output_path):
	'''
	Reads documents in input path dir
	and collects every unique pair of
	(wordID, docID)
	'''

	files = os.listdir(input_path)
	rdds = [] # list of word indices per document

	# Read in files individually, parse text and create dictionary
	for filename in files:
		words_seen_in_file = {}
		f = spark.read.text(input_path + '/' + filename) \
			.withColumn("docID", lit(filename))
		f = f.select(
				f.docID,
				f.value
			) \
			.rdd.flatMapValues(lambda line: line.strip('\n').translate(str.maketrans('', '', string.punctuation)).split(" ")) \
			.distinct()
		rdds.append(f)

	# Combine (and invert) array of dictionaries from each document into one index
	output =  sc.union(rdds) \
				.keyBy(lambda x: x[1]) \
				.mapValues(lambda x: x[0]) \
				.groupByKey() \
				.map(lambda x: (x[0], list(x[1])))

	output.coalesce(1).saveAsTextFile(output_path)

if __name__ == "__main__":
	main()
