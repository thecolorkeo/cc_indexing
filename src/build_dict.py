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

def build_dict(path):
	'''
	Reads documents in path folder
	and collects every unique pair of
	(wordID, docID)
	'''
	#os.chdir(path)
	files = os.listdir(path)

	# sc = SparkContext()
	# create one dictionary for keeping track of unique words and assigning ids
	word_ids = {}
	word_id = 0
	# create a master list to output (wordid, docid) pairs
	rdds = []

	for filename in files:
		words_seen_in_file = {}
		f = spark.read.text(path + '/' + filename) \
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
	output.coalesce(1).saveAsTextFile('./test/')

def sort_dict(list):
	'''
	Takes list of (wordid, docid) pairs
	and returns a sorted list sorted by
	first wordid, then docid
	'''
	return sorted(list, key = operator.itemgetter(0,1))

def build_ii(sorted):
	'''
	Takes a sorted list and builds an
	inverted index: Outputs a nested list
	where each wordid is paired with a list
	of all the documents it is found in
	'''
	output = {}
	for pair in sorted:
		if pair[0] not in output:
			output[pair[0]] = []
		output[pair[0]].append(pair[1])
	return output

def export(index, path):
	'''
	Writes inverted index (list of lists)
	to output file `path`
	'''
	out_stream = open(path, 'w')
	for i in index:
		out_string = '(' + str(i) + ', [' + ','.join(index[i])
		out_string += '])'
		out_stream.write(out_string)

def main():
	input_path = sys.argv[1]
	output_path = sys.argv[2]

	dict = build_dict(input_path)
	# sorted_dict = sort_dict(dict)
	# inverted_index = build_ii(sorted_dict)
	# export(inverted_index, output_path)

if __name__ == "__main__":
	main()
