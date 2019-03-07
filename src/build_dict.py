# Keo Chan
# Insight Practice Coding Challenge W3

import os
import sys
import codecs
import string
import operator

def build_dict(path):
	'''
	Reads documents in path folder
	and collects every unique pair of
	(wordID, docID)
	'''
	os.chdir(path)
	files = os.listdir(path)
	# create one dictionary for keeping track of unique words and assigning ids
	word_ids = {}
	word_id = 0
	# create a master list to output (wordid, docid) pairs
	output_pairs = []
	for filename in files:
		words_seen_in_file = {}
		with codecs.open(filename, mode='r', encoding='cp1252') as f:
			for line in iter(f.readline,''):
				line = line.strip('\n').translate(str.maketrans('', '', string.punctuation)).split(" ")
				for word in line:
					if word not in word_ids:
						word_ids[word] = word_id
						word_id += 1
					if word not in words_seen_in_file and word != '':
						words_seen_in_file[word] = ''
						# append to master only the first time a new word is encountered per document
						output_pairs.append([word_ids[word], filename])
					words_seen_in_file[word] = ''
	return output_pairs

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


def main():
	input_path = sys.argv[1]
	output_path = sys.argv[2]
	dict = build_dict(input_path)
	sorted_dict = sort_dict(dict)
	inverted_index = build_ii(sorted_dict)
	print(inverted_index)

if __name__ == "__main__":
	main()
