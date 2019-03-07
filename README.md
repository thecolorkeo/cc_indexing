# cc_indexing
Insight Practice Coding Challenge: Indexing

# Challenge Introduction
We have a collection of N documents. We want a dictionary that matches every word from the corpus of documents with a unique id. In this case, we have one file per document and the name of the file is simply the index of the document as shown below:
![alt text](https://github.com/thecolorkeo/cc_indexing/blob/master/figures/Documents.png)

We want a dictionary that matches every word from the documents with a unique id:
![alt text](https://github.com/thecolorkeo/cc_indexing/blob/master/figures/Dictionary.png)

Using both the dataset and the dictionary we can build an inverted index that gives, for every word, the list of documents that it appears in:
![alt text](https://github.com/thecolorkeo/cc_indexing/blob/master/figures/InvertedIndex.png)

We want a solution that works with a massive dataset. Our algorithm has been able to run on a distributed system so we are not limited by the amounf ot storage, memory and CPU of a single machine.

Full challenge description can be found [here](https://github.com/Samariya57/coding_challenges/blob/master/challenge.pdf)

# Solution
Algorithm logic:
1. Read the documents and collect every pair (wordID, docID)
2. Sort those pairs by wordID and by docID
3. For every wordID, group the pairs so you have its list of documents
4. Merge the intermediate results to get the final inverted index

# Run
My solution uses Apache Spark 2.4 to read in all documents from `data` and output the inverted index to `output`. The pyspark script is found in `src`. To get started, run `./run.sh` from the root folder.

If you don't have Spark, download Spark 2.4 from [here](http://apache.mirrors.tds.net/spark/spark-2.4.0/). Run `pip install pyspark  --user` from the terminal to install pyspark.
