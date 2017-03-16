# Massive-Data-Processing - 2nd Assignement
Due 17th of March 2017

## Pre-processing the Input
In this assignment, you will use the document corpus of pg100.txt (from http://www.gutenberg.org/cache/epub/100/pg100.txt), as in your previous assignments, assuming that each line represents a distinct document (treat the line number as a document id). Implement a pre-processing job in which you will:

An inverted index provides for each distinct word in a document corpus, the filenames that contain this word, along with some other information (e.g., count/position within each document).

#### (a)(30) Run a MapReduce program to identify stop words (words with frequency > 4000) for the given document corpus. Store them in a single csv file on HDFS (stopwords.csv). You can edit the several parts of the reducers’ output after the job finishes (with hdfs commands or with a text editor), in order to merge them as a single csv file.
#### i. (10) Use 10 reducers and do not use a combiner. Report the execution time.

We start by creating a new project, package and class similarly to what was done in assignment 0. 
To download the 3 .txt files in the right folder we used:
```
cd ~/workspace/InvertedIndex
curl http://www.gutenberg.org/cache/epub/100/pg100.txt | perl -pe 's/^nxEFnxBB
> nxBF//' > pg100.txt
```

This first question can be solved by using the WordCount class 'edu.stanford.cs246.wordcount' which we tweak for our purposes:
* Similarly to task 3 of the previous assignment, we consider all words as lower case since doing differently would lead to misleading results. In the mapper we add:
```java
word.set(token.toLowerCase(Locale.ENGLISH));
```
* To identify the stop words we simply need to exclude words with frequency less than 4000 in the reduce process. In the reducer we add:
```java
if (sum > 4000) {              // we only write keys associated to values > 4000 on the output file
        	 context.write(key, new IntWritable(sum));
```


Looking at the job's logs we see that the total run time is 1min 35sec (see [Job Tracker](https://github.com/paulvercoustre/Massive-Data-Processing/blob/master/Assignment_1/img/Screen_Shot_Stop_Words_10_reducers_no_combiner.jpg)). The complete code for this job is available [here](https://github.com/paulvercoustre/Massive-Data-Processing/tree/master/Assignment_1/src/inverted_index)

![Job Tracker](https://github.com/paulvercoustre/Massive-Data-Processing/blob/master/Assignment_1/img/Screen_Shot_Stop_Words_10_reducers_no_combiner.jpg)

