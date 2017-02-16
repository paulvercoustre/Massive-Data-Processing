# Massive-Data-Processing - 1st Assignement
Due 16th of February 2016

## Preleminary matter: the set-up
In order to complete the assignment we used the following Hadoop set-up:

Using:
`
hadoop version checknative -adfsadmin
`

We obtain
```
Hadoop 2.6.0-cdh5.5.0
Subversion http://github.com/cloudera/hadoop -r fd21232cef7b8c1f536965897ce20f50b83ee7b2
Compiled by jenkins on 2015-11-09T20:37Z
Compiled with protoc 2.5.0
From source with checksum 98e07176d1787150a6a9c087627562c
This command was run using /usr/jars/hadoop-common-2.6.0-cdh5.5.0.jar
```
This has been run on the recommended Cloudera Quickstart Vitual Machine, for better performance the base memory has been set to 8102 MB

## Instructions
You are asked to implement an inverted index in MapReduce for the document corpus of: 
* pg100.txt (from http://www.gutenberg.org/cache/epub/100/pg100.txt)
* pg31100.txt (from http://www.gutenberg.org/cache/epub/31100/pg31100.txt)
* pg3200.txt (from http://www.gutenberg.org/cache/epub/3200/pg3200.txt) 

This corpus includes the complete works of William Shakespear, Mark Twain and Jane Austen, respectively.

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
* We set the number of reducers to 10 with (see [reference list](References.md)):
```java
job.setNumReduceTasks(10);
```
We run the job in the terminal and observe our results with the command `hadoop fs -ls outpu_stop_word_10_reducers_no_combiner`

As expected we obtain the following:
```
Found 11 items
-rw-r--r--   1 cloudera cloudera          0 2017-02-16 03:04 output_stop_word_10_reducers_no_combiner/_SUCCESS
-rw-r--r--   1 cloudera cloudera        108 2017-02-16 03:03 output_stop_word_10_reducers_no_combiner/part-r-00000
-rw-r--r--   1 cloudera cloudera        198 2017-02-16 03:04 output_stop_word_10_reducers_no_combiner/part-r-00001
-rw-r--r--   1 cloudera cloudera        159 2017-02-16 03:04 output_stop_word_10_reducers_no_combiner/part-r-00002
-rw-r--r--   1 cloudera cloudera        100 2017-02-16 03:04 output_stop_word_10_reducers_no_combiner/part-r-00003
-rw-r--r--   1 cloudera cloudera         51 2017-02-16 03:04 output_stop_word_10_reducers_no_combiner/part-r-00004
-rw-r--r--   1 cloudera cloudera         98 2017-02-16 03:04 output_stop_word_10_reducers_no_combiner/part-r-00005
-rw-r--r--   1 cloudera cloudera        138 2017-02-16 03:04 output_stop_word_10_reducers_no_combiner/part-r-00006
-rw-r--r--   1 cloudera cloudera        129 2017-02-16 03:04 output_stop_word_10_reducers_no_combiner/part-r-00007
-rw-r--r--   1 cloudera cloudera        167 2017-02-16 03:04 output_stop_word_10_reducers_no_combiner/part-r-00008
-rw-r--r--   1 cloudera cloudera         29 2017-02-16 03:04 output_stop_word_10_reducers_no_combiner/part-r-00009
```

Looking at the job's logs we see that the total run time is 1min 35sec (see [Job Tracker](image/Screen_Shot_Stop_Words_10_reducers_no_combiner.jpg)). The complete code for this job is available [here](code/InvertedIndex_10_Reducers_no_Combiner.java)

#### ii. (10) Run the same program again, this time using a Combiner. Report the execution time. Is there any difference in the execution time, compared to the previous execution? Why?

In order to run the job using a Combiner we simply add `job.setCombinerClass(Reduce.class); // we add a combiner` (see [reference list](References.md). 

We obtain similar results except this time the run time went down to 1min 19sec (see [Job Tracker](image/Screen_Shot_Stop_Words_10_reducers.jpg))

This makes sense because instead of passing all the key-value pairs from the mapper to the reducer, the Combiner ensures that key-value pairs with the same key are grouped. This reduces the amount of data passed on to the reduce step.

#### iii. (5) Run the same program again, this time compressing the intermediate results of map (using any codec you wish). Report the execution time. Is there any difference in the execution, time compared to the previous execution? Why?

Building on the changes we made previously, we add the following to compress the output of map before it is passed on to next step (see [reference list](References.md)):
```
conf.setBoolean(Job.MAP_OUTPUT_COMPRESS, true); // we compress the map output using BZip2Codec
      conf.setClass(Job.MAP_OUTPUT_COMPRESS_CODEC, BZip2Codec.class,
```

We obtain similar results and the run time is almost the same : 1 min  18sec. The fact that compression does not seem to have a significant impact may be due to the relatively small size of the data we are working with. When scaling up, the impact of compression is usually blatant. (see [Job Tracker](image/Screen_Shot_Stop_Words_10_reducers_map_compression)).jpg

#### iv. (5) Run the same program again, this time using 50 reducers. Report the execution time. Is there any difference in the execution time, compared to the previous execution? Why?


