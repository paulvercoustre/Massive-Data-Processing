# Massive-Data-Processing - 1rst Assignement
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
To download the 3 .txt files we used:
```
cd ~/workspace/InvertedIndex
curl http://www.gutenberg.org/cache/epub/100/pg100.txt | perl -pe 's/^nxEFnxBB
> nxBF//' > pg100.txt
```

Rather than coding the whole job from the ground up we used the WordCount class which we tweaked for our purposes:


