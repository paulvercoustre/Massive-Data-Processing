# Massive-Data-Processing - 1rst Assignement
Due 16th of February 2016

Preleminary matter: the set-up
img/

You are asked to implement an inverted index in MapReduce for the document corpus of: 
* pg100.txt (from http://www.gutenberg.org/cache/epub/100/pg100.txt)
* pg31100.txt (from http://www.gutenberg.org/cache/epub/31100/pg31100.txt)
* pg3200.txt (from http://www.gutenberg.org/cache/epub/3200/pg3200.txt) 

This corpus includes the complete works of William Shakespear, Mark Twain and Jane Austen, respectively.

An inverted index provides for each distinct word in a document corpus, the filenames that contain this word, along with some other information (e.g., count/position within each document).

(a) (30) Run a MapReduce program to identify stop words (words with frequency > 4000) for the given document corpus. Store them in a single csv file on HDFS (stopwords.csv). You can edit the several parts of the reducers’ output after the job finishes (with hdfs commands or with a text editor), in order to merge them as a single csv file.
i. (10) Use 10 reducers and do not use a combiner. Report the execution time.
