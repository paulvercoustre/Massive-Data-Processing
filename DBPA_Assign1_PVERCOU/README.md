# Massive-Data-Processing - 1st Assignement
Due 16th of February 2016

## Preleminary matter: the set-up
In order to complete the assignment we used the following Hadoop set-up:

Using:
`
hadoop version checknative -adfsadmin
`

We obtain:
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

Looking at the job's logs we see that the total run time is 1min 35sec (see [Job Tracker](https://github.com/paulvercoustre/Massive-Data-Processing/blob/master/Assignment_1/img/Screen_Shot_Stop_Words_10_reducers_no_combiner.jpg)). The complete code for this job is available [here](https://github.com/paulvercoustre/Massive-Data-Processing/tree/master/Assignment_1/src/inverted_index)

![Job Tracker](https://github.com/paulvercoustre/Massive-Data-Processing/blob/master/Assignment_1/img/Screen_Shot_Stop_Words_10_reducers_no_combiner.jpg)

#### ii. (10) Run the same program again, this time using a Combiner. Report the execution time. Is there any difference in the execution time, compared to the previous execution? Why?

In order to run the job using a Combiner we simply add `job.setCombinerClass(Reduce.class); // we add a combiner` (see [reference list](References.md)). 

We obtain similar results except this time the run time went down to 1min 19sec (see [Job Tracker](https://github.com/paulvercoustre/Massive-Data-Processing/blob/master/Assignment_1/img/Screen_Shot_Stop_Words_10_reducers.jpg))

![Job Tracker](https://github.com/paulvercoustre/Massive-Data-Processing/blob/master/Assignment_1/img/Screen_Shot_Stop_Words_10_reducers.jpg)

This makes sense because instead of passing all the key-value pairs from the mapper to the reducer, the Combiner ensures that key-value pairs with the same key are grouped. This reduces the amount of data passed on to the reduce step.

#### iii. (5) Run the same program again, this time compressing the intermediate results of map (using any codec you wish). Report the execution time. Is there any difference in the execution, time compared to the previous execution? Why?

Building on the changes we made previously, we add the following to compress the output of map before it is passed on to next step (see [reference list](References.md)):
```
conf.setBoolean(Job.MAP_OUTPUT_COMPRESS, true); // we compress the map output using BZip2Codec
      conf.setClass(Job.MAP_OUTPUT_COMPRESS_CODEC, BZip2Codec.class,
```

We obtain similar results and the run time is almost the same : 1 min  18sec. The fact that compression does not seem to have a significant impact may be due to the relatively small size of the data we are working with. When scaling up, the impact of compression is usually blatant. (see [Job Tracker](https://github.com/paulvercoustre/Massive-Data-Processing/blob/master/Assignment_1/img/Screen_Shot_Stop_Words_10_reducers_map_compression.jpg))

![Job Tracker](https://github.com/paulvercoustre/Massive-Data-Processing/blob/master/Assignment_1/img/Screen_Shot_Stop_Words_10_reducers_map_compression.jpg)

#### iv. (5) Run the same program again, this time using 50 reducers. Report the execution time. Is there any difference in the execution time, compared to the previous execution? Why?

Here we simply need to change `java
job.setNumReduceTasks(10);
` to `java
job.setNumReduceTasks(50);
`

The output is composed of 50 seperate files and the run time is signifcantly longer than the previous jobs: 4mins 31sec. Again, this makes sense because we are running on a single machine therefore each reducer has to wait for the previous reducer to be done with it's work before it can start operating. (see [Job Tracker](https://github.com/paulvercoustre/Massive-Data-Processing/blob/master/Assignment_1/img/Screen_Shot_Stop_Words_50_reducers.png))
![Job Tracker](https://github.com/paulvercoustre/Massive-Data-Processing/blob/master/Assignment_1/img/Screen_Shot_Stop_Words_50_reducers.png)

To obtain a csv file we add `job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ",");` in our class.

See here for our final result: [Result Section A](https://github.com/paulvercoustre/Massive-Data-Processing/tree/master/Assignment_1/outputs)

#### (b) (30) Implement a simple inverted index for the given document corpus, as shown in the previous Table, skipping the words of stopwords.csv.

In order to make our work easier in the beginning, we will implement an inverted index that does not exclude the stop words. We use the class previously created as a base for this task since it's mostly the map and reduce class that will need to be modified.

The resulting map class is:
```java
public static class Map extends Mapper<LongWritable, Text, Text, Text> { // the output of the mapper is text for the key and text for the value 
      private Text word = new Text(); // we define the variable corresponding to the key
      private Text file_membership = new Text(); // we define the variable corresponding to the value

      @Override
      public void map(LongWritable key, Text value, Context context)
              throws IOException, InterruptedException {
    	 
    	 String file_name = ((FileSplit) context.getInputSplit()) // we get the document name associated
                  .getPath().getName();
         file_membership = new Text(file_name);
         
         for (String token: value.toString().split("\\s+")) {
            word.set(token.toLowerCase(Locale.ENGLISH)); // we convert all strings to lower case
            context.write(word, file_membership); // output in the inverted index format
         }
      }
   }
```

The reduce class we implement is the following:
```java
public static class Reduce extends Reducer<Text, Text, Text, Text> { // both input and output for both key and value is text
      @Override
      public void reduce(Text key, Iterable<Text> values, Context context)
              throws IOException, InterruptedException {
    	  
    	  Set<String> document_list = new LinkedHashSet<String>();
    	  
    	  for (Text val : values) {
    		  document_list.add(val.toString());
    	  }
    	  
    	  context.write(key, new Text(document_list.toString().replace("[","").replace("]","")));  
         }
         
      }
```
You can find the full code relative to this job [here](https://github.com/paulvercoustre/Massive-Data-Processing/tree/master/Assignment_1/src/inverted_index)

In order to exclude the stop words from our inverted index we have 2 options: either we get rid of them in the map phase or we suppress them later on during the reduce phase. Intuitevely it seems that taking care of the stop words in the map phase is better design since we do not carry data we do not want for an extra step. We do this by changing the map class as follows:
```java
public static class Map extends Mapper<LongWritable, Text, Text, Text> { // the output of the mapper is text for both key & value 
      private Text word = new Text(); // we define the variable corresponding to the key
      private Text file_membership = new Text(); // we define the variable corresponding to the value

      @Override
      public void map(LongWritable key, Text value, Context context)
              throws IOException, InterruptedException {
    	  
    	 HashSet<String> excluded_words = new HashSet<String>(); 
    	 BufferedReader DocumentReader = new BufferedReader( // we use BuffferedReader to read our stop words file.
    			 new FileReader(new File("/home/cloudera/workspace/InvertedIndex/output/stop_words_final_output.txt")));
    	 
    	// words present in the stop words file are added in a list that cannot contain duplicates
    	 String case_;
    	 while ((case_ = DocumentReader.readLine()) != null) { 
    		 excluded_words.add(case_.toLowerCase());
    	 }
    	 String file_name = ((FileSplit) context.getInputSplit()) // we get the document name associated to the key...
                  .getPath().getName();
         file_membership = new Text(file_name); // ... and store it in a text variable
         
         for (String token: value.toString().split("\\s+")) {
            if (!excluded_words.contains(token.toLowerCase(Locale.ENGLISH))) { // we check that the word is not a stop word
            	word.set(token.toLowerCase());
            }
         }
         context.write(word, file_membership); // output in the inverted index format
      }
  }
```
You can find the full code relative to this job [here](https://github.com/paulvercoustre/Massive-Data-Processing/tree/master/Assignment_1/src/inverted_index)

The run time of the job is 49 sec. You can find the output [Result Section B](https://github.com/paulvercoustre/Massive-Data-Processing/tree/master/Assignment_1/outputs)

![Job Tracker](https://github.com/paulvercoustre/Massive-Data-Processing/blob/master/Assignment_1/img/Screen_Shot_Inverted_Index_Excluding_Stop_Words.png)

#### (c) (10) How many unique words exist in the document corpus (excluding stop words)? Which counter(s) reveal(s) this information? Define your own counter for the number of words appearing in a single document only. What is the value of this counter? Store the final value of this counter on a new file on HDFS.

The number of unique words in the document corpus (excluding stop words)) is easily given by the number of lines contained in the output file we produced in the previous section. We recall the results from the previous job:
```
Map-Reduce Framework
		Map input records=507535
		Map output records=507535
		Map output bytes=8319425
		Map output materialized bytes=335229
		Input split bytes=402
		Combine input records=507535
		Combine output records=88748
		Reduce input groups=71037
		Reduce shuffle bytes=335229
		Reduce input records=88748
	==>	Reduce output records=71037  <==
		Spilled Records=177496
		Shuffled Maps =3
		Failed Shuffles=0
		Merged Map outputs=3
		GC time elapsed (ms)=725
		CPU time spent (ms)=0
		Physical memory (bytes) snapshot=0
		Virtual memory (bytes) snapshot=0
		Total committed heap usage (bytes)=773603328
```

The Reduce output records correspond to the number of lines, therefore there are 71 037 unique words in the corpus excluding the stop words. 

To count the number of words that appear in one document only is equivalent to counting the number of lines where the size of the value is equal to 1. To do this, we define an enum type and increment it in the reduce class as follows (see [reference list](References.md):

``` java
   public static class Reduce extends Reducer<Text, Text, Text, Text> { // both input and output for both key and value is text
      @Override
      public void reduce(Text key, Iterable<Text> values, Context context)
              throws IOException, InterruptedException {
    	  
    	  HashSet<String> document_list = new HashSet<String>();
    	  
    	  for (Text val : values) {
    		  document_list.add(val.toString());
    	  }
    	  if (document_list.size() == 1) {
			  context.getCounter(SINGLE_DOC_COUNTER.SINGLE_DOCUMENT).increment(1);
    	  }
    	  context.write(key, new Text(document_list.toString().replace("[","").replace("]","")));
         }         
      }
```

However this leads to the rather akward and wrong results of 145 781 (i.e more words in one document only than there are unique words...): 
```
inverted_index_qb.FullInvertedIndex$SINGLE_DOC_COUNTER
		SINGLE_DOCUMENT=145781
```

Alternaively we can include `context.write(key, new Text(document_list.toString().replace("[","").replace("]","")));` in the condition we added and then count the number of lines in the output file. You can find the full code relative to this job [here](https://github.com/paulvercoustre/Massive-Data-Processing/tree/master/Assignment_1/src/inverted_index)

We obtain the following results:
```
Map-Reduce Framework
		Map input records=507535
		Map output records=507535
		Map output bytes=8319425
		Map output materialized bytes=335229
		Input split bytes=402
		Combine input records=507535
		Combine output records=88748
		Reduce input groups=71037
		Reduce shuffle bytes=335229
		Reduce input records=88748
	==>	Reduce output records=57033   <==
		Spilled Records=177496
		Shuffled Maps =3
		Failed Shuffles=0
		Merged Map outputs=3
		GC time elapsed (ms)=753
		CPU time spent (ms)=0
		Physical memory (bytes) snapshot=0
		Virtual memory (bytes) snapshot=0
		Total committed heap usage (bytes)=773603328
```

We see that there are 57 003 unique words present in 1 document only. 

#### (d) (30) Extend the inverted index of (b), in order to keep the frequency of each word for each document. The new output should be of the form:

| Key  | Value                              | 
| ---- |:----------------------------------:|
| this | doc1.txt#1, doc2.txt#1             |
| is   | doc1.txt#2, doc2.txt#1, doc3.txt#1 |
| a    | doc1.txt#1                         |

which means that the word frequency should follow a single ‘#’ character, which should follow the filename, for each file that contains this word. You are required to use a Combiner.

To tackle this question we will use the class from the previous question as a template and will simply need to change the reduce function. Here we need not only to output an inverse index as in question b, but to augment it with the frequeny of the key for each value. We used the HashMap class to implement this change (see [reference](References.md)).
The reduce class is as follows: 
```java
   public static class Reduce extends Reducer<Text, Text, Text, Text> { // both input and output for both key and value is text
      @Override
      public void reduce(Text key, Iterable<Text> values, Context context)
              throws IOException, InterruptedException {
    	  
    	  HashMap<String,Integer> document_list = new HashMap<String,Integer>(); // we create a new hashmap
    	  	
    	  	for (Text val : values) {
    	  		
    	  		if (document_list.containsKey(val.toString())) {
    	  			document_list.put(val.toString(), document_list.get(val.toString())+1); // iteratively increase the count 
    		    }
    			   
    	  		else {
    	  			document_list.put(val.toString(), 1);   // add the document name to the hashmap at the first iteration with value 1
    	  		}
    		  }
    		 
			context.write(key, new Text(document_list.toString().replace("{", "").replace("}", "").replace("=", "#")));    	  
         }         
      }
```

You can access to the full code for this job [here](https://github.com/paulvercoustre/Massive-Data-Processing/tree/master/Assignment_1/src/inverted_index) and the final output [here](https://github.com/paulvercoustre/Massive-Data-Processing/tree/master/Assignment_1/outputs)

The total run time is 1min 3sec. This was run with a combiner, like the previous jobs of this assignment.
![Job Tracker](https://github.com/paulvercoustre/Massive-Data-Processing/blob/master/Assignment_1/img/Screen%20Shot_Final_Inverted_Index_with_Freq.png)

Unfortunately the outfile does not have exactly the correct output since the format is:

| Key  | Value                                     | 
| ---- |:-----------------------------------------:|
| this | doc1.txt#21#1, doc2.txt#2#1               |
| is   | doc1.txt#2#1, doc2.txt#1#1, doc3.txt#10#1 |
| a    | doc1.txt#100#1                            |

Several attemps were made to counter this issue including the use of Collections. 
