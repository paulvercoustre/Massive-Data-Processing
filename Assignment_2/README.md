# Massive-Data-Processing - 2nd Assignement
Due 17th of March 2017

## Pre-processing the input
In this assignment, you will use the document corpus of pg100.txt (from http://www.gutenberg.org/cache/epub/100/pg100.txt), as in your previous assignments, assuming that each line represents a distinct document (treat the line number as a document id). Implement a pre-processing job in which you will:

#### (a)(2) Remove all stopwords (you can use the stopwords file of your previous assignment), special characters (keep only [a-z],[A-Z] and [0-9]) and keep each unique word only once per line. Don’t keep empty lines.

We start by creating a new project, package and class similarly to what was done in assignment 0 & 1.
To find the stopwords in pg100.txt we run the same "stopwords" job from the previous assignment on this particular text only. We obtain [this file](https://github.com/paulvercoustre/Massive-Data-Processing/blob/master/Assignment_2/stopwords/part-r-00000).

To remove special characters we apply `replaceAll("[^A-Za-z0-9]"," ")` to the input values of the map function.
To ensure we don't keep empty lines we simply add the condition `if (!value.toString().replaceAll("[^A-Za-z0-9]"," ").isEmpty())`. Note that we do not transform the input to lower case as it was not required.

All of these steps are included in the mapper function of the job.

```java

public static class Map extends Mapper<LongWritable, Text, LongWritable, Text> {
      public Set<String> excluded_words;
      public Set<String> unique_words;
      public LongWritable unique_key = new LongWritable(0);
      
      /*
       * We use the setup method in order to open the stopwords file and spill its content only once.
       */
      @Override
      public void setup(Context context) throws IOException, InterruptedException {
     	 File File_stop_words = new File("/home/cloudera/Desktop/Massive-Data-Processing/Assignment_2/stop_words.txt");
     	 BufferedReader DocumentReader = new BufferedReader(new FileReader(File_stop_words)); // we use BuffferedReader to read our stop words file.  			 
     	 
     	 // words present in the stop words file are added in a list that cannot contain duplicates
     	 excluded_words = new HashSet<String>(); 
     	 String case_ = null;
     	 while ((case_ = DocumentReader.readLine()) != null) { 
     		 excluded_words.add(case_);     		     	 
     	 }
     	 DocumentReader.close();  
      }
      
      /*
       * The map function takes as input a line of the document
       * It checks that it is not empty, parses it, removes the stopwords and duplicates
       * It outputs the line number as key (i.e. Document ID) and the word as value  
       */
      @Override
      public void map(LongWritable key, Text value, Context context)
              throws IOException, InterruptedException {
    	 
    	 if (!value.toString().replaceAll("[^A-Za-z0-9]"," ").isEmpty()) {  // check it is not an empty line
    		     		 
    		 unique_words = new HashSet<String>();  // we keep each unique word by transforming the sentence in a list with no duplicates
    		 
    		 context.getCounter(LINE_COUNTER.SINGLE_LINE).increment(1);  // since line is not empty we increment our counter
        	 unique_key.set(context.getCounter((LINE_COUNTER.SINGLE_LINE)).getValue());  // we use the line number as the output key: one key = one documentID
    		 
    		 for (String token: value.toString().replaceAll("[^A-Za-z0-9]"," ").split("\\s+")) {
    			 
    			 if (!excluded_words.contains(token)){  // we check that the word is not a stop word
    				 unique_words.add(token);    	    // we add it to a list that cannot contain duplicates			 
    			 }
    		 }
    	 
    		 for (String u_word: unique_words){
    			 
    			 if (!u_word.isEmpty()){
    				 context.write(unique_key, new Text(u_word));
```

#### (b)(1) Store on HDFS the number of output records (i.e., total lines).
We use a counter in the reducer whose value is then stored in a text file. To do this we implement a cleanup method at the end of the reduce phase:
```java
protected void cleanup(Context context) throws IOException, InterruptedException {
    	  try{
    		 Path path = new Path("number_lines/nb_lines.txt");
    		 FileSystem file_out = FileSystem.get(new Configuration());
    		 BufferedWriter DocumentWriter = new BufferedWriter(new OutputStreamWriter(file_out.create(path,true)));
    		 Long nb_line = context.getCounter(FINAL_COUNTER.FINAL_LINE).getValue();
    		 System.out.println(nb_line);
    		 DocumentWriter.write(nb_line.toString() + "\n");
    		 DocumentWriter.close();
    	  }
    	  catch(Exception exception){
	            System.out.println("Line Counter Failed");
```

There are 115105 lines in the pre-processed file

#### (c)(7) Order the tokens of each line in ascending order of global frequency.

To get the global frequency of each word in the text we use the wordcount job from assignemnt 0 (available [here](https://github.com/paulvercoustre/Massive-Data-Processing/blob/master/Assignment_2/src/pre_process/word_frequency.java)) which we compute on pg100.txt.
We import it in the reduce method and use it to order the words of each sentence as required with a TreeMap. 
```java
public static class Reduce extends Reducer<LongWritable, Text, LongWritable, Text> {
	  private HashMap<String, Integer> word_frequency = new HashMap<String,Integer>();  // the words and their frequency is spilled in a dictionary-like data structure
	  
	  /*
	  * We use the setup method in order to open the word frequency file only once and store it in a HashMap.
	  */
	  @Override
	  public void setup(Context context) throws IOException, InterruptedException {
		  File File_word_frequency = new File("/home/cloudera/Desktop/Massive-Data-Processing/Assignment_2/word_frequency.txt");
	      BufferedReader DocumentReader = new BufferedReader(new FileReader(File_word_frequency));	
	       
	      String case_ = null;
	       
	      while ((case_ = DocumentReader.readLine()) != null) {
	    	  String[] word = case_.split(",");  // word & frequencies are comma separated in file 
	    	  word_frequency.put(word[0], Integer.valueOf(word[1]));  // store the word as key and frequency as value
	      }
	   	  DocumentReader.close();	   	  
	  }
	   
	  /*
	   * The reduce class orders the word of each sentence (i.e. each input key) in increasing order of frequency with a TreeMap
	   * It outputs the line number as key and the full ordered sentence as output
	   */ 
      @Override
      public void reduce(LongWritable key, Iterable<Text> values, Context context)
              throws IOException, InterruptedException {
    	 
    	 String final_sentence = new String(); 
    	 SortedMap<Integer, String> sorted_sentence = new TreeMap<Integer, String>();
    	 
         for (Text val : values) {
        	 sorted_sentence.put(word_frequency.get(val.toString()), val.toString());  // (frequency, word) pairs are stored in a "sorted list"         	    
         }
         for (String token : sorted_sentence.values()) {
        	 final_sentence += token+";";        
         }
         final_sentence = final_sentence.substring(0, final_sentence.length()-1);  // get rid of the last semi-colon for next task...
         context.write(key, new Text(final_sentence));
	 context.getCounter(FINAL_COUNTER.FINAL_LINE).increment(1);
```

You can find the full code for this job [here](https://github.com/paulvercoustre/Massive-Data-Processing/blob/master/Assignment_2/src/pre_process/pre_processing.java).

Looking at the job's logs we see that the total run time is 32sec (see [Job Tracker](https://github.com/paulvercoustre/Massive-Data-Processing/blob/master/Assignment_2/img/Screen%20Shot%202017-03-18%20at%2006.09.35.png)).
![Job Tracker](https://github.com/paulvercoustre/Massive-Data-Processing/blob/master/Assignment_2/img/Screen%20Shot%202017-03-18%20at%2006.09.35.png)

The resulting output looks like this:
```
1,EBook;Complete;Works;Shakespeare;William;Gutenberg;Project;by
2,Shakespeare;William
3,anyone;anywhere;eBook;cost;use;This;at;no
4,restrictions;whatsoever;copy;almost;away;give;may;You;or;no
5,included;License;re;Gutenberg;Project;terms;under;use
6,online;gutenberg;www;eBook;org;or;at
7,COPYRIGHTED;Below;eBook;Gutenberg;Project;This
```

## Set-similarity joins

You are asked to efficiently identify all pairs of documents (d1, d2) that are similar (sim(d1, d2) >= t), given a similarity function sim and a similarity threshold t. Specifically, assume that:
- each output line of the pre-processing job is a unique document (line number is the document id),
- documents are represented as sets of words,
- sim(d1, d2) = Jaccard(d1, d2) = |d1 Ո d2| / |d1 U d2|,
- t = 0.8.

Example:
Input: d1: “I have a dog” d2: “I have a cat” d3: “I have a big dog”
- sim(d1, d2) = 3/5 < 0.8 -> d1 and d2 are not similar 
- sim(d2, d3) = 3/6 < 0.8 -> d2 and d3 are not similar 
- sim(d1, d3) = 4/5 = 0.8 -> d1 and d3 are similar
Output: (d1, d3), 0.8

#### (a)(40) Perform all pair-wise comparisons between documents, using the following technique: Each document is handled by a single mapper (remember that lines are used to represent documents in this assignment). The map method should emit, for each document, the document id along with one other document id as a key (one such pair for each other document in the corpus) and the document’s content as a value. In the reduce phase, perform the Jaccard computations for all/some selected pairs. Output only similar pairs on HDFS, in TextOutputFormat. Make sure that the same pair of documents is compared no more than once. Report the execution time and the number of performed comparisons.

To implement this job we take our pre-processed file as input as well as the text file containing the number of lines in the document (via the setup method). Since there are 115105 lines in the original pre-processed file, we will work with a smaller version containing 1000 lines to ease computation.

We implement the following mapper:
```java 
public static class Map extends Mapper<LongWritable, Text, Text, Text> {
	  public Integer number_lines = 0;
      
      /*
       * We use the setup method in order to open the file containing the number of lines 
       * and spill its content only once.
       */
	  @Override
      public void setup(Context context) throws IOException, InterruptedException {
     	 File File_nb_lines = new File("/home/cloudera/Desktop/Massive-Data-Processing/Assignment_2/nb_lines.txt");
     	 BufferedReader DocumentReader = new BufferedReader(new FileReader(File_nb_lines)); // we use BuffferedReader to read our stop words file.  			 
     	 
     	 // we assign the value in the document to an integer variable     
     	 String case_ = null;
     	 while ((case_ = DocumentReader.readLine()) != null) { 
     		 number_lines = Integer.parseInt(case_);	     	 
     	 }
     	 DocumentReader.close();  
      }
      
      /*
       * The map function takes as input a line of the document
       * It parses it, taking the line number and sentence separately 
       * It outputs all key combinations (given the number of lines) and the sentence as value 
       */
      @Override
      public void map(LongWritable key, Text value, Context context)
              throws IOException, InterruptedException {
    	  
    	 Integer key_1 = Integer.parseInt(value.toString().split(",")[0]); // item before comma is the key 
    	 Text sentence = new Text(value.toString().split(",")[1]);         // item after comma is the sentence
    	 
    	 for (Integer key_2 = 1; key_2 < number_lines + 1; key_2++) {  // we get all possible combinations of lines
    		 if (key_2 != key_1){  // we don't want to compute the similarity of a line with itself...                
    			 Integer max_key = Math.max(key_1, key_2);
    			 Integer min_key = Math.min(key_1, key_2);
    			 // we order the keys so as not to get duplicates
    			 // we will get both sentences for each key combination in the reducer
    			 Text combination = new Text(min_key.toString() + "," + max_key.toString());
    			 context.write(combination, sentence);
```

The mapper outputs, for each input value (i.e. sentence) all the possible combinations of comparisons as keys (line numbers) and the sentence as value. The line numbers are always written in ascending order so as to facilitate computation in the reduce step. 

We implement the following reduce method: 
```java
   public static class Reduce extends Reducer<Text, Text, Text, Text> {
	  public static Integer numberOfComparisons = 0;
	  /*
	   * The reduce class computes the Jaccard similarity between sentences
	   * The input keys are tuples (key_1, key_2)
	   * Each input key has 2 values: the 2 sentences for which we want to compute similarity
	   * It outputs the 2 sentences as key and the similarity score (if sim(key_1, key_2) > 0.8) as value
	   */ 
      @Override
      public void reduce(Text key, Iterable<Text> values, Context context)
              throws IOException, InterruptedException {
    	 
    	 HashSet<String> unique_words = new HashSet<String>();  // we store words in a list with no duplicates
    	 List<String> union = new ArrayList<String>();    	    // we store all the words from both sentences
    	 float jaccard_sim = 0;
    	 
         for (Text val : values) {                              // for both sentences
        	 for (String token : val.toString().split(";")){    // parse the sentence
        		 unique_words.add(token);         				// add words to HashSet
        		 union.add(token);								// add words to List
        	 }        	 
         }        
         jaccard_sim = ((float) union.size() - unique_words.size()) / ((float)union.size());  // compute the Jaccard similarity
         numberOfComparisons += 1;
         
         if (jaccard_sim > 0.25){  // we use 0.25 as the cutoff point because otherwise no pairs come up 								
        	 context.write(key, new Text(" similarity : " + Float.toString(jaccard_sim)));
```

When we perform all of the above steps on the sample input file (1000 lines), the resulting pre-processed file has 855 lines, and when computing the Jaccard similarities, 365813 comparisons are made. Note that we deliberately lowered the similarity cutoff point to 0.25 because otherwise no pair would get written on the output file.

The full code for this job is available [here](https://github.com/paulvercoustre/Massive-Data-Processing/blob/master/Assignment_2/src/similarity_joins/naive_similarity.java).

Looking at the job's logs we see that the total run time is 28sec (see [Job Tracker](https://github.com/paulvercoustre/Massive-Data-Processing/blob/master/Assignment_2/img/Screen%20Shot%202017-03-19%20at%2020.50.04.png)).
![Job Tracker](https://github.com/paulvercoustre/Massive-Data-Processing/blob/master/Assignment_2/img/Screen%20Shot%202017-03-19%20at%2020.50.04.png)

The resulting output looks like this:
``` 
[cloudera@quickstart Jar_Files]$ hadoop fs -cat output/part* | head
1,31, similarity : 0.31578946
1,9, similarity : 0.3529412
10,132, similarity : 0.33333334
14,122, similarity : 0.33333334
14,22, similarity : 0.33333334
14,38, similarity : 0.33333334
15,17, similarity : 0.2631579
2,10, similarity : 0.4
2,132, similarity : 0.4
2,20, similarity : 0.33333334
```

#### b) (40) Create an inverted index, only for the first |d| - ⌈t |d|⌉ + 1 words of each document d (remember that they are stored in ascending order of frequency). In your reducer, compute the similarity of the document pairs. Output only similar pairs on HDFS, in TextOutputFormat. Report the execution time and the number of performed comparisons.

The intuition here is that we only want to compute similarity between documents if they have words in common to gain time.

The mapping method takes as input the pre-processed file and output an inverted index of the words in that file. Specifically we implement this mapper:
```java
public static class Map extends Mapper<LongWritable, Text, Text, Text> {
           
      /*
       * The map function takes as input a line of the document
       * It parses it, taking the line number and sentence separately 
       * It outputs all key, value pairs in the inverted index format 
       */
      @Override
      public void map(LongWritable key, Text value, Context context)
              throws IOException, InterruptedException {
    	  
    	 Integer key_1 = Integer.parseInt(value.toString().split(",")[0]); // item before comma is the key 
    	 Text sentence = new Text(value.toString().split(",")[1]);         // item after comma is the sentence
    	 
    	 Integer d = sentence.toString().split(";").length;                // number of words in the sentence (d)
    	 float t = (float) 0.25;										   // similarity threshold (t)
    	 
    	 Integer cutoff = d - Math.round(t * d) + 1;					   // compute the filter point
    	 Integer cut = 0;
    	 
    	 if (d == 1) {  // special case when sentence is 1 word
    		 cut = 1;
    	 } else {    		
    		 cut = cutoff;
    	 }
    	 
    	 for (Integer i = 0; i < cut; i++){
    			 String token = sentence.toString().split(";")[i];
    			 context.write(new Text(token), new Text(key_1.toString()));  // output in inverted index format
```

The reduced function takes as input a word as key and a list documents it is contained in. We compute the similarity between all the documents in that list (ensuring they have not been compared before) and output documents that are similar.

Specifically we implement the following reducer:
```java
public static class Reduce extends Reducer<Text, Text, Text, Text> {
	  public static Integer numberOfComparisons = 0;
	  public HashSet<String> comparisons_record = new HashSet<String>();  // we store the comparisons made
	  public HashMap<Integer, String> documents = new HashMap<Integer, String>(); // we store the documents 
	  
	  @Override
	  
	  public void setup(Context context) throws IOException, InterruptedException {
		  File Processed_File = new File("/home/cloudera/Desktop/Massive-Data-Processing/Assignment_2/processed_pg100.txt");
	      BufferedReader DocumentReader = new BufferedReader(new FileReader(Processed_File)); 			 
	     	 
	   	  // we store each document in a HashMap     
	   	  String case_ = null;
    	  while ((case_ = DocumentReader.readLine()) != null) {
    		  String[] document = case_.split(",");                      // documentID & document are comma separated in file 
	    	  documents.put(Integer.valueOf(document[0]), document[1]);  // store the docID as key and doc as value
    	  }
    	  DocumentReader.close();
	  }
	  
	  /*
	   * For every word (key), the reduce class computes the Jaccard similarity ..
	   * .. between sentences contained in the values of that word 
	   * A record of the comparisons made is kept in order not to compare sentences twice
	   * It outputs the 2 sentences as key and the similarity score (if sim(key_1, key_2) > 0.8) as value
	   */ 
      @Override
      public void reduce(Text key, Iterable<Text> values, Context context)
              throws IOException, InterruptedException {
    	 
    	 float jaccard_sim = 0;
    	 
    	 HashSet<String> doc_list = new HashSet<String>();  // we store the docIDs of doc containing that word
    	 
    	 for (Text val : values) {
    		 doc_list.add(val.toString());
    	 }
    	 
         for (String doc_l1 : doc_list) {
        	 String doc_id1 = doc_l1; 
        	 for (String doc_l2: doc_list) {
        		 String doc_id2 = doc_l2;
        		 Integer max_key = Math.max(Integer.valueOf(doc_id1), Integer.valueOf(doc_id2));
        		 Integer min_key = Math.min(Integer.valueOf(doc_id1), Integer.valueOf(doc_id2));
        			 
        		 // we do not compare a document with itself.. or if it has already been compared
       			 if (!doc_id1.equals(doc_id2) && !comparisons_record.contains(min_key + "," + max_key) ) {      			 
       				 HashSet<String> unique_words = new HashSet<String>();  // we store words in a list with no duplicates
       				 List<String> union = new ArrayList<String>();          // we store all the words from both sentences
       				 
       				 for (String token_1 : documents.get(min_key).toString().split(";")) {
       					 unique_words.add(token_1);  // spill doc 1's words
       					 union.add(token_1);
       				 }
       				 for (String token_2 : documents.get(max_key).toString().split(";")) {
        				 unique_words.add(token_2);  // spill doc 2's words
        				 union.add(token_2);
         			 }
        				 
       				 comparisons_record.add(min_key + "," + max_key);  // record that we have made this comparison
       				 jaccard_sim = ((float) union.size() - unique_words.size()) / ((float)union.size());  // compute the Jaccard similarity
       		         numberOfComparisons += 1;
        		         
       		         if (jaccard_sim > 0.25){  // we use 0.25 as the cutoff point because otherwise no pairs come up 								
       		        	 context.write(new Text(min_key + "," + max_key), new Text(" similarity : " + Float.toString(jaccard_sim)));
```

We lauch the job on the same sample file as previously, with the same similarity threshold (25%) and find that 36743 comparisons were made. We note that we found the same number of similar pairs with both approaches. 

The full code for this job is available [here](https://github.com/paulvercoustre/Massive-Data-Processing/blob/master/Assignment_2/src/similarity_joins/inverted_index_similarity.java).

Looking at the job's logs we see that the total run time is 30sec (see [Job Tracker](https://github.com/paulvercoustre/Massive-Data-Processing/blob/master/Assignment_2/img/Screen%20Shot%202017-03-19%20at%2020.50.04.png)).
![Job Tracker](https://github.com/paulvercoustre/Massive-Data-Processing/blob/master/Assignment_2/img/Screen%20Shot%202017-03-19%20at%2020.50.04.png)


#### c) (10) Explain and justify the difference between a) and b) in the number of performed comparisons, as well as their difference in execution time.

