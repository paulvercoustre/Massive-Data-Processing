# Massive-Data-Processing - 2nd Assignement
Due 17th of March 2017

## Pre-processing the input
In this assignment, you will use the document corpus of pg100.txt (from http://www.gutenberg.org/cache/epub/100/pg100.txt), as in your previous assignments, assuming that each line represents a distinct document (treat the line number as a document id). Implement a pre-processing job in which you will:

#### (a)(2) Remove all stopwords (you can use the stopwords file of your previous assignment), special characters (keep only [a-z],[A-Z] and [0-9]) and keep each unique word only once per line. Don’t keep empty lines.

We start by creating a new project, package and class similarly to what was done in assignment 0 & 1.
To find the stopwords in pg100.txt we run the same "stopwords" job from the previous assignment on this particular text only. We obtain [this file]().

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


#### (c) (7) Order the tokens of each line in ascending order of global frequency.

To get the global frequency of each word in the text we use the wordcount job from assignemnt 0 (available [here]()) which we compute on pg100.txt.
We import it in the reduce method and use it to order the words of each sentence as required. 
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
      }
   }
}   
```   

You can find the full code for this job [here]().

Looking at the job's logs we see that the total run time is 1min 35sec (see [Job Tracker]()).

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

