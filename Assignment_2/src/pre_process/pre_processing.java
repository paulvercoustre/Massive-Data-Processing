package pre_process;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class pre_processing extends Configured implements Tool {
   public static void main(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      int res = ToolRunner.run(new Configuration(), new pre_processing(), args);
      
      System.exit(res);
   }
   
   public static enum LINE_COUNTER {   // we define an enum type that will count the number of lines
	   SINGLE_LINE
   }

   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      Configuration conf = new Configuration();
      Job job = new Job(conf);
      job.setJarByClass(pre_processing.class);
      job.setOutputKeyClass(LongWritable.class);
      job.setOutputValueClass(Text.class);

      job.setMapperClass(Map.class);
      job.setReducerClass(Reduce.class);
      
      job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ","); // getting a csv formatted output

      job.setInputFormatClass(TextInputFormat.class); // each line is considered a separate file
      job.setOutputFormatClass(TextOutputFormat.class);   

      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      
      FileSystem file_out = FileSystem.newInstance(getConf()); 

		if (file_out.exists(new Path(args[1]))) {      // check if there already is an output file
			file_out.delete(new Path(args[1]), true);  // if yes deletes it automatically to avoid manual deletion... 
		}
      
      job.waitForCompletion(true);
      
      return 0;
   }
   
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
    			 }	          
    		 }
    	 }	 
      }
   }         
   
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