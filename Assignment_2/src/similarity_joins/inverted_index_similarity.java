package similarity_joins;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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

public class inverted_index_similarity extends Configured implements Tool {
   public static void main(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      int res = ToolRunner.run(new Configuration(), new inverted_index_similarity(), args);
      
      System.exit(res);
   }
   
   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      Configuration conf = new Configuration();
      Job job = new Job(conf);
      job.setJarByClass(inverted_index_similarity.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);

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
      
      System.out.println("Number of Comparisons : " + Reduce.numberOfComparisons);
      
      return 0;
   }
   
   public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
           
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
    	 
    	 Integer d = sentence.toString().split(";").length;                // number of words in the sentence
    	 float t = (float) 0.25;										   // similarity threshold
    	 
    	 Integer cutoff = d - Math.round(t * d) + 1;					   // compute the filter point
    	 
    	 for (Integer i = 0; i < cutoff; i++){
    		 String token = sentence.toString().split(";")[i];
    		 context.write(new Text(token), new IntWritable(key_1));
    		 System.out.println(token + ":" + key_1);
    	 }
      }
   }         
   
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
         } 
      }      
   }
}