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
      
      System.out.println("Number of Comparisons : " + Reduce.numberOfComparisons);
      
      return 0;
   }
   
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
    	 float t = (float) 0.8;										   // similarity threshold (t)
    	 
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
    	 }
      }
   }         
   
   public static class Reduce extends Reducer<Text, Text, Text, Text> {
	  public static Integer numberOfComparisons = 0;
	  public HashSet<String> comparisons_record = new HashSet<String>();  // we store the comparisons made
	  public HashMap<Integer, String> documents = new HashMap<Integer, String>(); // we store the documents 
	  
	  @Override
	  
	  public void setup(Context context) throws IOException, InterruptedException {
		  File Processed_File = new File("/home/cloudera/Desktop/Massive-Data-Processing/Assignment_2/full_processed.txt");
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
        		         
       		         if (jaccard_sim > 0.8){  // we use 0.25 as the cutoff point because otherwise no pairs come up 								
       		        	 context.write(new Text(min_key + "," + max_key), new Text(" similarity : " + Float.toString(jaccard_sim)));
       		         }	 
        				 
       			 }
        	}
         }        	         	                                            
      }      
   }
}