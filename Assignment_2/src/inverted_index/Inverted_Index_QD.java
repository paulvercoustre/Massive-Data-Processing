package inverted_index;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Set;

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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Inverted_Index_QD extends Configured implements Tool {
   public static void main(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      int res = ToolRunner.run(new Configuration(), new Inverted_Index_QD(), args);
      
      System.exit(res);
   }
   
   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      Configuration conf = new Configuration();
      conf.setBoolean(Job.MAP_OUTPUT_COMPRESS, true); 
      conf.setClass(Job.MAP_OUTPUT_COMPRESS_CODEC, BZip2Codec.class, // we compress the map output using BZip2Codec
      CompressionCodec.class);
      Job job = new Job(conf);
      job.setJarByClass(Inverted_Index_QD.class);
      job.setOutputKeyClass(Text.class); // setting the output key to text
      job.setOutputValueClass(Text.class); // setting the output value to text

      job.setMapperClass(Map.class);
      job.setReducerClass(Reduce.class);
      job.setNumReduceTasks(1); // we set the number of reducers to 1
      //job.setCombinerClass(Reduce.class); // we add a combiner
      
      job.getConfiguration().set("mapreduce.output.textoutputformat.separator", " => "); // getting a clearer separator

      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);   

      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      
      FileSystem file_out = FileSystem.newInstance(getConf()); 

		if (file_out.exists(new Path(args[1]))) {      // check of there already is an output file
			file_out.delete(new Path(args[1]), true);  // if yes deletes it automatically to avoid manual deletion... 
		}

      job.waitForCompletion(true);
      
      return 0;
   }
   
   public static class Map extends Mapper<LongWritable, Text, Text, Text> { // the output of the mapper is text for both key & value 
      private Text word = new Text(); // we define the variable corresponding to the output key
      private Text file_membership = new Text(); // we define the variable corresponding to the output value
      private Set<String> excluded_words;
      
      @Override
      public void setup(Context context) throws IOException, InterruptedException {
     	 File File_stop_words = new File("/home/cloudera/Desktop/Massive-Data-Processing/Assignment_2/stop_words.txt");
     	 BufferedReader DocumentReader = new BufferedReader(new FileReader(File_stop_words)); // we use BuffferedReader to read our stop words file.  			 
     	 
     	 // words present in the stop words file are added in a list that cannot contain duplicates
     	 excluded_words = new HashSet<String>(); 
     	 String case_ = null;
     	 while ((case_ = DocumentReader.readLine()) != null) { 
     		 excluded_words.add(case_.toLowerCase());
     		 System.out.println(case_);    	 
     	 }
     	 DocumentReader.close();  
      }

      @Override
      public void map(LongWritable key, Text value, Context context)
              throws IOException, InterruptedException {
    	  
    	 // System.out.println(key);
    	 
    	 String file_name = ((FileSplit) context.getInputSplit())
    			 .getPath().getName(); // we get the document name associated to the key...
         file_membership = new Text(file_name); // ... and store it in a text variable
         
         for (String token: value.toString().replaceAll("[^A-Za-z0-9]"," ").split("\\s+")) { // keep text and numbers only
            if (!excluded_words.contains(token.toLowerCase(Locale.ENGLISH))) { // we check that the word is not a stop word
            	word.set(token.toLowerCase());
            	// System.out.println(word + " : " + file_membership );
            }
            context.write(word, file_membership); // output in the inverted index format
         }	
         // System.out.println("End of Task");
      }
  }

   public static class Reduce extends Reducer<Text, Text, Text, Text> { // both input and output for both key and value is text
	  
	  @Override
      public void reduce(Text key, Iterable<Text> values, Context context)
              throws IOException, InterruptedException {
		  
		  	// System.out.println(key);
    	  
    	  	HashMap<String,Integer> document_list = new HashMap<String,Integer>(); // we create a new hashmap
    	  	
    	  	for (Text val : values) {
    	  		// System.out.println(val);
    	  		
    	  		if (document_list.containsKey(val.toString())) {
    	  			document_list.put(val.toString(), document_list.get(val.toString())+1); // iteratively increase the count 
    	  			// System.out.println(document_list);
    	  			// System.out.println(document_list.toString());
    	  		}
    			   
    	  		else {
    	  			document_list.put(val.toString(), 1);   // add the document name to the hashmap at the first iteration with value 1
    	  		}
    		  }
    		 
			context.write(key, new Text(document_list.toString().replace("{", "").replace("}", "").replace("=", "#")));
			// System.out.println(key +" : "+ document_list.toString() );
         }
      
   }
   public static class Combine extends Reducer <Text, Text, Text, Text> {
	   
   }
}
  
 