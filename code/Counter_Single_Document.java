package inverted_index_qb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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

public class FullInvertedIndex extends Configured implements Tool {
   public static void main(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      int res = ToolRunner.run(new Configuration(), new FullInvertedIndex(), args);
      
      System.exit(res);
   }
   
   public static enum SINGLE_DOC_COUNTER {   // we define an enum type 
	   SINGLE_DOCUMENT
   }

   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      Configuration conf = new Configuration();
      conf.setBoolean(Job.MAP_OUTPUT_COMPRESS, true); 
      conf.setClass(Job.MAP_OUTPUT_COMPRESS_CODEC, BZip2Codec.class, // we compress the map output using BZip2Codec
      CompressionCodec.class);
      Job job = new Job(conf);
      job.setJarByClass(FullInvertedIndex.class);
      job.setOutputKeyClass(Text.class); // setting the output key to text
      job.setOutputValueClass(Text.class); // setting the output value to text

      job.setMapperClass(Map.class);
      job.setReducerClass(Reduce.class);
      job.setNumReduceTasks(1); // we set the number of reducers to 1
      job.setCombinerClass(Reduce.class); // we add a combiner
      
      job.getConfiguration().set("mapreduce.output.textoutputformat.separator", " => "); // getting a clearer separator

      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);   

      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      job.waitForCompletion(true);
      
      return 0;
   }
   
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
			  context.write(key, new Text(document_list.toString().replace("[","").replace("]","")));
    	  }
    	  
         }         
      }
   }
