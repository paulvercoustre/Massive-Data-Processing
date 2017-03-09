package inverted_index;

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

public class InvertedIndex_QB extends Configured implements Tool {
   public static void main(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      int res = ToolRunner.run(new Configuration(), new InvertedIndex_QB(), args);
      
      System.exit(res);
   }

   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      Configuration conf = new Configuration();
      conf.setBoolean(Job.MAP_OUTPUT_COMPRESS, true); // we compress the map output using BZip2Codec
      conf.setClass(Job.MAP_OUTPUT_COMPRESS_CODEC, BZip2Codec.class,
      CompressionCodec.class);
      Job job = new Job(conf);
      job.setJarByClass(InvertedIndex_QB.class);
      job.setOutputKeyClass(Text.class); // setting the output key to text
      job.setOutputValueClass(Text.class); // setting the output value to text

      job.setMapperClass(Map.class);
      job.setReducerClass(Reduce.class);
      job.setNumReduceTasks(1); // we set the number of reducers to 50
      job.setCombinerClass(Reduce.class); // we add a combiner
      
      job.getConfiguration().set("mapreduce.output.textoutputformat.separator", " => "); // getting a clearer separator

      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);   

      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      job.waitForCompletion(true);
      
      return 0;
   }
   
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
   }