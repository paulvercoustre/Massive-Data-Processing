package pre_process;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;

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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class stop_words extends Configured implements Tool {
   public static void main(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      int res = ToolRunner.run(new Configuration(), new stop_words(), args);
      
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
      job.setJarByClass(stop_words.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);

      job.setMapperClass(Map.class);
      job.setReducerClass(Reduce.class);
      job.setNumReduceTasks(1); // we set the number of reducers to 1
      job.setCombinerClass(Combine.class); // we add a combiner
      
      job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ","); // getting a csv formatted output

      job.setInputFormatClass(TextInputFormat.class); // each line is considered a separate file
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
   
   public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
      private final static IntWritable ONE = new IntWritable(1);
      private Text word = new Text();

      @Override
      public void map(LongWritable key, Text value, Context context)
              throws IOException, InterruptedException {
    	 // System.out.println(key);
    	 // System.out.println(value + "!! End of value !!");
         for (String val: value.toString().replaceAll("[^A-Za-z0-9]"," ").split("\\s+")) {
            word.set(val);
            context.write(word, ONE);
            // System.out.println(word + " : " + ONE );
         }
      }
   }

   public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
      @Override
      public void reduce(Text key, Iterable<IntWritable> values, Context context)
              throws IOException, InterruptedException {
    	 // System.out.println(key);
    	 
         int sum = 0;
         for (IntWritable val : values) {
        	// System.out.println(val);
            sum += val.get();
         }
         if (sum > 4000) {              // we only write keys associated to values > 4000 on the output file
        	 context.write(key, new IntWritable(sum));         
         }
         // System.out.println(key + ":" + sum);
         // System.out.println("End of task");
      }
   }
   
   public static class Combine extends Reducer<Text, IntWritable, Text, IntWritable> {
	   @Override
	   public void reduce(Text key, Iterable<IntWritable> values, Context context)
	   		throws IOException, InterruptedException {
		   
		   int sum = 0;
		   for (IntWritable val : values) {
			   sum += val.get();
		   }
		   
		   context.write(key, new IntWritable(sum)); // The combiner outputs all keys with no condition
			
	   }
   }
}