/**
 * 
 */
package edu.cs.iit.cs495.wikiranker.sorter;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * @author cshou
 *
 */
public class ResultSorter extends Configured {

	public void runSorter (Configuration conf, String in, String out) throws Exception {
		
		conf.set("key.value.separator.in.input.line", "\t");
		
		Job job = new Job(conf, "ResultRanker");
		job.setJarByClass(ResultSorter.class);
		
		FileInputFormat.setInputPaths(job, new Path(in));
		FileOutputFormat.setOutputPath(job, new Path(out));
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(DoubleWritable.class);
		job.setOutputValueClass(Text.class);
		
		
		job.setMapperClass(ResultMapper.class);
		
		job.waitForCompletion(true);
	}
	
	public static class ResultMapper extends Mapper<Text, Text, DoubleWritable, Text> {
		
		@Override
		public void map (Text key, Text value, Context context) 
				throws IOException, InterruptedException {
			
			String val = value.toString();
			if (!val.isEmpty() && val != null) {
				String[] params = val.split(";");
				double score = Double.parseDouble(params[0]);
				context.write(new DoubleWritable(score), key);
			}
			
		}
		
	}
	
}
