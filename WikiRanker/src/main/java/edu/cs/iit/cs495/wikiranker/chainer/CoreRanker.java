/**
 * 
 */
package edu.cs.iit.cs495.wikiranker.chainer;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * @author cshou
 *
 */
public class CoreRanker extends Configured {
	
	public void runRanker (Configuration conf, String in, String out) throws Exception {
		
		conf.set("key.value.separator.in.input.line", "\t");
		
		Job job = new Job(conf, "Ranking Wiki");
		job.setJarByClass(CoreRanker.class);
		
		FileInputFormat.setInputPaths(job, new Path(in));
		FileOutputFormat.setOutputPath(job, new Path(out));
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		
		job.setMapperClass(RankingMapper.class);
		job.setReducerClass(RankingReducer.class);
		
		job.waitForCompletion(true);
		
	}
	
	public static class RankingMapper extends Mapper<Text, Text, Text, Text> {
		
		@Override
		public void map (Text key, Text value, Context context) 
				throws IOException, InterruptedException {
			
			String val = value.toString();
			String[] params = val.split(";");
			String score = params[0];
			String[] outlinks = null;
			if (params.length > 1) {
				if (!params[1].isEmpty() && params[1] != null)
					outlinks = params[1].split(",");
			}
			
			if (outlinks != null)
				context.write(key, new Text("@!" + params[1]));
			else
				context.write(key, new Text("@!"));
			
			if (outlinks != null) {
				int numOfLinks = outlinks.length;
				for (String link : outlinks) {
					
					if (link.isEmpty())
						continue;
					
					String format = key.toString() + ";" + score + ";" + Integer.toString(numOfLinks);
					context.write(new Text(link), new Text(format));
				}
			}
		}
	}
	
	public static class RankingReducer extends Reducer<Text, Text, Text, Text> {
		
		@Override
		public void reduce (Text key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {
			
			Double base = 0.15;
			boolean isPage = false;
			String outlinks = "";
			
			for (Text text : values) {
				String txt = text.toString();
				if (txt.startsWith("@!")) {
					isPage = true;
					if (txt.length() > 2) {
						outlinks = txt.substring(2);
					}
					continue;
				}
				String params[] = txt.split(";");
				double score = Double.parseDouble(params[1]);
				double num = Double.parseDouble(params[2]);
				base += 0.85 * score / num;
			}
			
			if (isPage) {
				context.write(key, new Text(base + ";" + outlinks));
			}
		}
	}

}
