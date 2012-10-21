/**
 * 
 */
package edu.cs.iit.cs495.wikiranker.parser;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.mahout.text.wikipedia.XmlInputFormat;
import org.mortbay.xml.XmlParser;

/**
 * @author cshou
 *
 */
public class WikiParser extends Configured {
	
	public void runParser(Configuration conf, String in, String out) throws Exception {
		
		conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
		conf.set(XmlInputFormat.END_TAG_KEY, "</page>");
		
		Job job = new Job(conf, "Parsing Wiki");
		job.setJarByClass(XmlParser.class);
		
		FileInputFormat.setInputPaths(job, new Path(in));
		FileOutputFormat.setOutputPath(job, new Path(out));
		
		job.setInputFormatClass(XmlInputFormat.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(ParsingMapper.class);
		job.setReducerClass(ParsingReducer.class);
		
		job.waitForCompletion(true);
	}
	
	public static class ParsingMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		private final static Pattern linkPattern = Pattern.compile("\\[\\[.+?\\]\\]");
		
		@Override
		public void map (LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
			
			String[] titleAndText = parseTitleAndText(value.toString());
			
			String pageString = titleAndText[0];	// Get the title
			Text page = new Text(pageString.replace(' ', '_'));
			
			boolean enter = false;
			
			// Find link pattern in text
			Matcher matcher = linkPattern.matcher(titleAndText[1]);
			
			while (matcher.find()) {
				
				if (enter == false)
					enter = true;
				
				String current = getRealLink(matcher.group());
				
				if (current == null || current.isEmpty()) {
					continue;
				}
				
				current = current.replace(' ', '_');
				
				context.write(page, new Text(current));
				
			}
			
			if (!enter) {
				context.write(page, new Text(""));
			}
		}
		
		private String getRealLink(String str) {
			
			String link = str.substring(2, str.length() - 2);
			
			if (link.contains(":")) {

				link = null;
			}
			else if (link.contains("|")) {

				link = link.substring(0, link.indexOf("|")).trim();
			}
			
			return link;
		}
		
		/* Get the String between <title>
		 * and the String between <text>
		 * Ignore others
		 */
		private String[] parseTitleAndText (String content) {
			String[] str = content.split("\n");
			
			boolean intext = false;
			boolean intitle = false;
			
			String title = "";
			String text = "";
			
			int i = 0;
			while (i < str.length) {
				String current = str[i];
				if (current.indexOf("</title>") != -1) {
					title += current;
					title = stripTag(title);
					intitle = false;
				}
				else if (current.indexOf("<title>") != -1) {
					title += current;
					intitle = true;
				}
				else if (intitle) {
					title += current;
				}
				else if (current.indexOf("</text>") != -1) {
					text += current;
					text = stripTag(text);
					intext = false;
				}
				else if (current.indexOf("<text") != -1) {
					intext = true;
					text += current;
				}
				else if (intext) {
					text += current;
				}
				
				i++;
			}
			
			String[] titleAndText = new String[] {title.trim(), text.trim()};
			
			return titleAndText;
		}
		
		private String stripTag(String str) {
			boolean ignore = false;
	        StringBuffer strb = new StringBuffer();
	        for ( int i = 0; i < str.length(); i++ )
	        {
	            char ch = str.charAt( i );
	            if ( ch == '<' )
	            {
	                ignore = true;
	            }
	            else if ( ch == '>' )
	            {
	                ignore = false;
	            }
	            else if ( !ignore )
	            {
	                strb.append( ch );
	            }
	        }  
	        
	        String strP = strb.toString();       
	        
	        return strP;
		}
	}
	
	public static class ParsingReducer extends Reducer<Text, Text, Text, Text> {
		
		@Override
		public void reduce (Text key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {
			
			String links = "1.0;";
			boolean first = true;
			
			for (Text value : values) {
				if (!value.toString().isEmpty()) {
					if (!first) {
						links += ",";
					}
					links += value.toString();
					first = false;
				}
			}
			
			context.write(key, new Text(links));
			
		}
	}
	
}
