package edu.cs.iit.cs495.wikiranker;

import java.text.DecimalFormat;
import java.text.NumberFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.cs.iit.cs495.wikiranker.chainer.CoreRanker;
import edu.cs.iit.cs495.wikiranker.parser.WikiParser;
import edu.cs.iit.cs495.wikiranker.sorter.ResultSorter;

public class WikiRanker {

	private final static NumberFormat nf = new DecimalFormat("00");
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		// String input = args[0];
		// String output = args[1];
		// String input = ".";
		// String output = "output";
		
		WikiParser xp = new WikiParser();
		CoreRanker cr = new CoreRanker();
		ResultSorter rr = new ResultSorter();
		
		Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    String input = otherArgs[0];
	    String output = otherArgs[1];
		
		xp.runParser(conf, input + "/input", input + "/ranking/iter00");
		
		int runs = 0;
        for (; runs < 5; runs++) {
            cr.runRanker(conf, input + "/ranking/iter"+nf.format(runs), input + "/ranking/iter"+nf.format(runs + 1));
        }
		
        rr.runSorter(conf, input + "/ranking/iter"+nf.format(runs), input + "/" + output);
        
	}
}