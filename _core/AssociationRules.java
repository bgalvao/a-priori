package _core;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;

import org.apache.hadoop.fs.Path;       
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;             
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;      
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

// project
import frequentItemsetsFinder.ItemsetsReducer;
import frequentItemsetsFinder.MONO_ItemsetsReducer;

public class AssociationRules {
	                              
	// user parameters               
	public static int MIN_SUPPORT = 100;                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         
	public static double MIN_CONF = (double) 0.1;
	private boolean MONO = true; // set to true to use complete monotonic property in finding frequent itemsets.
	private boolean ANTI_MONO = true; // set to true to use RHS anti-monotone property for rule extraction.
	
	// non-user assetss
	public static TreeMap<String, Integer> singletToIndex = new TreeMap<String, Integer>(); // a tree map for sorted itemset generator
	public static TreeMap<Integer, String> indexToSinglet = new TreeMap<Integer, String>();
	public static HashMap<Integer, SortedSet<Integer>> singletIndexToLines = new HashMap<Integer, SortedSet<Integer>>(); // a hashmap to optimize lookups
	public static Set<Set<String>> blacklist = new HashSet<Set<String>>();
	public static int itr = 1;
	
	// methods
	public static void main(String[] args) throws Exception {

		long start = System.currentTimeMillis();
		
		AssociationRules run = new AssociationRules();
		
		// Job 1 - extract the supported singlets and their lines
		long start1 = System.currentTimeMillis();
		System.out.println("Creating the base...");
		run.initialBaseCreator("_data/in", "_data/itemsetSize1");
		long end1 = System.currentTimeMillis();
		System.out.println("MR(1) time -> " + ((end1 - start1)/1000.0000) + "s");
		System.out.println("------------------------------------------------------------------------");	
		
		// Job 2 - extract support itemsets of size "itr" and their lines
		System.out.println("Finding frequent itemsets...");
		long start2 = System.currentTimeMillis();
		long lines = 0;
		do {
			lines = run.frequentItemsetFinder("_data/itemsetSize" + itr, "_data/itemsetSize" + (itr + 1));
			itr++;
		} while (lines > 0);
		long end2 = System.currentTimeMillis();
		System.out.println("MR(2) overall time -> " + ((end2 - start2)/1000.0000) + "s");
		System.out.println("------------------------------------------------------------------------");
		
		// Job 3: Gather the counts and extract the rules
		System.out.println("Extracting the rules...");
		long start3 = System.currentTimeMillis();
		run.ruleExtractor("_data/itemsetSize", "_data/out");
		long end3 = System.currentTimeMillis();
		System.out.println("MR(3) time -> " + ((end3 - start3)/1000.0000) + "s");
		System.out.println("------------------------------------------------------------------------");
		
		long end = System.currentTimeMillis();
		
		System.out.printf("Total time with overhead: " + ((end-start)/1000.0000) + "s   <->   " + ((int)(end-start)/60000) + "min");

	}
	
	public void initialBaseCreator(String inputPath, String outputPath) throws IOException, Exception, InterruptedException {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Base Creation");
		
		job.setJarByClass(AssociationRules.class);
		
		job.setMapperClass(baseCreator.Mapper1.class);
		job.setReducerClass(baseCreator.Reducer1.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.waitForCompletion(true);
		boolean check = job.waitForCompletion(true);
		if (check) {
			System.out.println("MR(1) job completed");
		} else {
			System.out.println("MR(1) job failed to complete");
		}
	}
	
	public Long frequentItemsetFinder(String inputPath, String outputPath) throws IOException, Exception, InterruptedException {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Finding Frequent Itemsets");
		
		job.setJarByClass(AssociationRules.class);
		
		
		if (MONO) {
			job.setMapperClass(frequentItemsetsFinder.MONO_ItemsetsMapper.class);
			job.setReducerClass(frequentItemsetsFinder.MONO_ItemsetsReducer.class);
		} else {
			job.setMapperClass(frequentItemsetsFinder.ItemsetsMapper.class);
			job.setReducerClass(frequentItemsetsFinder.ItemsetsReducer.class);
		}
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.waitForCompletion(true);
		
		boolean check = job.waitForCompletion(true);
		if (check) {
			System.out.println("MR(2) job completed - itemsets of size " + (itr + 1));
		} else {
			System.out.println("MR(2) job failed to complete");
		}
		
		if (MONO) {
			return job.getCounters().findCounter(MONO_ItemsetsReducer.Counters.MONO_LINES_COUNT).getValue();
		} else {
			return job.getCounters().findCounter(ItemsetsReducer.Counters.LINES_COUNT).getValue();
		}
		
	}
	
	public void ruleExtractor(String inputPath, String outputPath) throws IOException, Exception, InterruptedException {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Rule Generator");
		
		job.setJarByClass(AssociationRules.class);
		
		
		job.setMapperClass(ruleExtractor.ExtractMapper.class);
		
		if (ANTI_MONO) {
			job.setReducerClass(ruleExtractor.ANTI_MONO_ExtractReducerPowersetLookup.class);
		} else {
			job.setReducerClass(ruleExtractor.ExtractReducerPowersetLookup.class);
		}
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// Taking it from various sources. By the rules, the last itr file should be empty.
		for (int i = 1; i < itr; i++) {
			FileInputFormat.addInputPath(job, new Path(inputPath+""+i));
		}
		
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		job.waitForCompletion(true);
		boolean check = job.waitForCompletion(true);
		if (check) {
			System.out.println("MR(3) job completed");
		} else {
			System.out.println("MR(3) job failed to complete");
		}
	}
}
