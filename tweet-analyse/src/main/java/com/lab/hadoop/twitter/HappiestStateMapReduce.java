package com.lab.hadoop.twitter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HappiestStateMapReduce {
	
	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.out
					.println("Usage: java com.lab.hadoop.twitter.HappiestState [dictionary file] [tweets file]");
			System.exit(-1);
		}
		
		FileSystem fs = FileSystem.get(new Configuration());		
		Path tweets = new Path(args[1]);
		Path output = new Path(args[2]);
		fs.delete(output, true);
		
		Job job = new Job();
		job.setJarByClass(HappiestStateMapReduce.class);
		job.setJobName("Happiest State MapReduce");
				
		FileInputFormat.addInputPath(job, tweets);
		FileOutputFormat.setOutputPath(job, output);
		
		job.setMapperClass(HappiestStateMapper.class);
		job.setReducerClass(HappiestStateReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
