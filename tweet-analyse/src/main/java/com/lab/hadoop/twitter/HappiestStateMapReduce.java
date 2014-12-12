package com.lab.hadoop.twitter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.lab.hadoop.twitter.map.HappiestStateMapper;
import com.lab.hadoop.twitter.reduce.HappiestStateReducer;

public class HappiestStateMapReduce {
	
	public static void main(String[] args) throws Exception {
		if (args.length < 3) {
			System.out
					.println("Usage: java com.lab.hadoop.twitter.HappiestState [dictionary file] [tweets file] [output]");
			System.exit(-1);
		}
		
		Configuration conf = new Configuration();
		conf.set("dictionaryURI", args[0]);
		Path tweets = new Path(args[1]);
		Path output = new Path(args[2]);
		FileSystem.get(new Configuration()).delete(output, true);
		
		conf.setBoolean("mapreduce.map.output.compress", true);
		conf.setBoolean("mapreduce.output.fileoutputformat.compress", false);
		conf.set("mapred.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");
				
		Job job = new Job(conf);
		job.setJarByClass(HappiestStateMapReduce.class);
		job.setJobName("Happiest State MapReduce");
				
		FileInputFormat.addInputPath(job, tweets);
		FileOutputFormat.setOutputPath(job, output);
		
		job.setMapperClass(HappiestStateMapper.class);
		job.setReducerClass(HappiestStateReducer.class);
		
		//job.setCombinerClass(HappiestStateReducer.class);
		
		//job.setInputFormatClass(CustomFileInputFormat.class);	
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
