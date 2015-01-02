package com.lab.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class IndexDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance();
		job.setJobName("Index Builder");
		job.setSpeculativeExecution(false);
		job.setJarByClass(getClass());

		// Set Input and Output paths
		Path output = new Path(args[1].toString());
		FileSystem.get(new Configuration()).delete(output, true);
		FileInputFormat.addInputPath(job, new Path(args[0].toString()));
		FileOutputFormat.setOutputPath(job, output);
		
		// Use TextInputFormat
		job.setInputFormatClass(TextInputFormat.class);

		// Mapper has no output
		job.setMapperClass(IndexMapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setNumReduceTasks(0);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err
					.println("Usage: com.lab.hadoop.IndexDriver <input path> <output path>");
			System.exit(-1);
		}
		int exitCode = ToolRunner.run(new IndexDriver(), args);
		System.exit(exitCode);
	}
}