package com.lab.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends
		Mapper<NullWritable, Text, Text, IntWritable> {

	@Override
	public void map(NullWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		StringTokenizer token = new StringTokenizer(value.toString(), " ");
		while (token.hasMoreTokens()) {
			context.write(new Text(token.nextToken()), new IntWritable(1));
		}
	}
}
