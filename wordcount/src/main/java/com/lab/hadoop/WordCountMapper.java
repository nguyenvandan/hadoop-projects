package com.lab.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {
	
	public WordCountMapper() throws IOException {
		// TODO Auto-generated constructor stub
		super();
		System.out.println("WordCountMapper()");
		Configuration conf = new Configuration();
		System.out.println(conf.get("test"));
	}

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException, NullPointerException {
		
		System.out.println("Call Mapper :" + value.toString());
		
		StringTokenizer token = new StringTokenizer(value.toString(), " ");
		while (token.hasMoreTokens()) {
			context.write(new Text(token.nextToken()), new IntWritable(1));
		}
	}
}
