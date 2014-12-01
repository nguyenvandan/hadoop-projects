package com.lab.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {
	
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int nbrOccurences = 0;
		for (IntWritable value : values) {
			nbrOccurences += value.get();
		}
		context.write(key, new IntWritable(nbrOccurences));
	}
}
