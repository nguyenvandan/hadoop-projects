package com.lab.hadoop.twitter.reduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HappiestStateReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int stateScore = 0;
		for (IntWritable score : values) {
			stateScore += score.get();
		}
		context.write(key, new IntWritable(stateScore));
	}
}
