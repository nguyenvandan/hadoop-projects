package com.lab.hadoop.twitter.reduce;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HappiestStateReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {

	/**
	 * Called once at the start of the task.
	 */
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		System.out.println(context.getTaskAttemptID()
				+ " - Reducer Starting setup " + new Date());
	}

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int stateScore = 0;
		for (IntWritable score : values) {
			stateScore += score.get();
		}
		context.write(key, new IntWritable(stateScore));
	}

	/**
	 * Called once at the end of the task.
	 */
	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		System.out.println(context.getTaskAttemptID() + " - Reducer Finsished "
				+ new Date());
	}
}
