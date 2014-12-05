package com.lab.hadoop.twitter;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.lab.hadoop.tool.LoadData;

public class HappiestStateMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {
	
	private Map<String, Integer> statesScores = null;
	private LoadData loadedData = null;

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
	}
	
	
	private void checkHappiestState() {
		
	}
}
