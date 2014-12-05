package com.lab.hadoop.twitter;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HappiestStateReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {

}
