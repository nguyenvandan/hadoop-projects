package com.lab.hadoop.twitter.map;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import twitter4j.GeoLocation;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

public class HappiestStateMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {

	private LoadData loadedData = null;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		System.out.println(context.getTaskAttemptID() + " - Starting setup " + new Date());
		loadedData = new LoadData(context.getConfiguration().get("dictionaryURI"));
		System.out.println(context.getTaskAttemptID() + " - Ended setup " + new Date());
	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
//		String[] lines = value.toString().split(System.getProperty("line.separator"));
//		for (String line : lines) {
			try {
				Status status = TwitterObjectFactory.createStatus(value.toString());
				String state = getState(status.getGeoLocation());
				if (state != null) {
					// get score of the state
					String text = null;
					if ((text = status.getText()) != null) {
						int score = getTweetScore(text);
						// Set to Map output
						if (score != 0) {
							context.write(new Text(state), new IntWritable(score));
						}
					}
				}
			} catch (TwitterException e) {
				// Do nothing
			}
		//}
	}

	/**
	 * 
	 * @param statesLatLong
	 * @param geoLocation
	 * @return
	 */
	public String getState(GeoLocation geoLocation) {
		if (geoLocation == null) {
			return null;
		}

		Map<String, List<double[]>> statesLatLong = loadedData
				.getStatesLatLong();

		double lon = geoLocation.getLongitude();
		double lat = geoLocation.getLatitude();

		// Search for each state
		for (String state : statesLatLong.keySet()) {
			List<double[]> latLongs = statesLatLong.get(state);
			int polysides = latLongs.size();
			int j = polysides - 1;
			boolean oddnodes = false;

			for (int i = 0; i < polysides; i++) {
				double[] coordsI = latLongs.get(i);
				double[] coordsJ = latLongs.get(j);
				if ((coordsI[1] < lon && coordsJ[1] >= lon)
						|| (coordsJ[1] < lon && coordsI[1] >= lon)) {
					if ((coordsI[0] + (lon - coordsI[1])
							/ (coordsJ[1] - coordsI[1])
							* (coordsJ[0] - coordsI[0])) < lat) {
						oddnodes = true;
					}

				}
				j = i;
			}

			if (oddnodes) {
				return state;
			}
		}
		return null;
	}

	/**
	 * 
	 * @param text
	 * @return
	 */
	public int getTweetScore(String text) {
		int totalScore = 0;
		for (String word : text.split(" ")) {
			Integer score = loadedData.getDictionary().get(word.trim());
			if (score != null) {
				totalScore += score;
			}
		}
		return totalScore;
	}
	
	@Override
	protected void cleanup(Context context
            ) throws IOException, InterruptedException { 
		System.out.println(context.getTaskAttemptID() + " - Finsished " + new Date());
	}
}
