package com.lab.hadoop.twitter;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import com.lab.hadoop.tool.LoadData;

public class HappiestState {
	
	private Map<String, Integer> statesScores = null;

	public static void main(String[] args) {
		if (args.length < 2) {
			System.out
					.println("Usage: java com.lab.hadoop.twitter.HappiestState [dictionary file] [tweets file]");
			System.exit(-1);
		}
		try {
			// Load data
			new LoadData(args[0]);
			HappiestState happiestState = new HappiestState();
			// Init states scores map
			happiestState.initMap();
			// Check happiest state
			happiestState.checkHappiestState(args[1]);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	

	/**
	 * 
	 * @param tweetsFilePath
	 */
	private void checkHappiestState(String tweetsFilePath)
			throws IOException {	
		// Load tweets file
		BufferedReader br = null;		
		try {
			br = LoadData.openFile(tweetsFilePath);
			// Check line by line
			String rawJSON = null;
			while ((rawJSON = br.readLine()) != null) {
				try {
					Status status = TwitterObjectFactory.createStatus(rawJSON);
					String state = ParseUSState.getState(status.getGeoLocation());
					if (state != null) {
						addStateScore(state, status.getText());
					}
				} catch (TwitterException te) {
					// Do nothing
					te.printStackTrace();
				}
			}
		} finally {
			// Close file
			if (br != null) {
				br.close();
			}
		}
	}
	
	/**
	 * 
	 * @param state
	 * @param text
	 */
	private void addStateScore(String state, String text) {
		// If the tweet's text is not empty
		if (text != null) {
			int score = getTweetScore(text);
			Integer actualScore = statesScores.get(state);
		}
	}
	
	/**
	 * 
	 * @param text
	 * @return
	 */
	private int getTweetScore(String text) {
		int totalScore = 0;
		for (String word : text.split(" ")) {
			Integer score = LoadData.getDictionary().get(word.trim());
			if (score != null) {
				totalScore += score;
			}
		}
		return totalScore;
	}
	
	/**
	 * 
	 */
	private void initMap() {
		statesScores = new HashMap<String, Integer>();
		for (String state : LoadData.getStatesLatLong().keySet()) {
			statesScores.put(state, new Integer(0));
		}
	}
}
