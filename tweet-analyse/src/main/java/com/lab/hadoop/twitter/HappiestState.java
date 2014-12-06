package com.lab.hadoop.twitter;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import twitter4j.GeoLocation;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import com.lab.hadoop.tool.LoadData;

public class HappiestState {

	private Map<String, Integer> statesScores = null;
	private LoadData loadedData = null;
	private String dictionaryFile = null;
	private String tweetsFile = null;
	private String happiestState = "NONE";

	public static void main(String[] args) throws IOException {
		if (args.length < 2) {
			System.out
					.println("Usage: java com.lab.hadoop.twitter.HappiestState [dictionary file] [tweets file]");
			System.exit(-1);
		}
		
		System.out.println(new Date() + " : Starting...");		
		HappiestState happiestState = new HappiestState(args[0], args[1]);
		happiestState.checkHappiestState();	
		System.out.println(new Date() + " : Done!");
	}
	
	/**
	 * 
	 * @param dictionaryFile
	 * @param tweetsFile
	 */
	public HappiestState(String dictionaryFile, String tweetsFile) throws IOException {
		this.dictionaryFile = dictionaryFile;
		this.tweetsFile = tweetsFile;
		// Load data
		loadedData = new LoadData(this.dictionaryFile);
		// Init states scores map
		initMap();
	}

	/**
	 * 
	 * @param tweetsFilePath
	 */
	public void checkHappiestState() throws IOException {
		// Load tweets file
		BufferedReader br = null;
		try {
			br = LoadData.openFile(tweetsFile);
			// Check line by line
			String rawJSON = null;
			while ((rawJSON = br.readLine()) != null) {
				try {
					Status status = TwitterObjectFactory.createStatus(rawJSON);
					String state = getState(status.getGeoLocation());
					if (state != null) {
						// Add tweet's score to the total score of the state
						addStateScore(state, status.getText());
					}
				} catch (TwitterException te) {
					// Do nothing
					// System.out.println("Error input");
				}
			}
			// Repport
			printHappiestState();
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
			int tweetScore = getTweetScore(text);	
			statesScores.put(state, statesScores.get(state) + tweetScore);
		}
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
		
		Map<String, List<double[]>> statesLatLong = loadedData.getStatesLatLong();
		
		double lon = geoLocation.getLongitude();
		double lat = geoLocation.getLatitude();
		
		// Search for each state
		for (String state : statesLatLong.keySet()) {
			List<double[]> latLongs = statesLatLong.get(state);
			int polysides = latLongs.size();
			int j = polysides - 1;
			boolean oddnodes = false;
			
			for (int i = 0 ; i < polysides; i++) {	
				double[] coordsI = latLongs.get(i);
				double[] coordsJ = latLongs.get(j);				
				if ( (coordsI[1] < lon && coordsJ[1] >= lon) || (coordsJ[1] < lon && coordsI[1] >= lon) ) {
					if ( (coordsI[0] + (lon - coordsI[1]) / (coordsJ[1] - coordsI[1]) * (coordsJ[0] - coordsI[0])) < lat ) {
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
	 */
	private void printHappiestState() {
		int happiestStateScore = 0;
		String happiestState = "NONE";
		
		SortedMap<String, Integer> m = new TreeMap<String, Integer>(statesScores);

		for (String state : m.keySet()) {
			if (m.get(state) > happiestStateScore) {
				happiestState = state;
				happiestStateScore = m.get(state);
			}
			System.out.println(state + " : " + m.get(state));
		}

		System.out.println("*********** Happiest State in US **************");
		System.out.println("            " + happiestState);
		System.out.println("***********************************************");
		
		
	}

	/**
	 * 
	 */
	public void initMap() {
		statesScores = new HashMap<String, Integer>();
		for (String state : loadedData.getStatesLatLong().keySet()) {
			statesScores.put(state, new Integer(0));
		}
	}

	public Map<String, Integer> getStatesScores() {
		return statesScores;
	}

	public void setStatesScores(Map<String, Integer> statesScores) {
		this.statesScores = statesScores;
	}

	public LoadData getLoadedData() {
		return loadedData;
	}

	public void setLoadedData(LoadData loadedData) {
		this.loadedData = loadedData;
	}

	public String getHappiestState() {
		return happiestState;
	}

	public void setHappiestState(String happiestState) {
		this.happiestState = happiestState;
	}

	public String getDictionaryFile() {
		return dictionaryFile;
	}

	public void setDictionaryFile(String dictionaryFile) {
		this.dictionaryFile = dictionaryFile;
	}

	public String getTweetsFile() {
		return tweetsFile;
	}

	public void setTweetsFile(String tweetsFile) {
		this.tweetsFile = tweetsFile;
	}	
}
