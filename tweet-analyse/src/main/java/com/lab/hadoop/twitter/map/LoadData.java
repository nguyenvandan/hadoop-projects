package com.lab.hadoop.twitter.map;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class LoadData {
	// Dictionary
	private Map<String, Integer> dictionary = new HashMap<String, Integer>();
	// America states' latitude longitude data
	private Map<String, List<double[]>> statesLatLong = new HashMap<String, List<double[]>>();

	/**
	 * 
	 * @param dictionaryURI
	 * @throws IOException
	 */
	public LoadData(String dictionaryURI) throws IOException {
		loadAmericaStatesLatLong();
		loadSentimentDictionary(dictionaryURI);
	}

	/**
	 * 
	 * @return
	 */
	public Map<String, Integer> getDictionary() {
		return dictionary;
	}

	/**
	 * 
	 * @return
	 */
	public Map<String, List<double[]>> getStatesLatLong() {
		return statesLatLong;
	}

	/**
	 * 
	 * @param filename
	 * @return
	 * @throws IOException
	 */
	private void loadSentimentDictionary(String uri) throws IOException {
		BufferedReader br = null;
		InputStream in = null;
		FileSystem fs = FileSystem.get(new Configuration());
		try {
			in = fs.open(new Path(uri));
			br = new BufferedReader(new InputStreamReader(in, "UTF-8"));
			String line = null;
			while ((line = br.readLine()) != null) {
				StringTokenizer lineToken = new StringTokenizer(line, "\t");
				dictionary.put(lineToken.nextToken(),
						Integer.valueOf(lineToken.nextToken().trim()));
			}
		} finally {
			// Close file
			if (br != null) {
				br.close();
			}
			if (in != null) {
				in.close();
			}
		}
	}

	private void loadAmericaStatesLatLong() throws IOException {
		String currentState = null;
		// Load from file
		BufferedReader br = null;
		try {
			InputStream in = LoadData.class
					.getResourceAsStream("/US_States_LongLat.data");
			br = new BufferedReader(new InputStreamReader(in, "UTF-8"));
			String line = null;
			while ((line = br.readLine()) != null) {
				// is one state's name
				if (line.length() < 8) {
					currentState = line.substring(1, 3);
				} else {
					// current state LatLong
					addLatLong(statesLatLong, currentState, line);
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
	 * @param str
	 * @return
	 */
	private void addLatLong(Map<String, List<double[]>> statesLatLong,
			String currentState, String latLong) {
		StringTokenizer lineToken = new StringTokenizer(latLong, " ");
		// Latitude
		double latitude = Double.valueOf(lineToken.nextToken().substring(1, 8));
		// Longitude
		String longitudeStr = lineToken.nextToken();
		double longitude = Double.valueOf(longitudeStr.substring(0,
				longitudeStr.length() - 2));
		// put to map
		if (statesLatLong.keySet().contains(currentState)) {
			List<double[]> latLongs = statesLatLong.get(currentState);
			latLongs.add(new double[] { latitude, longitude });
		} else {
			List<double[]> latLongs = new ArrayList<double[]>();
			latLongs.add(new double[] { latitude, longitude });
			statesLatLong.put(currentState, latLongs);
		}

	}
}
