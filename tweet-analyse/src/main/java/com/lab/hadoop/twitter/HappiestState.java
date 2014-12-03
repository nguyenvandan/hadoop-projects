package com.lab.hadoop.twitter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

public class HappiestState {

	private Map<String, Integer> dictionary = null;
	private File tweetsFile = null;

	public static void main(String[] args) {
		if (args.length < 2) {
			System.out
					.println("Usage: java com.lab.hadoop.twitter.HappiestState [dictionary file] [tweets file]");
			System.exit(-1);
		}

		try {
			HappiestState hState = new HappiestState();
			// Load dictionary
			hState.loadDictionary(args[0]);
			// check
			hState.checkHappiestState(args[1]);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	/**
	 * 
	 * @param tweetsFilePath
	 */
	public void checkHappiestState(String tweetsFilePath) throws IOException {
		BufferedReader br = null;
		Twitter twitter = null;
		// Load tweets file
		try {
			br = new BufferedReader(new InputStreamReader(new FileInputStream(
					tweetsFilePath), "UTF-8"));
			// Check line by line
			String rawJSON = null;			
			while ((rawJSON = br.readLine()) != null) {
				if (rawJSON.length() > 100) {
					try {
						Status status = TwitterObjectFactory.createStatus(rawJSON);
						System.out.println(status.getGeoLocation());
					} catch (TwitterException te) {
						// Do nothing
						te.printStackTrace();
					}
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
	 * @param dictionaryFile
	 * @throws IOException
	 */
	public void loadDictionary(String dictionaryFile) throws IOException {
		dictionary = new HashMap<String, Integer>();
		// Load from file
		BufferedReader br = new BufferedReader(new FileReader(new File(
				dictionaryFile)));
		String line = null;
		while ((line = br.readLine()) != null) {
			StringTokenizer lineToken = new StringTokenizer(line, "\t");
			dictionary.put(lineToken.nextToken(),
					Integer.valueOf(lineToken.nextToken().trim()));
		}

		// Close file
		br.close();
	}
}
