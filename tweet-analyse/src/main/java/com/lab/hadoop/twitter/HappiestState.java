package com.lab.hadoop.twitter;

import java.io.BufferedReader;
import java.io.IOException;

import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import com.lab.hadoop.tool.LoadData;

public class HappiestState {

	public static void main(String[] args) {
		if (args.length < 2) {
			System.out
					.println("Usage: java com.lab.hadoop.twitter.HappiestState [dictionary file] [tweets file]");
			System.exit(-1);
		}
		try {
			// Load data
			new LoadData(args[0]);
			// Check happiest state
			new HappiestState().checkHappiestState(args[1]);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 
	 * @param tweetsFilePath
	 */
	public void checkHappiestState(String tweetsFilePath)
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
					System.out.println(status.getGeoLocation());
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
}
