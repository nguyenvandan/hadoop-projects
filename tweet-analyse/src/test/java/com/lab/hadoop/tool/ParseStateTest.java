package com.lab.hadoop.tool;

import java.io.IOException;

import org.junit.Test;

import com.lab.hadoop.twitter.ParseState;

import twitter4j.GeoLocation;
import static org.junit.Assert.*;

public class ParseStateTest {

	@Test
	public void getStateTest() {
		// Load data
		try {
			new LoadData("");
		} catch (IOException e) {
			//e.printStackTrace();
		}
		
		
		GeoLocation AL =  new GeoLocation(32.154106, -85.330811);
		assertEquals(ParseState.getState(AL), "AR");
		
		GeoLocation NH =  new GeoLocation(32.904956, -104.106445);
		assertEquals(ParseState.getState(NH), "NM");
		
		GeoLocation AZ =  new GeoLocation(33.406942, -111.906281);
		assertEquals(ParseState.getState(AZ), "AZ");
		
		GeoLocation MH =  new GeoLocation(44.018046, -92.467569);
		assertEquals(ParseState.getState(MH), "IA");
	}
}
