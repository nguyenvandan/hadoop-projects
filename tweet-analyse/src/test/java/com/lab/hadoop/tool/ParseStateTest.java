package com.lab.hadoop.tool;

import java.io.IOException;

import org.junit.Test;

import com.lab.hadoop.twitter.ParseUSState;

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
		assertEquals(ParseUSState.getState(AL), "AR");
		
		GeoLocation NH =  new GeoLocation(32.904956, -104.106445);
		assertEquals(ParseUSState.getState(NH), "NM");
		
		GeoLocation AZ =  new GeoLocation(33.406942, -111.906281);
		assertEquals(ParseUSState.getState(AZ), "AZ");
		
		GeoLocation MH =  new GeoLocation(44.018046, -92.467569);
		assertEquals(ParseUSState.getState(MH), "IA");
	}
}
