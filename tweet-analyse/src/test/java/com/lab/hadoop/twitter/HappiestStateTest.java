package com.lab.hadoop.twitter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import com.lab.hadoop.tool.LoadData;

public class HappiestStateTest {

	@Test
	public void checkHappiestStateTest() {
		HappiestState happiestState = null;
		try {
			happiestState = new HappiestState(getClass().getResource(
					"/AFINN-111.txt").getFile(), getClass().getResource(
					"/tweets_sample.txt").getFile());

			LoadData loadedData = new LoadData(getClass().getResource(
					"/AFINN-111.txt").getFile());
			happiestState.setLoadedData(loadedData);
			// Init states scores map
			happiestState.initMap();
			// Check happiest state
			happiestState.checkHappiestState();
		} catch (Exception e) {
			e.printStackTrace();
		}

		assertNotNull(happiestState);
		assertEquals(happiestState.getHappiestState(), "NONE");
	}

	@Test
	public void getTweetScoreTest() {
		HappiestState happiestState = null;
		try {
			happiestState = new HappiestState(getClass().getResource(
					"/AFINN-111.txt").getFile(), "");
		} catch (Exception e) {
			e.printStackTrace();
		}
		assertNotNull(happiestState);
		assertEquals(happiestState.getTweetScore("happy"), 3);
	}

	@Test
	public void runTest() {
		HappiestState happiestState = null;
		try {
			happiestState = new HappiestState(getClass().getResource(
					"/AFINN-111.txt").getFile(), getClass().getResource(
					"/tweets_sample.txt").getFile());
			happiestState.checkHappiestState();
		} catch (Exception e) {
			e.printStackTrace();
		}
		assertNotNull(happiestState);
		assertEquals(happiestState.getHappiestState(), "NONE");
	}
}
