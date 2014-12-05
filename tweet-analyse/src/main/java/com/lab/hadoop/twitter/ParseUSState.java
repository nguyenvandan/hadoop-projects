package com.lab.hadoop.twitter;

import java.io.PrintWriter;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Map;

import twitter4j.GeoLocation;

import com.lab.hadoop.tool.LoadData;

public class ParseUSState {
	
	/**
	 * 
	 * @param statesLatLong
	 * @param geoLocation
	 * @return
	 */
	public static String getState(GeoLocation geoLocation) {
		
		if (geoLocation == null) {
			return null;
		}
		
		Map<String, List<double[]>> statesLatLong = LoadData.getStatesLatLong();
		
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
}
