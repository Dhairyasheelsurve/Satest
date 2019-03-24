/**
 * Copyright 2019 Attivio Inc., All rights reserved.
 * 
 * @author bflynn
 */
package com.attivio.sa.satest;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.attivio.sdk.AttivioException;
import com.attivio.sdk.ingest.IngestDocument;

/**
 * The goal of this unit test is to ensure the associated GeocodeLocation Document 
 * Transformer is adding appropriate latitude and longitude coordinates to a given 
 * Attivio Document. The tests become increasingly more complex in location 
 * description and coordinate precision.
 * 
 * @author bflynn
 * @version Attivio 5.5.0.1
 */
public class GeocodeLocationTest {
	public GeocodeLocation xformer = new GeocodeLocation();

	@Test
	public void testTransformer() throws AttivioException {
		
		
		validateGeocoding("Boston", 42f, 43f, -72f, -71f); 
		// Geocode "Boston" within a latitude of 42° & 43° and longitude of -72° and -71°
		
		validateGeocoding("Tokyo", 35f, 36f, 139f, 140f); 
		// Geocode "Tokyo" within a latitude of 35° & 36° and longitude of 139° and 140°
		
		validateGeocoding("Washington D.C.", 38f, 39f, -78f, -77f); 
		// Geocode "Washington D.C" within a latitude of 38° & 39° and longitude of -78° and -77°
		
		validateGeocoding("02038", 42.0f, 42.1f, -71.5f, -71.4f); 
		// Geocode "02038" within a latitude of 42.0° & 42.1° and longitude of -71.5° and -71.4°
		
		validateGeocoding("275 Grove Street Newton, MA 02466", 42.3388f, 42.3389f, -71.2528f, -71.2527f); 
		// Geocode "275 Grove Street Newton, MA 02466" within a latitude of 42.3388° & 42.3389° and longitude of -71.2528° and -71.2527°
		
		validateGeocoding("Fastiv Фастів", 50.06376f, 50.06377f, 29.90496f, 29.90497f);
		// Geocode "Fastiv Фастів" within a latitude of 50.06376° & 50.06377° and longitude of 29.90496° and 29.90497°
		
		validateGeocoding("海南藏族 自治州", 36.29659f, 36.29660f, 100.6226f, 100.6227f);
		// Geocode "海南藏族 自治州" within a latitude of 36.29659° & 36.29660° and longitude of 100.6226° and 100.6227°
		
		validateGeocoding("الزلفي", 26.30985f, 26.30986f, 44.83183f, 44.83184f);
		// Geocode "الزلفي" within a latitude of 26.30985° & 26.30986° and longitude of 44.83183° and 44.83184°
		
	}

	private void validateGeocoding(String location, float latFloor, float latCeiling, float lngFloor, float lngCeiling) {
		try {
			
			System.out.println("**************************************");
			System.out.println("Geocoding location: " + location);
			
			IngestDocument doc = new IngestDocument(location);
			doc.setField("location", location);
			xformer.processDocument(doc);
			float latitude = Float.parseFloat(doc.getFirstValue("latitude").toString());
			float longitude = Float.parseFloat(doc.getFirstValue("longitude").toString());
			
			boolean isLatitudeWithinRange = (latitude >= latFloor && latitude <= latCeiling);
			boolean isLongitudeWithinRange = (longitude >= lngFloor && longitude <= lngCeiling);
			
			System.out.println("Expecting latitude field >= " + latFloor + " && <= " + latCeiling);
			System.out.println("Is " + latitude + " within this range? " + isLatitudeWithinRange);
			System.out.println("Expecting longitude field >= " + lngFloor + " && <= " + lngCeiling);
			System.out.println("Is " + longitude + " within this range? " + isLongitudeWithinRange);
			
			assertTrue(isLatitudeWithinRange);
			assertTrue(isLongitudeWithinRange);
		} catch (AttivioException e) {
			e.printStackTrace();
		}
	}

}
