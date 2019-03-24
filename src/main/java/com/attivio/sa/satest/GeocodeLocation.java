/**
 * Copyright 2017 Attivio Inc., All rights reserved.
 */
package com.attivio.sa.satest;

import java.io.*;
import java.net.*;
import com.attivio.sdk.*;
import com.attivio.sdk.ingest.IngestDocument;
import com.attivio.sdk.schema.FieldNames;
import com.attivio.sdk.server.annotation.ConfigurationOption;
import com.attivio.sdk.server.annotation.ConfigurationOptionInfo;
import com.attivio.sdk.server.component.ingest.DocumentModifyingTransformer;
import com.google.gson.*;


/** An Example of the simplest document transformer possible. */
@ConfigurationOptionInfo(displayName = "Sample Simple Ingest Transformer", description = "Simple transformer sample code provided by the SDK", groups = {
  @ConfigurationOptionInfo.Group(path = ConfigurationOptionInfo.PLATFORM_COMPONENT, propertyNames = { "field", "value" })
})
public class GeocodeLocation implements DocumentModifyingTransformer {

  private String field = FieldNames.TITLE;
  private String value = "My new title";  
  private final String key = "AIzaSyDMD_7PBpZfVlnmKHo3nhb1JfOUVdNYo_M";
  private final String geocodeUrl = "https://maps.googleapis.com/maps/api/geocode/json?address=";
  
  @Override
  public boolean processDocument(IngestDocument doc) throws AttivioException {
    // a really simple example to set a field value.
    doc.setField(field, value);

    
	String location = "";
	String fetchLocationDetailsURL  = "";
    URL url = null;
	URLConnection request = null;
    
    
	try {
    if(doc.getField("location").getValue(0) != null) {
	    location = doc.getField("location").getValue(0).toString().replaceAll(" ", "");	
	    /*Url to fetch latitude and longitude of location*/
	    fetchLocationDetailsURL = geocodeUrl+location+"&key="+key; 
	    url = new URL(fetchLocationDetailsURL);
		request = url.openConnection();
		request.connect();
		/*Retrieving the contents from request object and then putting the result in an json array to get latitude and longitude*/
		InputStreamReader read = new InputStreamReader((InputStream) request.getContent());
		JsonParser jsonparser = new JsonParser();
		JsonElement root = jsonparser.parse(read); //Convert the input stream to a json element
		JsonObject rootobject = root.getAsJsonObject();			 
		JsonArray arrayResult = (JsonArray) rootobject.get("results");
		
		
		/*fetching latitude and longitude from the json result array*/
		String latitude = arrayResult.get(0).getAsJsonObject().get("geometry").getAsJsonObject().get("location").getAsJsonObject().get("lat").toString();
		String longitude = arrayResult.get(0).getAsJsonObject().get("geometry").getAsJsonObject().get("location").getAsJsonObject().get("lng").toString();
		doc.setField("latitude", latitude);	  
		doc.setField("longitude", longitude); 
    }
    else
    	return false;
	} catch (IOException e2) {
	System.out.println("Error Message - " + e2.getMessage());
	}	      
   return true;
  }

  @ConfigurationOption(displayName = "Field to set", description = "Name of the new field to create")
  public String getField() {
    return field;
  }

  public void setField(String field) {
    this.field = field;
  }

  @ConfigurationOption(displayName = "Field value", description = "Value for the new field")
  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }


}
