package edu.duke.dbmsplus.datahooks.yarnrecord;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;


/**
 * Get Yarn metrics from yarn timeline server
 * @author Xiaodan
 */

public class AppLisener {
	//the following code is a experiment to get an application info and convert to a java object.
	private static String timelineUri = "http://localhost:8188";
	
	public static void main (String[] args) {
		Client client = ClientBuilder.newClient();
		WebTarget webTarget = client.target(timelineUri);
		WebTarget appsResource = webTarget.path("/ws/v1/applicationhistory/apps/application_1436472754353_0002");
		Invocation.Builder appList = appsResource.request();
		Response response = appList.get();
		System.out.println(response.getStatus());
		//System.out.println(response.readEntity(String.class));
		
		ObjectMapper mapper = new ObjectMapper();
		
		try {
			//need to use BufferReader here
            App cricketer = mapper.readValue(response.readEntity(String.class), App.class);
            System.out.println("App object created from JSON String :");
            System.out.println(cricketer);

        } catch (JsonGenerationException ex) {
            ex.printStackTrace();
        } catch (JsonMappingException ex) {
            ex.printStackTrace();
        } catch (IOException ex) {
            ex.printStackTrace();
		}
	}
}