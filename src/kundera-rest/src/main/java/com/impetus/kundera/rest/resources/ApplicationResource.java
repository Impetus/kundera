/**
 * Copyright 2012 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.impetus.kundera.rest.resources;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.ws.rs.DELETE;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.rest.common.Constants;
import com.impetus.kundera.rest.common.ResponseCode;
import com.impetus.kundera.rest.common.ResponseBuilder;
import com.impetus.kundera.rest.common.TokenUtils;
import com.impetus.kundera.rest.repository.EMFRepository;

/**
 * Application Token Resource
 * 
 * @author amresh.singh
 */

@Path("/" + Constants.KUNDERA_API_PATH + Constants.APPLICATION_RESOURCE_PATH)
public class ApplicationResource {
	private static Logger log = LoggerFactory
			.getLogger(ApplicationResource.class);

	/**
	 * Handler for GET method requests for this resource Generates Application
	 * token and returns, creates and puts EMF into repository
	 * 
	 * @param persistenceUnits
	 * @return
	 */

	@POST
	@Produces({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
	@Path("/{persistenceUnits}")
	public Response getApplicationToken(
			@PathParam("persistenceUnits") String persistenceUnits,
			@Context HttpHeaders headers, String externalProperties) {
	    String mediaType = headers != null && headers.getRequestHeaders().containsKey("Content-type")? headers.getRequestHeader("Content-type").get(0) : MediaType.APPLICATION_JSON;
		mediaType = mediaType.equalsIgnoreCase(MediaType.APPLICATION_XML) ? MediaType.APPLICATION_XML : MediaType.APPLICATION_JSON;
		if (log.isDebugEnabled())
			log.debug("GET Persistence Unit(s):" + persistenceUnits);
		EntityManagerFactory emf = null;
		if (externalProperties != null && !externalProperties.isEmpty()) {
			
			try {
				if (persistenceUnits.contains(",")) {
					Map<String, Map<String, String>> puProperties = new HashMap<String, Map<String, String>>();
					puProperties = new ObjectMapper().readValue(externalProperties,
							puProperties.getClass());
					emf = Persistence
							.createEntityManagerFactory(persistenceUnits,
									puProperties);
				} else {
					Map<String, Object> puProperties = new HashMap<String, Object>();
					puProperties = new ObjectMapper().readValue(externalProperties,
							puProperties.getClass());
					emf = Persistence
							.createEntityManagerFactory(persistenceUnits,
									puProperties);
				}

				
			} catch (JsonParseException e) {
				log.error(e.getMessage());
			} catch (JsonMappingException e) {
				log.error(e.getMessage());
			} catch (IOException e) {
				log.error(e.getMessage());
			}
		} else {
			 emf = Persistence
					.createEntityManagerFactory(persistenceUnits);
		}
		if (emf == null) {
			log.warn("Invalid emf");
			return Response.serverError().build();// ResponseCode.DELETE_AT_FAILED;

		}
		String applicationToken = TokenUtils.generateApplicationToken();

		EMFRepository.INSTANCE.addEmf(applicationToken, emf);
		//applicationToken = "\"" + applicationToken + "\"";
		return Response.ok(ResponseBuilder.buildOutput(applicationToken, "\""), mediaType).build();
	}

	/**
	 * Handler for DELETE method requests for this resource Closes EMF and
	 * removes application token along with from repository
	 * 
	 * @param id
	 * @return
	 */
	@DELETE
	@Produces({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
	public Response closeApplication(
			@HeaderParam(Constants.APPLICATION_TOKEN_HEADER_NAME) String applicationToken,
			@Context HttpHeaders headers) {
	    String mediaType = headers != null && headers.getRequestHeaders().containsKey("Content-type")? headers.getRequestHeader("Content-type").get(0) : MediaType.APPLICATION_JSON;
		mediaType = mediaType.equalsIgnoreCase(MediaType.APPLICATION_XML) ? MediaType.APPLICATION_XML : MediaType.APPLICATION_JSON;
		if (log.isDebugEnabled())
			log.debug("DELETE: Application Token:" + applicationToken);

		EntityManagerFactory emf = EMFRepository.INSTANCE
				.getEMF(applicationToken);
		if (emf == null) {
			log.warn("DELETE: Application Token:" + applicationToken
					+ " doesn't exist and hence can't be closed");
			return Response.serverError().build();// ResponseCode.DELETE_AT_FAILED;

		}

		EMFRepository.INSTANCE.removeEMF(applicationToken);
		return Response.ok(new String(ResponseCode.DELETE_AT_SUCCESS),
				mediaType).build();
	}

}
