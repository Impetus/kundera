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

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.rest.common.Constants;
import com.impetus.kundera.rest.common.ResponseCode;
import com.impetus.kundera.rest.common.ResponseBuilder;
import com.impetus.kundera.rest.common.TokenUtils;
import com.impetus.kundera.rest.repository.EMFRepository;
import com.impetus.kundera.rest.repository.EMRepository;

/**
 * Session Token REST Resource
 * 
 * @author amresh.singh
 */

@Path("/" + Constants.KUNDERA_API_PATH + Constants.SESSION_RESOURCE_PATH)
public class SessionResource {
	private static Logger log = LoggerFactory.getLogger(SessionResource.class);

	/**
	 * Handler for GET method requests for this resource Generates Session token
	 * and returns, creates and puts EM into repository
	 * 
	 * @param applicationToken
	 * @return
	 */

	@GET
	@Produces({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
	@Consumes({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
	public Response getSessionToken(
			@HeaderParam(Constants.APPLICATION_TOKEN_HEADER_NAME) String applicationToken,
			@Context HttpHeaders headers) {
		if (log.isDebugEnabled())
			log.debug("GET: Application Token:" + applicationToken);
		String mediaType = headers != null && headers.getRequestHeaders().containsKey("Content-type")? headers.getRequestHeader("Content-type").get(0) : MediaType.APPLICATION_JSON;
		mediaType = mediaType.equalsIgnoreCase(MediaType.APPLICATION_XML) ? MediaType.APPLICATION_XML : MediaType.APPLICATION_JSON;
		EntityManagerFactory emf = EMFRepository.INSTANCE
				.getEMF(applicationToken);

		if (emf == null) {
			if (log.isDebugEnabled())
				log.warn("GET: Application Token:" + applicationToken
						+ " doesn't exist and hence Session can't be created");
			return Response.serverError().build();
		}

		String sessionToken = TokenUtils.generateSessionToken();
		EntityManager em = emf.createEntityManager();

		EMRepository.INSTANCE.addEm(sessionToken, em);
		//sessionToken = "\""+sessionToken+"\"";
		return Response.ok(ResponseBuilder.buildOutput(sessionToken, "\""), mediaType).build();
	}

	/**
	 * Handler for GET method requests for this resource Generates Session token
	 * and returns, creates and puts EM into repository
	 * 
	 * @param applicationToken
	 * @return
	 */
	@PUT
	@Produces({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
	@Consumes({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
	public Response flush(
			@HeaderParam(Constants.SESSION_TOKEN_HEADER_NAME) String sessionToken,
			@Context HttpHeaders headers) {
		if (log.isDebugEnabled())
			log.debug("PUT: Session Token:" + sessionToken);
		String mediaType = headers != null && headers.getRequestHeaders().containsKey("Content-type")? headers.getRequestHeader("Content-type").get(0) : MediaType.APPLICATION_JSON;
		mediaType = mediaType.equalsIgnoreCase(MediaType.APPLICATION_XML) ? MediaType.APPLICATION_XML : MediaType.APPLICATION_JSON;
		EntityManager em = EMRepository.INSTANCE.getEM(sessionToken);

		if (em == null) {
			if (log.isDebugEnabled())
				log.warn("PUT: Session Token:" + sessionToken
						+ " doesn't exist and hence can't be deleted");
			return Response.serverError().build();
		}

		try {
			em.flush();
		} catch (Exception e) {
			log.error("PUT: Failed: " + e.getMessage());
			Response.serverError().build();
		}

		return Response.ok(new String(ResponseCode.PUT_ST_SUCCESS), mediaType)
				.build();
	}

	/**
	 * Handler for DELETE method requests for this resource Closes EM and
	 * removes session token alongwith from repository
	 * 
	 * @param id
	 * @return
	 */
	@DELETE
	@Produces({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
	@Consumes({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
	public Response deleteSession(
			@HeaderParam(Constants.SESSION_TOKEN_HEADER_NAME) String sessionToken,
			@Context HttpHeaders headers) {

		if (log.isDebugEnabled())
			log.debug("DELETE: Session Token:" + sessionToken);
		String mediaType = headers != null && headers.getRequestHeaders().containsKey("Content-type")? headers.getRequestHeader("Content-type").get(0) : MediaType.APPLICATION_JSON;
		mediaType = mediaType.equalsIgnoreCase(MediaType.APPLICATION_XML) ? MediaType.APPLICATION_XML : MediaType.APPLICATION_JSON;
		EntityManager em = EMRepository.INSTANCE.getEM(sessionToken);
		if (em == null) {
			if (log.isDebugEnabled())
				log.warn("DELETE: Session Token:" + sessionToken
						+ " doesn't exist and hence can't be deleted");

			Response.serverError().build();
		}

		EMRepository.INSTANCE.removeEm(sessionToken);
		return Response.ok(new String(ResponseCode.DELETE_ST_SUCCESS),
				mediaType).build();

	}

}
