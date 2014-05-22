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
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.rest.common.Constants;
import com.impetus.kundera.rest.common.EntityUtils;
import com.impetus.kundera.rest.common.JAXBUtils;
import com.impetus.kundera.rest.common.StreamUtils;
import com.impetus.kundera.rest.repository.EMRepository;

/**
 * REST resource for CRUD operations
 * 
 * @author amresh.singh
 */

@Path("/" + Constants.KUNDERA_API_PATH + Constants.CRUD_RESOURCE_PATH
		+ "/{entityClass}")
public class CRUDResource {
	/** log for this class. */
	private static Logger log = LoggerFactory.getLogger(CRUDResource.class);

	@Context
	UriInfo uriInfo;

	/**
	 * Handler for POST method requests for this resource Inserts an entity into
	 * datastore
	 * 
	 * @param sessionToken
	 * @param entityClassName
	 * @param in
	 * @return
	 */
	@POST
	// @Consumes({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
	@Produces({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
	public Response insert(
			@HeaderParam(Constants.SESSION_TOKEN_HEADER_NAME) String sessionToken,
			@PathParam("entityClass") String entityClassName, String input,
			@Context HttpHeaders headers) {
		String mediaType = headers.getRequestHeader("Content-type").get(0);
		// String mediaType = MediaType.APPLICATION_JSON;
		sessionToken = sessionToken.replaceAll("^\"|\"$", "");

		if (log.isDebugEnabled()) {
			log.debug("POST: SessionToken: " + sessionToken);
			log.debug("POST: entityClass: " + entityClassName);
		}

		Object id;
		try {
			EntityManager em = EMRepository.INSTANCE.getEM(sessionToken);
			Class<?> entityClass = EntityUtils.getEntityClass(entityClassName,
					em);

			if (log.isDebugEnabled())
				log.debug("POST: entityClass" + entityClass);

			if (log.isDebugEnabled())
				log.debug("POST: Media Type:" + mediaType);

			log.debug("Entity Data" + input);

			Object entity = JAXBUtils.toObject(
					StreamUtils.toInputStream(input), entityClass, mediaType);

			log.debug("Entity Data" + entity);
			em.persist(entity);

		} catch (Exception e) {
			log.error(e.getMessage());
			return Response.serverError().build();
		}

		return Response.ok("Record persisted", mediaType).build();
	}

	/**
	 * Handler for GET method requests for this resource Finds an entity from
	 * datastore
	 * 
	 * @param sessionToken
	 * @param entityClassName
	 * @param id
	 * @return
	 */
	@GET
	@Produces({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
	@Path("/{id}")
	public Response find(
			@HeaderParam(Constants.SESSION_TOKEN_HEADER_NAME) String sessionToken,
			@PathParam("entityClass") String entityClassName,
			@PathParam("id") String id, @Context HttpHeaders headers) {

		sessionToken = sessionToken.replaceAll("^\"|\"$", "");
		String mediaType = headers.getRequestHeader("Content-type").get(0);
		log.debug("GET: sessionToken:" + sessionToken);
		log.debug("GET: entityClass:" + entityClassName);
		log.debug("GET: ID:" + id);

		Object entity = null;
		Class<?> entityClass;
		try {
			EntityManager em = EMRepository.INSTANCE.getEM(sessionToken);
			entityClass = EntityUtils.getEntityClass(entityClassName, em);
			log.debug("GET: entityClass" + entityClass);

			entity = em.find(entityClass, id);
		} catch (Exception e) {
			log.error(e.getMessage());
			return Response.serverError().build();
		}

		log.debug("GET: " + entity);

		if (entity == null) {
			return Response.noContent().build();
		}

		return Response.ok(JAXBUtils.toString(entityClass, entity, mediaType),
				mediaType).build();
	}

	/**
	 * Handler for PUT method requests for this resource Updates an entity into
	 * datastore
	 * 
	 * @param sessionToken
	 * @param entityClassName
	 * @param in
	 * @return
	 */
	@PUT
	// @Consumes({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
	@Produces({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
	public Response update(
			@HeaderParam(Constants.SESSION_TOKEN_HEADER_NAME) String sessionToken,
			@PathParam("entityClass") String entityClassName, String input,
			@Context HttpHeaders headers) {
		sessionToken = sessionToken.replaceAll("^\"|\"$", "");

		log.debug("PUT: sessionToken:" + sessionToken);
		log.debug("PUT: entityClassName:" + entityClassName);
		String mediaType = headers.getRequestHeader("Content-type").get(0);

		log.debug("POST: Media Type:" + mediaType);

		Object output;
		Class<?> entityClass;
		Object entity;
		try {
			EntityManager em = EMRepository.INSTANCE.getEM(sessionToken);
			entityClass = EntityUtils.getEntityClass(entityClassName, em);
			log.debug("PUT: entityClass: " + entityClass);

			entity = JAXBUtils.toObject(StreamUtils.toInputStream(input),
					entityClass, mediaType);
			output = em.merge(entity);
		} catch (Exception e) {
			log.error(e.getMessage());
			return Response.serverError().build();
		}

		if (output == null) {
			return Response.notModified().build();
		}

		return Response.ok(JAXBUtils.toString(entityClass, entity, mediaType),
				mediaType).build();
	}

	/**
	 * Handler for DELETE method requests for this resource Deletes an entity
	 * from datastore
	 * 
	 * @param sessionToken
	 * @param entityClassName
	 * @param id
	 * @return
	 */
	@DELETE
	@Produces({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
	@Path("/delete/{id}")
	public Response delete(
			@HeaderParam(Constants.SESSION_TOKEN_HEADER_NAME) String sessionToken,
			@PathParam("entityClass") String entityClassName,
			@PathParam("id") String id, @Context HttpHeaders headers) {

		sessionToken = sessionToken.replaceAll("^\"|\"$", "");
		String mediaType = headers.getRequestHeader("Content-type").get(0);
		log.debug("DELETE: sessionToken:" + sessionToken);
		log.debug("DELETE: entityClass Name:" + entityClassName);
		log.debug("DELETE: ID:" + id);

		try {
			EntityManager em = EMRepository.INSTANCE.getEM(sessionToken);
			Class<?> entityClass = EntityUtils.getEntityClass(entityClassName,
					em);
			log.debug("DELETE: entityClass" + entityClass);

			Object entity = em.find(entityClass, id);
			em.remove(entity);
		} catch (Exception e) {
			log.error(e.getMessage());
			return Response.serverError().build();
		}

		return Response.ok(new String("Deleted Successfully"), mediaType)
				.build();

	}

}