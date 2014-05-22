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

import java.net.URLDecoder;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
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
import com.impetus.kundera.rest.converters.CollectionConverter;
import com.impetus.kundera.rest.repository.EMRepository;

/**
 * REST based resource for Native Queries
 * 
 * @author amresh.singh
 */

@Path("/" + Constants.KUNDERA_API_PATH + Constants.NATIVE_QUERY_RESOURCE_PATH)
public class NativeQueryResource {
	private static Logger log = LoggerFactory
			.getLogger(NativeQueryResource.class);

	/************** Native Queries **************************/

	/**
	 * Handler for POST method requests for this resource Retrieves records from
	 * datasource for a given Native query
	 * 
	 * @return
	 */

	/************** Named Native Queries **************************/

	/**
	 * Executes Native Query and returns resposne
	 * 
	 * @param headers
	 * @param info
	 * @return
	 */
	@POST
	@Produces({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
	@Path("/{entityClassName}")
	public Response executeNativeQuery(@Context HttpHeaders headers,
			@Context UriInfo info, String query) {

		String sessionToken = headers.getRequestHeader(
				Constants.SESSION_TOKEN_HEADER_NAME).get(0);

		String mediaType = headers.getRequestHeader("Content-type").get(0);
		String entityClassName = info.getPathParameters().getFirst(
				"entityClassName");
		if (log.isDebugEnabled())
			log.debug("GET:: Session Token:" + sessionToken
					+ ", Entity Class Name:" + entityClassName
					+ ", Native Query:" + query + ", Media Type:" + mediaType);

		List result = null;
		Class<?> entityClass = null;
		Query q;

		try {
			query = URLDecoder.decode(query, "UTF-8");
			sessionToken = sessionToken.replaceAll("^\"|\"$", "");
			EntityManager em = EMRepository.INSTANCE.getEM(sessionToken);

			entityClass = EntityUtils.getEntityClass(entityClassName, em);
			if (log.isDebugEnabled())
				log.debug("GET: entityClass" + entityClass);
			if (entityClass == null) {
				return Response.serverError().build();
			}

			q = em.createNativeQuery(query, entityClass);
			result = q.getResultList();
		} catch (Exception e) {
			log.error(e.getMessage());
			return Response.serverError().build();
		}

		if (result == null) {
			return Response.noContent().build();
		}

		if (log.isDebugEnabled())
			log.debug("GET: Media Type:" + mediaType);

		String output = CollectionConverter.toString(result, entityClass,
				mediaType);

		return Response.ok(output, mediaType).build();
	}

	/**
	 * Executes named native queries and returns response
	 * 
	 * @param headers
	 * @param info
	 * @return
	 */

	@GET
	@Produces({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
	@Path("/{entityClass}/{namedNativeQueryName}")
	public Response executeNamedNativeQuery(@Context HttpHeaders headers,
			@Context UriInfo info) {

		String entityClassName = info.getPathParameters().getFirst(
				"entityClass");
		String namedNativeQueryName = info.getPathParameters().getFirst(
				"namedNativeQueryName");
		String sessionToken = headers.getRequestHeader(
				Constants.SESSION_TOKEN_HEADER_NAME).get(0);

		String mediaType = headers.getRequestHeader("Content-type").get(0);
		sessionToken = sessionToken.replaceAll("^\"|\"$", "");
		if (log.isDebugEnabled())
			log.debug("GET:: Session Token:" + sessionToken
					+ ", Entity Class Name:" + entityClassName
					+ ", Named Native Query:" + namedNativeQueryName
					+ ", Media Type:" + mediaType);

		Class<?> entityClass = null;
		List result = null;

		try {
			EntityManager em = EMRepository.INSTANCE.getEM(sessionToken);

			entityClass = EntityUtils.getEntityClass(entityClassName, em);
			if (log.isDebugEnabled())
				log.debug("GET: entityClass" + entityClass);
			if (entityClass == null) {
				return Response.serverError().build();
			}

			Query q = em.createNamedQuery(namedNativeQueryName);
			result = q.getResultList();
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			return Response.serverError().build();
		}

		if (result == null) {
			return Response.noContent().build();
		}

		String output = CollectionConverter.toString(result, entityClass,
				mediaType);

		return Response.ok(output, mediaType).build();
	}

}
