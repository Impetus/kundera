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
import java.util.Map;
import java.util.StringTokenizer;

import javax.persistence.EntityManager;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;
import com.impetus.kundera.rest.common.Constants;
import com.impetus.kundera.rest.common.EntityUtils;
import com.impetus.kundera.rest.common.JAXBUtils;
import com.impetus.kundera.rest.common.ResponseBuilder;
import com.impetus.kundera.rest.dto.Schema;
import com.impetus.kundera.rest.dto.SchemaMetadata;
import com.impetus.kundera.rest.dto.Table;
import com.impetus.kundera.rest.repository.EMRepository;

/**
 * REST Resource for Meta data related operations
 * 
 * @author amresh.singh
 */

@Path("/" + Constants.KUNDERA_API_PATH + Constants.META_DATA_RESOURCE_PATH)
public class MetadataResource {
    private static Logger log = LoggerFactory.getLogger(MetadataResource.class);

    /**
     * Handler for GET requests. Returns schema List and related metadata for the given list of persistence units.
     * 
     * @param persistenceUnits
     * @return
     */
    @GET
    @Produces({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
    @Path("/schemaList/{persistenceUnits}")
    public Response getSchemaList(@HeaderParam(Constants.SESSION_TOKEN_HEADER_NAME) String sessionToken,
        @PathParam("persistenceUnits") String persistenceUnits, @Context HttpHeaders headers) {
        if (log.isDebugEnabled())
            log.debug("GET: Persistence Units:" + persistenceUnits);
        sessionToken = sessionToken.replaceAll("^\"|\"$", "");

        EntityManager em = EMRepository.INSTANCE.getEM(sessionToken);
        StringTokenizer st = new StringTokenizer(persistenceUnits, ",");
        EntityManagerFactoryImpl emfImpl = (EntityManagerFactoryImpl) em.getEntityManagerFactory();

        SchemaMetadata schemaMetadata = new SchemaMetadata();
        String mediaType = headers != null && headers.getRequestHeaders().containsKey("Content-type")? headers.getRequestHeader("Content-type").get(0) : MediaType.APPLICATION_JSON;
        mediaType =
            mediaType.equalsIgnoreCase(MediaType.APPLICATION_XML) ? MediaType.APPLICATION_XML
                : MediaType.APPLICATION_JSON;
        while (st.hasMoreTokens()) {

            String persistenceUnit = st.nextToken();
            PersistenceUnitMetadata puMetadata =
                emfImpl.getKunderaMetadataInstance().getApplicationMetadata()
                    .getPersistenceUnitMetadata(persistenceUnit);
            String schemaStr = puMetadata.getProperty("kundera.keyspace");

            if (schemaStr != null) {
                Schema schema = new Schema();
                schema.setSchemaName(schemaStr);

                
                MetamodelImpl metamodel = (MetamodelImpl) em.getEntityManagerFactory().getMetamodel();
                Map<String, EntityMetadata> metamodelMap = metamodel.getEntityMetadataMap();

                for (String clazz : metamodelMap.keySet()) {
                    EntityMetadata m = metamodelMap.get(clazz);
                    Table table = new Table();
                    table.setEntityClassName(clazz);
                    table.setTableName(m.getTableName());
                    table.setSimpleEntityClassName(m.getEntityClazz().getSimpleName());

                    schema.addTable(table);
                }
                schemaMetadata.addSchema(schema);
            }
        }

        if (schemaMetadata.getSchemaList().isEmpty()) {
            if (log.isDebugEnabled())
                log.warn("GET: getSchemaList: Can't find Schema for PUs " + persistenceUnits);
            return Response.noContent().build();
        } else {
            if (mediaType.equalsIgnoreCase(MediaType.APPLICATION_JSON)) {
                ObjectMapper mapper = new ObjectMapper();
                String output = null;

                try {
                    output = mapper.writeValueAsString(schemaMetadata);
                } catch (JsonGenerationException e) {
                    log.error(e.getMessage());
                } catch (JsonMappingException e) {
                    log.error(e.getMessage());
                } catch (IOException e) {
                    log.error(e.getMessage());
                }

          

                return Response.ok(ResponseBuilder.buildOutput(output, "'"), mediaType).build();
            } else {
                return Response.ok(schemaMetadata, mediaType).build();
            }
        }

    }

    /**
     * Handler for GET requests. Returns schema List and related metadata for the given list of persistence units.
     * 
     * @param persistenceUnits
     * @return
     */
    @POST
    @Produces({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
    @Path("/{entityClass}")
    public Response getEntityModel(@HeaderParam(Constants.SESSION_TOKEN_HEADER_NAME) String sessionToken,
        @PathParam("entityClass") String entityClassName, @Context HttpHeaders headers) {
        Class<?> entityClass;
        String mediaType = headers != null && headers.getRequestHeaders().containsKey("Content-type")? headers.getRequestHeader("Content-type").get(0) : MediaType.APPLICATION_JSON;
        mediaType =
            mediaType.equalsIgnoreCase(MediaType.APPLICATION_XML) ? MediaType.APPLICATION_XML
                : MediaType.APPLICATION_JSON;
        if (log.isDebugEnabled())
            log.debug("GET: Persistence Units:" + entityClassName);
        sessionToken = sessionToken.replaceAll("^\"|\"$", "");
        try {
            EntityManager em = EMRepository.INSTANCE.getEM(sessionToken);
            entityClass = EntityUtils.getEntityClass(entityClassName, em);

            String output = JAXBUtils.getSchema(entityClass, mediaType);

            return Response.ok(ResponseBuilder.buildOutput(output, "'"), mediaType).build();
        } catch (Exception e) {
            log.error(e.getMessage());
            return Response.serverError().build();
        }
    }

}
