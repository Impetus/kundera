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

import javax.ws.rs.Path;

import com.impetus.kundera.rest.common.Constants;

/**
 * REST Resource for Meta data related operations
 * 
 * @author amresh.singh
 */

@Path("/" + Constants.KUNDERA_API_PATH + Constants.META_DATA_RESOURCE_PATH)
public class MetadataResource
{/*
    private static Logger log = LoggerFactory.getLogger(MetadataResource.class);

    *//**
     * Handler for GET requests. Returns schema List and related metadata for
     * the given list of persistence units.
     * 
     * @param persistenceUnits
     * @return
     *//*
    @GET
    @Produces({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
    @Path("/schemaList/{persistenceUnits}")
    public Response getSchemaList(@PathParam("persistenceUnits") String persistenceUnits)
    {
        if (log.isDebugEnabled())
            log.debug("GET: Persistence Units:" + persistenceUnits);

        StringTokenizer st = new StringTokenizer(persistenceUnits, ",");

        SchemaMetadata schemaMetadata = new SchemaMetadata();

        while (st.hasMoreTokens())
        {

            String persistenceUnit = st.nextToken();
            PersistenceUnitMetadata puMetadata = KunderaMetadataManager.getPersistenceUnitMetadata(kunderaMetadata, persistenceUnit);
            String schemaStr = puMetadata.getProperty("kundera.keyspace");

            if (schemaStr != null)
            {
                Schema schema = new Schema();
                schema.setSchemaName(schemaStr);

                MetamodelImpl metamodel = KunderaMetadataManager.getMetamodel(persistenceUnit);
                Map<String, EntityMetadata> metamodelMap = metamodel.getEntityMetadataMap();

                for (String clazz : metamodelMap.keySet())
                {
                    EntityMetadata m = metamodelMap.get(clazz);
                    Table table = new Table();
                    table.setEntityClassName(clazz);
                    table.setTableName(m.getTableName());

                    schema.addTable(table);
                }
                schemaMetadata.addSchema(schema);
            }
        }

        if (schemaMetadata.getSchemaList().isEmpty())
        {
            if (log.isDebugEnabled())
                log.warn("GET: getSchemaList: Can't find Schema for PUs " + persistenceUnits);
            return Response.noContent().build();
        }
        else
        {
            return Response.ok(schemaMetadata).build();
        }

    }

*/}
