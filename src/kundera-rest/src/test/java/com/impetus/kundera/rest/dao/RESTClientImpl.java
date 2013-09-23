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
package com.impetus.kundera.rest.dao;

import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.rest.common.Constants;
import com.impetus.kundera.rest.common.Professional;
import com.impetus.kundera.rest.common.StreamUtils;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.WebResource;

/**
 * REST Client implementation for test cases
 * 
 * @see RESTClient
 * @author amresh.singh
 */
public class RESTClientImpl implements RESTClient
{
    private static Logger log = LoggerFactory.getLogger(RESTClientImpl.class);

    private WebResource webResource = null;

    private String mediaType;

    @Override
    public void initialize(WebResource wr, String mediaType)
    {
        this.webResource = wr;
        this.mediaType = mediaType;
    }

    @Override
    public String getApplicationToken(String persistenceUnit)
    {
        String applicationToken;
        try
        {
            log.debug("\n\nGetting Application Token...");
            WebResource.Builder atBuilder = webResource.path(
                    Constants.KUNDERA_API_PATH + "/application/" + persistenceUnit).accept(MediaType.TEXT_PLAIN);
            String atResponse = atBuilder.get(ClientResponse.class).toString();
            applicationToken = atBuilder.get(String.class);
            log.debug("Response: " + atResponse);
            log.debug("Application Token:" + applicationToken);
        }
        catch (UniformInterfaceException e)
        {
            log.error("Error during getApplicationToken, Caused by:" + e.getMessage() + ", returning null");
            return null;
        }
        catch (ClientHandlerException e)
        {
            log.error("Error during getApplicationToken, Caused by:" + e.getMessage() + ", returning null");
            return null;
        }
        return applicationToken;
    }

    @Override
    public String closeApplication(String applicationToken)
    {
        log.debug("\n\nClosing Application for Token:" + applicationToken);
        WebResource.Builder atBuilder = webResource.path(Constants.KUNDERA_API_PATH + "/application")
                .accept(MediaType.TEXT_PLAIN).header(Constants.APPLICATION_TOKEN_HEADER_NAME, applicationToken);
        String response = atBuilder.delete(String.class);
        log.debug("Application Closure Response: " + response);
        return response;
    }

    @Override
    public String getSessionToken(String applicationToken)
    {
        log.debug("\n\nGetting Session Token...");
        WebResource.Builder stBuilder = webResource.path(Constants.KUNDERA_API_PATH + "/session")
                .accept(MediaType.TEXT_PLAIN).header(Constants.APPLICATION_TOKEN_HEADER_NAME, applicationToken);

        String sessionToken = stBuilder.get(String.class);

        log.debug("Session Token:" + sessionToken);
        return sessionToken;
    }

    @Override
    public String closeSession(String sessionToken)
    {

        log.debug("\n\nClosing Session for Token:" + sessionToken);
        WebResource.Builder stBuilder = webResource.path(Constants.KUNDERA_API_PATH + "/session")
                .accept(MediaType.TEXT_PLAIN).header(Constants.SESSION_TOKEN_HEADER_NAME, sessionToken);
        String response = stBuilder.delete(String.class);
        log.debug("Session Deletion Response: " + response);
        return response;
    }

    @Override
    public String insertEntity(String sessionToken, String entityStr, String entityClassName)
    {
        log.debug("\n\nInserting Entity...");

        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date(1344079067777l));
        Professional prof = new Professional("11111111111111", 34662345, false, 31, 'C', (byte) 8, (short) 5,
                (float) 10.0, 163.12, new Date(Long.parseLong("1344079065781")), new Date(
                        Long.parseLong("1344079067623")), new Date(Long.parseLong("1344079069105")), 2, new Long(
                        3634521523423L), new Double(0.23452342343),
                /*
                 * new java.sql.Date(new
                 * Date(Long.parseLong("1344079061111")).getTime()), new
                 * java.sql.Time(new
                 * Date(Long.parseLong("1344079062222")).getTime()), new
                 * java.sql.Timestamp(new
                 * Date(Long.parseLong("13440790653333")).getTime()),
                 */
                new BigInteger("123456789"), new BigDecimal(123456789), cal);

        WebResource.Builder insertBuilder = webResource.path(Constants.KUNDERA_API_PATH + "/crud/" + entityClassName)
                .type(mediaType).accept(MediaType.APPLICATION_OCTET_STREAM)
                .header(Constants.SESSION_TOKEN_HEADER_NAME, sessionToken);
        StringBuffer sb = new StringBuffer().append(entityStr);
        ClientResponse insertResponse = (ClientResponse) insertBuilder.post(ClientResponse.class, sb.toString());
        log.debug("Response From Insert Entity: " + insertResponse);
        return insertResponse.toString();
    }

    @Override
    public String findEntity(String sessionToken, String pk, String entityClassName)
    {
        log.debug("\n\nFinding Entity...");
        WebResource.Builder findBuilder = webResource
                .path(Constants.KUNDERA_API_PATH + "/crud/" + entityClassName + "/" + pk).accept(mediaType)
                .header(Constants.SESSION_TOKEN_HEADER_NAME, sessionToken);
        ;

        ClientResponse findResponse = (ClientResponse) findBuilder.get(ClientResponse.class);

        InputStream is = findResponse.getEntityInputStream();
        String entityStr = StreamUtils.toString(is);

        log.debug("Found Entity:" + entityStr);
        return entityStr;
    }

    @Override
    public String updateEntity(String sessionToken, String newEntityStr, String entityClassName)
    {
        log.debug("\n\nUpdating Entity... " + newEntityStr);
        WebResource.Builder updateBuilder = webResource.path(Constants.KUNDERA_API_PATH + "/crud/" + entityClassName)
                .type(mediaType).accept(mediaType).header(Constants.SESSION_TOKEN_HEADER_NAME, sessionToken);
        ClientResponse updateResponse = updateBuilder.put(ClientResponse.class, newEntityStr);
        InputStream is = updateResponse.getEntityInputStream();
        String updatedEntityStr = StreamUtils.toString(is);
        log.debug("Updated Entity: " + updatedEntityStr);
        return updatedEntityStr;
    }

    @Override
    public void deleteEntity(String sessionToken, String entityStr, String pk, String entityClassName)
    {
        log.debug("\n\nDeleting Entity... " + entityStr);
        WebResource.Builder deleteBuilder = webResource
                .path(Constants.KUNDERA_API_PATH + "/crud/" + entityClassName + "/delete/" + pk)
                .accept(MediaType.TEXT_PLAIN).header(Constants.SESSION_TOKEN_HEADER_NAME, sessionToken);
        ClientResponse deleteResponse = (ClientResponse) deleteBuilder.delete(ClientResponse.class);
        log.debug("Delete Response:" + deleteResponse.getStatus());
    }

    @Override
    public String runJPAQuery(String sessionToken, String jpaQuery, Map<String, Object> params)
    {
        log.debug("\n\nRunning JPA Query... ");

        String paramsStr = buildParamString(params);
        WebResource.Builder queryBuilder = webResource
                .path(Constants.KUNDERA_API_PATH + Constants.JPA_QUERY_RESOURCE_PATH + "/" + jpaQuery + paramsStr)
                .accept(mediaType).header(Constants.SESSION_TOKEN_HEADER_NAME, sessionToken);
        ClientResponse queryResponse = (ClientResponse) queryBuilder.get(ClientResponse.class);
        log.debug("Query Response:" + queryResponse.getStatus());

        InputStream is = queryResponse.getEntityInputStream();

        String allStr = StreamUtils.toString(is);

        log.debug("Found Entities:" + allStr);
        return allStr;
    }

    @Override
    public String runNamedJPAQuery(String sessionToken, String entityClassName, String namedQuery,
            Map<String, Object> params)
    {
        log.debug("\n\nRunning Named JPA Query " + namedQuery + "... ");

        String paramsStr = buildParamString(params);

        WebResource wr = webResource.path(Constants.KUNDERA_API_PATH + Constants.JPA_QUERY_RESOURCE_PATH + "/"
                + entityClassName + "/" + namedQuery + paramsStr);

        WebResource.Builder queryBuilder = wr.accept(mediaType).header(Constants.SESSION_TOKEN_HEADER_NAME,
                sessionToken);

        ClientResponse queryResponse = (ClientResponse) queryBuilder.get(ClientResponse.class);
        log.debug("Query Response:" + queryResponse.getStatus());

        InputStream is = queryResponse.getEntityInputStream();
        String allBookStr = StreamUtils.toString(is);

        log.debug("Found Entities for query " + namedQuery + ":" + allBookStr);
        return allBookStr;
    }

    @Override
    public String runNativeQuery(String sessionToken, String entityClassName, String nativeQuery,
            Map<String, Object> params)
    {
        log.debug("\n\nRunning Native Query... ");

        String paramsStr = buildParamString(params);
        WebResource.Builder queryBuilder = webResource
                .path(Constants.KUNDERA_API_PATH + Constants.NATIVE_QUERY_RESOURCE_PATH + entityClassName + "/q="
                        + nativeQuery + paramsStr).accept(mediaType)
                .header(Constants.SESSION_TOKEN_HEADER_NAME, sessionToken);
        ClientResponse queryResponse = (ClientResponse) queryBuilder.get(ClientResponse.class);
        log.debug("Query Response:" + queryResponse.getStatus());

        InputStream is = queryResponse.getEntityInputStream();

        String allStr = StreamUtils.toString(is);

        log.debug("Found Entities:" + allStr);
        return allStr;
    }

    @Override
    public String runNamedNativeQuery(String sessionToken, String entityClassName, String namedNativeQuery,
            Map<String, Object> params)
    {
        log.debug("\n\nRunning Named Native JPA Query " + namedNativeQuery + "... ");

        String paramsStr = buildParamString(params);

        WebResource wr = webResource.path(Constants.KUNDERA_API_PATH + Constants.NATIVE_QUERY_RESOURCE_PATH
                + entityClassName + "/" + namedNativeQuery + paramsStr);

        WebResource.Builder queryBuilder = wr.accept(mediaType).header(Constants.SESSION_TOKEN_HEADER_NAME,
                sessionToken);

        ClientResponse queryResponse = (ClientResponse) queryBuilder.get(ClientResponse.class);
        log.debug("Query Response:" + queryResponse.getStatus());

        InputStream is = queryResponse.getEntityInputStream();
        String allBookStr = StreamUtils.toString(is);

        log.debug("Found Entities for query " + namedNativeQuery + ":" + allBookStr);
        return allBookStr;
    }

    @Override
    public String getAllEntities(String sessionToken, String entityClassName)
    {
        log.debug("\n\nFinding all Entities... ");
        WebResource.Builder queryBuilder = webResource
                .path(Constants.KUNDERA_API_PATH + Constants.JPA_QUERY_RESOURCE_PATH + "/" + entityClassName + "/all")
                .accept(mediaType).header(Constants.SESSION_TOKEN_HEADER_NAME, sessionToken);
        ClientResponse queryResponse = (ClientResponse) queryBuilder.get(ClientResponse.class);
        log.debug("Find All Response:" + queryResponse.getStatus());

        InputStream is = queryResponse.getEntityInputStream();
        // List books = (List) JAXBUtils.toObject(is, ArrayList.class,
        // MediaType.APPLICATION_XML);

        String allEntitiesStr = StreamUtils.toString(is);

        log.debug("Found All Entities:" + allEntitiesStr);
        return allEntitiesStr;
    }

    @Override
    public String getSchemaList(String persistenceUnit)
    {
        log.debug("\n\nGetting Schema List for PU :" + persistenceUnit);
        WebResource.Builder slBuilder = webResource.path(
                Constants.KUNDERA_API_PATH + "/metadata/schemaList/" + persistenceUnit).accept(mediaType);
        ClientResponse schemaResponse = (ClientResponse) slBuilder.get(ClientResponse.class);

        InputStream is = schemaResponse.getEntityInputStream();
        String schemaList = StreamUtils.toString(is);

        log.debug("Schema List:" + schemaList);
        return schemaList;
    }

    @Override
    public String insertPerson(String sessionToken, String personStr)
    {

        log.debug("\n\nInserting Entity...");
        WebResource.Builder insertBuilder = webResource.path(Constants.KUNDERA_API_PATH + "/crud/PersonnelUni1ToM")
                .type(mediaType).accept(MediaType.APPLICATION_OCTET_STREAM)
                .header(Constants.SESSION_TOKEN_HEADER_NAME, sessionToken);
        StringBuffer sb = new StringBuffer().append(personStr);
        ClientResponse insertResponse = (ClientResponse) insertBuilder.post(ClientResponse.class, sb.toString());
        log.debug("Response From Insert person: " + insertResponse);
        return insertResponse.toString();

    }

    @Override
    public String findPerson(String sessionToken, String isbn)
    {

        log.debug("\n\nFinding Entity...");
        WebResource.Builder findBuilder = webResource
                .path(Constants.KUNDERA_API_PATH + "/crud/PersonnelUni1ToM/" + isbn).accept(mediaType)
                .header(Constants.SESSION_TOKEN_HEADER_NAME, sessionToken);
        ;

        ClientResponse findResponse = (ClientResponse) findBuilder.get(ClientResponse.class);

        InputStream is = findResponse.getEntityInputStream();
        String personkStr = StreamUtils.toString(is);

        log.debug("Found Entity:" + personkStr);
        return personkStr;
    }

    @Override
    public String updatePerson(String sessionToken, String oldPersonStr)
    {

        log.debug("\n\nUpdating Entity... " + oldPersonStr);
        oldPersonStr = oldPersonStr.replaceAll("XXXXXXXXX", "YYYYYYYYY");
        WebResource.Builder updateBuilder = webResource.path(Constants.KUNDERA_API_PATH + "/crud/PersonnelUni1ToM")
                .type(mediaType).accept(mediaType).header(Constants.SESSION_TOKEN_HEADER_NAME, sessionToken);
        ClientResponse updateResponse = updateBuilder.put(ClientResponse.class, oldPersonStr);
        InputStream is = updateResponse.getEntityInputStream();
        String updatedPersonStr = StreamUtils.toString(is);
        log.debug("Updated Person: " + updatedPersonStr);
        return updatedPersonStr;

    }

    @Override
    public String getAllPersons(String sessionToken)
    {
        log.debug("\n\nFinding all Entities... ");
        WebResource.Builder queryBuilder = webResource
                .path(Constants.KUNDERA_API_PATH + Constants.JPA_QUERY_RESOURCE_PATH + "/PersonnelUni1ToM/all")
                .accept(mediaType).header(Constants.SESSION_TOKEN_HEADER_NAME, sessionToken);
        ClientResponse queryResponse = (ClientResponse) queryBuilder.get(ClientResponse.class);
        log.debug("Find All Response:" + queryResponse.getStatus());

        InputStream is = queryResponse.getEntityInputStream();
        // List books = (List) JAXBUtils.toObject(is, ArrayList.class,
        // MediaType.APPLICATION_XML);

        String allPersonStr = StreamUtils.toString(is);

        log.debug("Found All Entities:" + allPersonStr);
        return allPersonStr;
    }

    @Override
    public void deletePerson(String sessionToken, String updatedPerson, String isbn)
    {
        log.debug("\n\nDeleting Entity... " + updatedPerson);
        WebResource.Builder deleteBuilder = webResource
                .path(Constants.KUNDERA_API_PATH + "/crud/PersonnelUni1ToM/delete/" + isbn)
                .accept(MediaType.TEXT_PLAIN).header(Constants.SESSION_TOKEN_HEADER_NAME, sessionToken);
        ClientResponse deleteResponse = (ClientResponse) deleteBuilder.delete(ClientResponse.class);
        log.debug("Delete Response:" + deleteResponse.getStatus());
    }

    /**
     * @param params
     * @return
     */
    private String buildParamString(Map<String, Object> params)
    {
        String paramsStr = "";
        if (params != null && !params.isEmpty())
        {
            paramsStr += "?";

            for (String paramName : params.keySet())
            {
                if (paramsStr.length() == 1)
                {
                    paramsStr += (paramName + "=" + params.get(paramName));
                }
                else
                {
                    paramsStr += ("&" + paramName + "=" + params.get(paramName));
                }
            }
        }
        return paramsStr;
    }

}
