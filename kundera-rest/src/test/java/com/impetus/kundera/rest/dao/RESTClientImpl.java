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
import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.core.MediaType;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.rest.common.Constants;
import com.impetus.kundera.rest.common.JAXBUtils;
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
    private static Log log = LogFactory.getLog(RESTClientImpl.class);

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
            WebResource.Builder atBuilder = webResource.path("kundera/api/application/" + persistenceUnit).accept(
                    MediaType.TEXT_PLAIN);
            String atResponse = atBuilder.get(ClientResponse.class).toString();
            applicationToken = atBuilder.get(String.class);
            log.debug("Response: " + atResponse);
            log.debug("Application Token:" + applicationToken);
        }
        catch (UniformInterfaceException e)
        {
            e.printStackTrace();
            return null;
        }
        catch (ClientHandlerException e)
        {
            e.printStackTrace();
            return null;
        }
        return applicationToken;
    }

    @Override
    public String closeApplication(String applicationToken)
    {
        log.debug("\n\nClosing Application for Token:" + applicationToken);
        WebResource.Builder atBuilder = webResource.path("kundera/api/application").accept(MediaType.TEXT_PLAIN)
                .header(Constants.APPLICATION_TOKEN_HEADER_NAME, applicationToken);
        String response = atBuilder.delete(String.class);
        log.debug("Application Closure Response: " + response);
        return response;
    }

    @Override
    public String getSessionToken(String applicationToken)
    {
        log.debug("\n\nGetting Session Token...");
        WebResource.Builder stBuilder = webResource.path("kundera/api/session").accept(MediaType.TEXT_PLAIN)
                .header(Constants.APPLICATION_TOKEN_HEADER_NAME, applicationToken);

        String sessionToken = stBuilder.get(String.class);

        log.debug("Session Token:" + sessionToken);
        return sessionToken;
    }

    @Override
    public String closeSession(String sessionToken)
    {

        log.debug("\n\nClosing Session for Token:" + sessionToken);
        WebResource.Builder stBuilder = webResource.path("kundera/api/session").accept(MediaType.TEXT_PLAIN)
                .header(Constants.SESSION_TOKEN_HEADER_NAME, sessionToken);
        String response = stBuilder.delete(String.class);
        log.debug("Session Deletion Response: " + response);
        return response;
    }

    @Override
    public String insertBook(String sessionToken, String bookStr)
    {
        log.debug("\n\nInserting Entity...");
        WebResource.Builder insertBuilder = webResource.path("kundera/api/crud/Book").type(mediaType)
                .accept(MediaType.APPLICATION_OCTET_STREAM).header(Constants.SESSION_TOKEN_HEADER_NAME, sessionToken);
        StringBuffer sb = new StringBuffer().append(bookStr);
        ClientResponse insertResponse = (ClientResponse) insertBuilder.post(ClientResponse.class, sb.toString());
        log.debug("Response From Insert Book: " + insertResponse);
        return insertResponse.toString();
    }

    @Override
    public String findBook(String sessionToken, String isbn)
    {
        log.debug("\n\nFinding Entity...");
        WebResource.Builder findBuilder = webResource.path("kundera/api/crud/Book/" + isbn).accept(mediaType)
                .header(Constants.SESSION_TOKEN_HEADER_NAME, sessionToken);
        ;

        ClientResponse findResponse = (ClientResponse) findBuilder.get(ClientResponse.class);

        InputStream is = findResponse.getEntityInputStream();
        String bookStr = StreamUtils.toString(is);

        log.debug("Found Entity:" + bookStr);
        return bookStr;
    }

    @Override
    public String updateBook(String sessionToken, String oldBookStr)
    {
        log.debug("\n\nUpdating Entity... " + oldBookStr);
        oldBookStr = oldBookStr.replaceAll("Amresh", "Saurabh");
        WebResource.Builder updateBuilder = webResource.path("kundera/api/crud/Book").type(mediaType).accept(mediaType)
                .header(Constants.SESSION_TOKEN_HEADER_NAME, sessionToken);
        ClientResponse updateResponse = updateBuilder.put(ClientResponse.class, oldBookStr);
        InputStream is = updateResponse.getEntityInputStream();
        String updatedBookStr = StreamUtils.toString(is);
        log.debug("Updated Book: " + updatedBookStr);
        return updatedBookStr;
    }

    @Override
    public void deleteBook(String sessionToken, String updatedBook, String isbn)
    {
        log.debug("\n\nDeleting Entity... " + updatedBook);
        WebResource.Builder deleteBuilder = webResource.path("kundera/api/crud/Book/delete/" + isbn)
                .accept(MediaType.TEXT_PLAIN).header(Constants.SESSION_TOKEN_HEADER_NAME, sessionToken);
        ClientResponse deleteResponse = (ClientResponse) deleteBuilder.delete(ClientResponse.class);
        log.debug("Delete Response:" + deleteResponse.getStatus());
    }

    @Override
    public String runQuery(String sessionToken, String jpaQuery)
    {
        log.debug("\n\nRunning JPA Query... ");
        WebResource.Builder queryBuilder = webResource.path("kundera/api/query/" + jpaQuery).accept(mediaType)
                .header(Constants.SESSION_TOKEN_HEADER_NAME, sessionToken);
        ClientResponse queryResponse = (ClientResponse) queryBuilder.get(ClientResponse.class);
        log.debug("Query Response:" + queryResponse.getStatus());

        InputStream is = queryResponse.getEntityInputStream();
        List records = (List) JAXBUtils.toObject(is, ArrayList.class, MediaType.APPLICATION_XML);

        String allStr = StreamUtils.toString(is);

        log.debug("Found Entities:" + allStr);
        return allStr;
    }

    @Override
    public String getAllBooks(String sessionToken)
    {
        log.debug("\n\nFinding all Entities... ");
        WebResource.Builder queryBuilder = webResource.path("kundera/api/query/Book/all").accept(mediaType)
                .header(Constants.SESSION_TOKEN_HEADER_NAME, sessionToken);
        ClientResponse queryResponse = (ClientResponse) queryBuilder.get(ClientResponse.class);
        log.debug("Find All Response:" + queryResponse.getStatus());

        InputStream is = queryResponse.getEntityInputStream();
        // List books = (List) JAXBUtils.toObject(is, ArrayList.class,
        // MediaType.APPLICATION_XML);

        String allBookStr = StreamUtils.toString(is);

        log.debug("Found All Entities:" + allBookStr);
        return allBookStr;
    }

    @Override
    public String getSchemaList(String persistenceUnit)
    {
        log.debug("\n\nGetting Schema List for PU :" + persistenceUnit);
        WebResource.Builder slBuilder = webResource.path("kundera/api/metadata/schemaList/" + persistenceUnit).accept(
                mediaType);
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
        WebResource.Builder insertBuilder = webResource.path("kundera/api/crud/PersonnelUni1ToM").type(mediaType)
                .accept(MediaType.APPLICATION_OCTET_STREAM).header(Constants.SESSION_TOKEN_HEADER_NAME, sessionToken);
        StringBuffer sb = new StringBuffer().append(personStr);
        ClientResponse insertResponse = (ClientResponse) insertBuilder.post(ClientResponse.class, sb.toString());
        log.debug("Response From Insert person: " + insertResponse);
        return insertResponse.toString();

    }

    @Override
    public String findPerson(String sessionToken, String isbn)
    {

        log.debug("\n\nFinding Entity...");
        WebResource.Builder findBuilder = webResource.path("kundera/api/crud/PersonnelUni1ToM/" + isbn)
                .accept(mediaType).header(Constants.SESSION_TOKEN_HEADER_NAME, sessionToken);
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
        WebResource.Builder updateBuilder = webResource.path("kundera/api/crud/PersonnelUni1ToM").type(mediaType)
                .accept(mediaType).header(Constants.SESSION_TOKEN_HEADER_NAME, sessionToken);
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
        WebResource.Builder queryBuilder = webResource.path("kundera/api/query/PersonnelUni1ToM/all").accept(mediaType)
                .header(Constants.SESSION_TOKEN_HEADER_NAME, sessionToken);
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
        WebResource.Builder deleteBuilder = webResource.path("kundera/api/crud/PersonnelUni1ToM/delete/" + isbn)
                .accept(MediaType.TEXT_PLAIN).header(Constants.SESSION_TOKEN_HEADER_NAME, sessionToken);
        ClientResponse deleteResponse = (ClientResponse) deleteBuilder.delete(ClientResponse.class);
        log.debug("Delete Response:" + deleteResponse.getStatus());
    }

    @Override
    public String executeNamedQuery()
    {
        log.debug("\n\nExecuting named Query... ");

        return null;
    }
}
