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
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriBuilder;

import com.impetus.kundera.rest.common.JAXBUtils;
import com.impetus.kundera.rest.common.StreamUtils;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;

/**
 * <Prove description of functionality provided by this Type>
 * 
 * @author amresh.singh
 */
public class RESTClientImpl implements RESTClient
{
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
            System.out.println("\n\nGetting Application Token...");
            WebResource.Builder atBuilder = webResource.path("kundera/api/application/pu/" + persistenceUnit)
                    .accept(MediaType.TEXT_PLAIN);
            String atResponse = atBuilder.get(ClientResponse.class).toString();
            applicationToken = atBuilder.get(String.class);
            System.out.println("Response: " + atResponse);
            System.out.println("Application Token:" + applicationToken);
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
    public void closeApplication(String applicationToken)
    {
        System.out.println("\n\nClosing Application for Token:" + applicationToken);
        WebResource.Builder atBuilder = webResource.path("kundera/api/application/" + applicationToken)
                .accept(MediaType.TEXT_PLAIN);
        String response = atBuilder.delete(String.class);
        System.out.println("Application Closure Response: " + response);
    }

    @Override
    public String getSessionToken(String applicationToken)
    {
        System.out.println("\n\nGetting Session Token...");
        WebResource.Builder stBuilder = webResource.path("kundera/api/session/at/" + applicationToken)
                .accept(MediaType.TEXT_PLAIN);
        String stResponse = stBuilder.get(ClientResponse.class).toString();
        String sessionToken = stBuilder.get(String.class);
        System.out.println("Response: " + stResponse);
        System.out.println("Session Token:" + sessionToken);
        return sessionToken;
    }

    @Override
    public void closeSession(String sessionToken)
    {

        System.out.println("\n\nClosing Session for Token:" + sessionToken);
        WebResource.Builder stBuilder = webResource.path("kundera/api/session/" + sessionToken)
                .accept(MediaType.TEXT_PLAIN);
        String response = stBuilder.delete(String.class);
        System.out.println("Session Deletion Response: " + response);
    }
    

    @Override
    public String insertBook(String sessionToken, String bookStr)
    {
        System.out.println("\n\nInserting Entity...");
        WebResource.Builder insertBuilder = webResource.path("kundera/api/crud/" + sessionToken + "/Book")
                .type(mediaType).accept(MediaType.APPLICATION_OCTET_STREAM);
        StringBuffer sb = new StringBuffer()
                .append(bookStr);
        ClientResponse insertResponse = (ClientResponse)insertBuilder.post(ClientResponse.class, sb.toString());
        System.out.println("Response From Insert Book: " + insertResponse);
        return insertResponse.toString();
    }

    @Override
    public String findBook(String sessionToken, String isbn)
    {
        System.out.println("\n\nFinding Entity...");
        WebResource.Builder findBuilder = webResource
                .path("kundera/api/crud/" + sessionToken + "/Book/" + isbn).accept(mediaType);
        
        ClientResponse findResponse = (ClientResponse)findBuilder.get(ClientResponse.class);
        
        InputStream is = findResponse.getEntityInputStream();
        String bookStr = StreamUtils.toString(is);  
        
        System.out.println("Found Entity:" + bookStr);
        return bookStr;
    }

    @Override
    public String updateBook(String sessionToken, String oldBookStr)
    {
        System.out.println("\n\nUpdating Entity... " + oldBookStr);
        oldBookStr = oldBookStr.replaceAll("Amresh", "Saurabh");        
        WebResource.Builder updateBuilder = webResource.path("kundera/api/crud/" + sessionToken + "/Book")
                .type(mediaType).accept(mediaType);
        ClientResponse updateResponse = updateBuilder.put(ClientResponse.class, oldBookStr);
        InputStream is = updateResponse.getEntityInputStream();
        String updatedBookStr = StreamUtils.toString(is);
        System.out.println("Updated Book: " + updatedBookStr);
        return updatedBookStr;
    }

    @Override
    public void deleteBook(String sessionToken, String updatedBook, String isbn)
    {
        System.out.println("\n\nDeleting Entity... " + updatedBook);
        WebResource.Builder deleteBuilder = webResource.path("kundera/api/crud/" + sessionToken + "/Book/delete/" + isbn)
                .accept(MediaType.TEXT_PLAIN);
        ClientResponse deleteResponse = (ClientResponse) deleteBuilder.delete(ClientResponse.class);
        System.out.println("Delete Response:" + deleteResponse.getStatus());
    }

    @Override
    public String getAllBooks(String sessionToken)
    {
        System.out.println("\n\nFinding all Entities... ");
        WebResource.Builder queryBuilder = webResource
                .path("kundera/api/query/" + sessionToken + "/Book/all")
                .accept(mediaType);
        ClientResponse queryResponse = (ClientResponse) queryBuilder.get(ClientResponse.class);
        System.out.println("Find All Response:" + queryResponse.getStatus());
        
        InputStream is = queryResponse.getEntityInputStream();
        List books = (List)JAXBUtils.toObject(is, ArrayList.class, MediaType.APPLICATION_XML);
        
        String allBookStr = StreamUtils.toString(is);  
        
        System.out.println("Found All Entities:" + allBookStr);
        return "";
    }

    @Override
    public String getSchemaList(String persistenceUnit)
    {
        System.out.println("\n\nGetting Schema List for PU :" + persistenceUnit);
        WebResource.Builder slBuilder = webResource.path("kundera/api/metadata/schemaList/" + persistenceUnit)
                .accept(mediaType);
        ClientResponse schemaResponse = (ClientResponse)slBuilder.get(ClientResponse.class);
        
        InputStream is = schemaResponse.getEntityInputStream();
        String schemaList = StreamUtils.toString(is);  
        
        System.out.println("Schema List:" + schemaList);
        return schemaList;        
    }    

}
