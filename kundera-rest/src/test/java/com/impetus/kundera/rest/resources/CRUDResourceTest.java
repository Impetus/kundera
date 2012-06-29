/*******************************************************************************
 * * Copyright 2012 Impetus Infotech.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 ******************************************************************************/
package com.impetus.kundera.rest.resources;

import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.core.MediaType;

import junit.framework.Assert;

import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.rest.common.CassandraCli;
import com.impetus.kundera.rest.common.Constants;
import com.impetus.kundera.rest.dao.RESTClient;
import com.impetus.kundera.rest.dao.RESTClientImpl;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.test.framework.JerseyTest;

/**
 * Test case for {@link CRUDResource}
 * 
 * @author amresh
 * 
 */
public class CRUDResourceTest extends JerseyTest
{
    private static Log log = LogFactory.getLog(CRUDResourceTest.class);

    String mediaType = MediaType.APPLICATION_XML;

    RESTClient restClient;

    String applicationToken = null;

    String sessionToken = null;

    String bookStr1;

    String bookStr2;

    String pk1;

    String pk2;

    private final static boolean USE_EMBEDDED_SERVER = true;

    private final static boolean AUTO_MANAGE_SCHEMA = true;

    public CRUDResourceTest() throws Exception
    {
        super(Constants.KUNDERA_REST_RESOURCES_PACKAGE);
    }

    @Before
    public void setup() throws Exception
    {

        if (USE_EMBEDDED_SERVER)
        {
            CassandraCli.cassandraSetUp();
        }

        if (AUTO_MANAGE_SCHEMA)
        {
            CassandraCli.createKeySpace("KunderaExamples");
            loadData();
        }
    }

    @After
    public void tearDown() throws Exception
    {
        super.tearDown();

        if (AUTO_MANAGE_SCHEMA)
        {
            CassandraCli.dropKeySpace("KunderaExamples");
        }
    }

    @Test
    public void testCRUD()
    {

        WebResource webResource = resource();

        if (MediaType.APPLICATION_XML.equals(mediaType))
        {
            bookStr1 = "<book><isbn>1111111111111</isbn><author>Amresh</author><publication>Willey</publication></book>";
            bookStr2 = "<book><isbn>2222222222222</isbn><author>Vivek</author><publication>OReilly</publication></book>";
            pk1 = "1111111111111";
            pk2 = "2222222222222";
        }
        else if (MediaType.APPLICATION_JSON.equals(mediaType))
        {
            bookStr1 = "{book:{\"isbn\":\"1111111111111\",\"author\":\"Amresh\", \"publication\":\"Willey\"}}";
            bookStr2 = "{book:{\"isbn\":\"2222222222222\",\"author\":\"Vivek\", \"publication\":\"Oreilly\"}}";
            pk1 = "1111111111111";
            pk2 = "2222222222222";
        }
        else
        {
            Assert.fail("Incorrect Media Type:" + mediaType);
            return;
        }

        RESTClient restClient = new RESTClientImpl();
        // Initialize REST Client
        restClient.initialize(webResource, mediaType);

        // Get Application Token
        applicationToken = restClient.getApplicationToken("twissandra");
        Assert.assertNotNull(applicationToken);
        Assert.assertTrue(applicationToken.startsWith("AT_"));

        // Get Session Token
        sessionToken = restClient.getSessionToken(applicationToken);
        Assert.assertNotNull(sessionToken);
        Assert.assertTrue(sessionToken.startsWith("ST_"));

        // Insert Record
        String insertResponse1 = restClient.insertBook(sessionToken, bookStr1);
        String insertResponse2 = restClient.insertBook(sessionToken, bookStr2);
        
        Assert.assertNotNull(insertResponse1);
        Assert.assertNotNull(insertResponse2);
        Assert.assertTrue(insertResponse1.indexOf("201") > 0);
        Assert.assertTrue(insertResponse2.indexOf("201") > 0);
        

        // Find Record
        String foundBook = restClient.findBook(sessionToken, pk1);
        Assert.assertNotNull(foundBook);
        if (MediaType.APPLICATION_JSON.equals(mediaType))
        {
            foundBook = "{book:" + foundBook + "}";
        }
        Assert.assertTrue(foundBook.indexOf("Amresh") > 0);

        // Update Record
        String updatedBook = restClient.updateBook(sessionToken, foundBook);
        Assert.assertNotNull(updatedBook);
        Assert.assertTrue(updatedBook.indexOf("Saurabh") > 0);
        
        String jpaQuery = "select b from Book b";
        String queryResult = restClient.runQuery(sessionToken, jpaQuery);
        log.debug("Query Result:" + queryResult);

        // Get All Books
        String allBooks = restClient.getAllBooks(sessionToken);
        Assert.assertNotNull(allBooks);
        Assert.assertTrue(allBooks.indexOf("books") > 0);
        log.debug(allBooks);

        // Delete Records
        //restClient.deleteBook(sessionToken, updatedBook, pk1);
        //restClient.deleteBook(sessionToken, updatedBook, pk2);

        // Close Session
        restClient.closeSession(sessionToken);

        // Close Application
        restClient.closeApplication(applicationToken);
    }

    /**
     * Load cassandra specific data.
     * 
     * @throws TException
     *             the t exception
     * @throws InvalidRequestException
     *             the invalid request exception
     * @throws UnavailableException
     *             the unavailable exception
     * @throws TimedOutException
     *             the timed out exception
     * @throws SchemaDisagreementException
     *             the schema disagreement exception
     */
    private void loadData() throws TException, InvalidRequestException, UnavailableException, TimedOutException,
            SchemaDisagreementException
    {
        KsDef ksDef = null;
        CfDef user_Def = new CfDef();
        user_Def.name = "BOOK";
        user_Def.keyspace = "KunderaExamples";
        user_Def.setComparator_type("UTF8Type");
        user_Def.setDefault_validation_class("UTF8Type");

        List<CfDef> cfDefs = new ArrayList<CfDef>();
        cfDefs.add(user_Def);

        try
        {
            ksDef = CassandraCli.client.describe_keyspace("KunderaExamples");
            CassandraCli.client.set_keyspace("KunderaExamples");

            List<CfDef> cfDefn = ksDef.getCf_defs();

            for (CfDef cfDef1 : cfDefn)
            {
                if (cfDef1.getName().equalsIgnoreCase("BOOK"))
                {
                    CassandraCli.client.system_drop_column_family("BOOK");
                }
            }
            CassandraCli.client.system_add_column_family(user_Def);

        }
        catch (NotFoundException e)
        {

            ksDef = new KsDef("KunderaExamples", "org.apache.cassandra.locator.SimpleStrategy", cfDefs);
            ksDef.setReplication_factor(1);
            CassandraCli.client.system_add_keyspace(ksDef);
        }

        CassandraCli.client.set_keyspace("KunderaExamples");
    }
}
