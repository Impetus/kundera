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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.core.MediaType;

import junit.framework.Assert;

import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.IndexType;
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
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.kundera.rest.common.Book;
import com.impetus.kundera.rest.common.CassandraCli;
import com.impetus.kundera.rest.common.Constants;
import com.impetus.kundera.rest.common.HabitatUni1ToM;
import com.impetus.kundera.rest.common.JAXBUtils;
import com.impetus.kundera.rest.common.PersonnelUni1ToM;
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
    private static final String _KEYSPACE = "KunderaExamples";

    private static Log log = LogFactory.getLog(CRUDResourceTest.class);

    static String mediaType = MediaType.APPLICATION_XML;

    static RESTClient restClient;

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

    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
        if (USE_EMBEDDED_SERVER)
        {
            CassandraCli.cassandraSetUp();
        }

        if (AUTO_MANAGE_SCHEMA)
        {
            CassandraCli.dropKeySpace(_KEYSPACE.toLowerCase());
            loadData();
        }

        // Initialize REST Client

    }

    @Before
    public void setup() throws Exception
    {

    }

    @After
    public void tearDown() throws Exception
    {
        super.tearDown();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception
    {

        if (AUTO_MANAGE_SCHEMA)
        {
            CassandraCli.dropKeySpace(_KEYSPACE);
        }
    }

    @Test
    public void testCRUD()
    {
        WebResource webResource = resource();
        restClient = new RESTClientImpl();
        restClient.initialize(webResource, mediaType);

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
        String queryResult = restClient.runJPAQuery(sessionToken, jpaQuery);
        log.debug("Query Result:" + queryResult);

        // Get All Books
        String allBooks = restClient.getAllBooks(sessionToken);
        Assert.assertNotNull(allBooks);
        Assert.assertTrue(allBooks.indexOf("books") > 0);
        Assert.assertTrue(allBooks.indexOf("Saurabh") > 0);
        Assert.assertTrue(allBooks.indexOf("Vivek") > 0);
        log.debug(allBooks);
        
        //Get books for a specific author
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("author", "Saurabh");
        String booksByAuthor = restClient.runNamedJPAQuery(sessionToken, Book.class.getSimpleName(), "findByAuthor", params);
        Assert.assertNotNull(booksByAuthor);
        Assert.assertTrue(booksByAuthor.indexOf("books") > 0);
        Assert.assertTrue(booksByAuthor.indexOf("Saurabh") > 0);
        Assert.assertFalse(booksByAuthor.indexOf("Vivek") > 0);
        log.debug(booksByAuthor);
        
        //Get books for a specific publication
        Map<String, Object> paramsPublication = new HashMap<String, Object>();
        paramsPublication.put("1", "Willey");
        String booksByPublication = restClient.runNamedJPAQuery(sessionToken, Book.class.getSimpleName(), "findByPublication", paramsPublication);
        Assert.assertNotNull(booksByPublication);
        Assert.assertTrue(booksByAuthor.indexOf("books") > 0);
        Assert.assertTrue(booksByAuthor.indexOf("Saurabh") > 0);
        Assert.assertFalse(booksByAuthor.indexOf("Vivek") > 0);
        Assert.assertTrue(booksByAuthor.indexOf("Willey") > 0);
        log.debug(booksByAuthor);
        
        // Delete Records
        restClient.deleteBook(sessionToken, updatedBook, pk1);
        restClient.deleteBook(sessionToken, updatedBook, pk2);

        // Close Session
        restClient.closeSession(sessionToken);

        // Close Application
        restClient.closeApplication(applicationToken);
    }

    @Test
    public void testCRUDOnAssociation()
    {
        String personStr;

        String personStr1;
        String personPk = "1234567";

        String person1Pk = "1234568";
        String addressPk = "201001";

        Set<HabitatUni1ToM> addresses = new HashSet<HabitatUni1ToM>();

        HabitatUni1ToM add1 = new HabitatUni1ToM();
        add1.setAddressId(addressPk);
        add1.setStreet("XXXXXXXXX");

        HabitatUni1ToM add = new HabitatUni1ToM();
        add.setAddressId(addressPk);
        add.setStreet("XXXXXXXXX");

        addresses.add(add1);
        addresses.add(add);

        PersonnelUni1ToM p = new PersonnelUni1ToM();
        p.setPersonId(personPk);
        p.setPersonName("kuldeep");
        p.setAddresses(addresses);

        PersonnelUni1ToM p1 = new PersonnelUni1ToM();
        p1.setPersonId(person1Pk);
        p1.setPersonName("kuldeep");
        p1.setAddresses(addresses);

        personStr = JAXBUtils.toString(PersonnelUni1ToM.class, p, mediaType);
        personStr1 = JAXBUtils.toString(PersonnelUni1ToM.class, p1, mediaType);

        // Get Application Token
        applicationToken = restClient.getApplicationToken("twissandra");
        Assert.assertNotNull(applicationToken);
        Assert.assertTrue(applicationToken.startsWith("AT_"));

        // Get Session Token
        sessionToken = restClient.getSessionToken(applicationToken);
        Assert.assertNotNull(sessionToken);
        Assert.assertTrue(sessionToken.startsWith("ST_"));

        // Insert person.
        String insertResponse = restClient.insertPerson(sessionToken, personStr);
        String insertResponse1 = restClient.insertPerson(sessionToken, personStr1);

        Assert.assertNotNull(insertResponse);
        Assert.assertTrue(insertResponse.indexOf("201") > 0);
        Assert.assertNotNull(insertResponse1);
        Assert.assertTrue(insertResponse1.indexOf("201") > 0);

        // Find person.
        String foundPerson = restClient.findPerson(sessionToken, personPk);
        Assert.assertNotNull(foundPerson);
        if (MediaType.APPLICATION_JSON.equals(mediaType))
        {
            foundPerson = "{personnelUni1ToM:" + foundPerson + "}";
        }
        Assert.assertTrue(foundPerson.indexOf(addressPk) > 0);
        Assert.assertTrue(foundPerson.indexOf("XXXXXXXXX") > 0);
        Assert.assertTrue(foundPerson.indexOf("kuldeep") > 0);

        // Update Record
        String updatedPerson = restClient.updatePerson(sessionToken, foundPerson);
        Assert.assertNotNull(updatedPerson);
        Assert.assertTrue(updatedPerson.indexOf("YYYYYYYYY") > 0);

        // Find all persons.
        String allPersons = restClient.getAllPersons(sessionToken);
        Assert.assertNotNull(allPersons);
        Assert.assertTrue(allPersons.indexOf("personneluni1toms") > 0);
        log.debug(allPersons);

        // Run Query.
        String jpaQuery = "select p from PersonnelUni1ToM p where p.personId >= " + person1Pk;
        String queryResult = restClient.runJPAQuery(sessionToken, jpaQuery);
        log.debug("Query Result:" + queryResult);

        // Delete person.
        restClient.deletePerson(sessionToken, updatedPerson, personPk);
        foundPerson = restClient.findPerson(sessionToken, personPk);
        Assert.assertEquals("", foundPerson);

        // NamedQuery on person.
        restClient.executeNamedQuery();

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
    private static void loadData() throws TException, InvalidRequestException, UnavailableException, TimedOutException,
            SchemaDisagreementException
    {
        KsDef ksDef = null;
        CfDef user_Def = new CfDef();
        user_Def.name = "BOOK";
        user_Def.keyspace = _KEYSPACE;
        user_Def.setComparator_type("UTF8Type");
        user_Def.setDefault_validation_class("UTF8Type");
        
        ColumnDef authorDef = new ColumnDef(ByteBuffer.wrap("AUTHOR".getBytes()), "UTF8Type"); authorDef.index_type = IndexType.KEYS;
        ColumnDef publicationDef = new ColumnDef(ByteBuffer.wrap("PUBLICATION".getBytes()), "UTF8Type"); publicationDef.index_type = IndexType.KEYS;
        user_Def.addToColumn_metadata(authorDef);
        user_Def.addToColumn_metadata(publicationDef);      

        CfDef person_Def = new CfDef();
        person_Def.name = "PERSONNEL";
        person_Def.keyspace = _KEYSPACE;
        // cfDef.column_type = "Super";
        person_Def.setComparator_type("UTF8Type");
        person_Def.setDefault_validation_class("UTF8Type");
        ColumnDef columnDef = new ColumnDef(ByteBuffer.wrap("PERSON_NAME".getBytes()), "UTF8Type");
        person_Def.addToColumn_metadata(columnDef);

        CfDef address_Def = new CfDef();
        address_Def.name = "ADDRESS";
        address_Def.keyspace = _KEYSPACE;

        ColumnDef street = new ColumnDef(ByteBuffer.wrap("STREET".getBytes()), "UTF8Type");
        street.index_type = IndexType.KEYS;
        address_Def.addToColumn_metadata(street);

        ColumnDef personId = new ColumnDef(ByteBuffer.wrap("PERSON_ID".getBytes()), "IntegerType");
        personId.index_type = IndexType.KEYS;
        address_Def.addToColumn_metadata(personId);

        List<CfDef> cfDefs = new ArrayList<CfDef>();
        cfDefs.add(user_Def);
        cfDefs.add(person_Def);
        cfDefs.add(address_Def);

        try
        {
            ksDef = CassandraCli.client.describe_keyspace(_KEYSPACE);
            CassandraCli.client.set_keyspace(_KEYSPACE);

            List<CfDef> cfDefn = ksDef.getCf_defs();

            for (CfDef cfDef1 : cfDefn)
            {
                if (cfDef1.getName().equalsIgnoreCase("BOOK"))
                {
                    CassandraCli.client.system_drop_column_family("BOOK");
                }
                if (cfDef1.getName().equalsIgnoreCase("PERSONNEL"))
                {

                    CassandraCli.client.system_drop_column_family("PERSONNEL");

                }
                if (cfDef1.getName().equalsIgnoreCase("ADDRESS"))
                {

                    CassandraCli.client.system_drop_column_family("ADDRESS");

                }
            }
            CassandraCli.client.system_add_column_family(user_Def);
            CassandraCli.client.system_add_column_family(person_Def);
            CassandraCli.client.system_add_column_family(address_Def);

        }
        catch (NotFoundException e)
        {

            ksDef = new KsDef(_KEYSPACE, "org.apache.cassandra.locator.SimpleStrategy", cfDefs);
            Map<String, String> strategy_options = new HashMap<String, String>();
            strategy_options.put("replication_factor", "1");
            ksDef.setStrategy_options(strategy_options);
            CassandraCli.client.system_add_keyspace(ksDef);
        }

        CassandraCli.client.set_keyspace(_KEYSPACE);
    }
}
