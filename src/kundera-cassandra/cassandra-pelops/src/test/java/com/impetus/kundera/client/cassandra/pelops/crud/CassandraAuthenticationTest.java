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
package com.impetus.kundera.client.cassandra.pelops.crud;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.apache.cassandra.auth.PasswordAuthenticator;
import org.apache.cassandra.thrift.AuthenticationException;
import org.apache.cassandra.thrift.AuthenticationRequest;
import org.apache.cassandra.thrift.AuthorizationException;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.client.cassandra.persistence.CassandraCli;
/**
 * Cassandra authentication test.
 * 
 * @author vivek.mishra
 */
public class CassandraAuthenticationTest /*extends BaseTest*/
{

    private String userName;

    private String password;

    /**
     * Sets the up.
     * 
     * @throws Exception
     *             the exception
     */
    @Before
    public void setUp() throws Exception
    {
        // userName = "kunderauser";
        // password = "kunderapassword";
        // System.setProperty("passwd.properties",
        // "../resources/passwd.properties");
        // System.setProperty("access.properties",
        // "../resources/access.properties");
        //
        // CassandraCli.cassandraSetUp();
    }

    @Test
    public void testDummy()
    {
        // do nothing.
        // please do not modify this test at all!
    }

    /**
     * Authenticate with valid credentials.
     */
    // @Test
    public void authenticateWithValidCredentials()
    {
        try
        {
            EntityManagerFactory emf = Persistence.createEntityManagerFactory("authenticationTest");
            Assert.assertNotNull(emf);
            loadData();
            EntityManager em = emf.createEntityManager();
            Assert.assertNotNull(em);
            PersonAuth o = new PersonAuth();
            o.setPersonId("1");
            o.setPersonName("vivek");
            o.setAge(10);
            em.persist(o);

            PersonAuth p = em.find(PersonAuth.class, "1");
            Assert.assertNotNull(p);

        }
        catch (Exception e)
        {
            Assert.fail(e.getMessage());
        }

    }

    /**
     * Authenticate with invalid credentials.
     * 
     * @throws SchemaDisagreementException
     * @throws TimedOutException
     * @throws UnavailableException
     * @throws InvalidRequestException
     * @throws TException
     * @throws IOException
     */
    // @Test
    public void authenticateWithInValidCredentials() throws IOException, TException, InvalidRequestException,
            UnavailableException, TimedOutException, SchemaDisagreementException
    {
        EntityManagerFactory emf = null;
        EntityManager em = null;
        try
        {
            userName = "kunderauser";
            password = "invalid";
            loadData();
            emf = Persistence.createEntityManagerFactory("invalidauthenticationTest");
            em = emf.createEntityManager();
            Assert.fail("Shouldn't be called");
        }
        catch (AuthenticationException e)
        {
            Assert.assertNull(emf);
            Assert.assertNull(em);
            userName = "kunderauser";
            password = "kunderapassword";
        }
        catch (AuthorizationException e)
        {
            Assert.assertNull(emf);
            Assert.assertNull(em);
            userName = "kunderauser";
            password = "kunderapassword";
        }

    }

    /**
     * No authentication test.
     * 
     */
    // @Test
    public void noAuthenticationTest()
    {
        try
        {
            EntityManagerFactory emf = Persistence.createEntityManagerFactory("cass_pu");
            Assert.assertNotNull(emf);
            EntityManager em = emf.createEntityManager();
            Assert.assertNotNull(em);
        }
        catch (Exception e)
        {
            Assert.fail(e.getMessage());
        }

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
     * @throws AuthorizationException
     * @throws AuthenticationException
     */
    private void loadData() throws TException, InvalidRequestException, UnavailableException, TimedOutException,
            SchemaDisagreementException, AuthenticationException, AuthorizationException
    {

        KsDef ksDef = null;
        CfDef user_Def = new CfDef();
        user_Def.name = "PERSON";
        user_Def.keyspace = "KunderaAuthentication";
        user_Def.setComparator_type("UTF8Type");
        user_Def.setDefault_validation_class("UTF8Type");
        ColumnDef columnDef = new ColumnDef(ByteBuffer.wrap("PERSON_NAME".getBytes()), "UTF8Type");
        columnDef.index_type = IndexType.KEYS;
        user_Def.addToColumn_metadata(columnDef);
        ColumnDef columnDef1 = new ColumnDef(ByteBuffer.wrap("AGE".getBytes()), "UTF8Type");
        columnDef1.index_type = IndexType.KEYS;
        user_Def.addToColumn_metadata(columnDef1);

        List<CfDef> cfDefs = new ArrayList<CfDef>();
        cfDefs.add(user_Def);

        try
        {
            PasswordAuthenticator idAuth = new PasswordAuthenticator();
            Map<String, String> credentials = new HashMap<String, String>();
            credentials.put(idAuth.USERNAME_KEY, userName);
            credentials.put(idAuth.PASSWORD_KEY, password);
            CassandraCli.client.login(new AuthenticationRequest(credentials));

            CassandraCli.createKeySpace("KunderaAuthentication");

            ksDef = CassandraCli.client.describe_keyspace("KunderaAuthentication");
            CassandraCli.client.set_keyspace("KunderaAuthentication");

            List<CfDef> cfDefn = ksDef.getCf_defs();

            for (CfDef cfDef1 : cfDefn)
            {

                if (cfDef1.getName().equalsIgnoreCase("PERSON"))
                {

                    CassandraCli.client.system_drop_column_family("PERSON");

                }
            }
            CassandraCli.client.system_add_column_family(user_Def);

        }
        catch (NotFoundException e)
        {

            ksDef = new KsDef("KunderaAuthentication", "org.apache.cassandra.locator.SimpleStrategy", cfDefs);
            ksDef.setReplication_factor(1);
            CassandraCli.client.system_add_keyspace(ksDef);
        }

        CassandraCli.client.set_keyspace("KunderaAuthentication");

    }

    /**
     * Tear down.
     * 
     * @throws TException
     * @throws AuthorizationException
     * @throws AuthenticationException
     * 
     * @throws Exception
     *             the exception
     */
    @After
    public void tearDown() throws AuthenticationException, AuthorizationException, TException
    {
        // Map<String, String> credentials = new HashMap<String, String>();
        // credentials.put(IAuthenticator.USERNAME_KEY, userName);
        // credentials.put(IAuthenticator.PASSWORD_KEY, password);
        // CassandraCli.client.login(new AuthenticationRequest(credentials));
        //
        // CassandraCli.dropKeySpace("KunderaAuthentication");
    }
}