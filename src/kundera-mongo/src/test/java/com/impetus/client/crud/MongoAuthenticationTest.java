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
package com.impetus.client.crud;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.client.utils.MongoUtils;
import com.impetus.kundera.loader.KunderaAuthenticationException;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

/**
 * The Class MongoAuthenticationTest.
 * 
 * @author Devender yadav
 */
public class MongoAuthenticationTest
{

    private final static String ADMIN_DB = "admin";

    private final static String DB = "KunderaAuthTests";

    private static String _PU;

    private static EntityManagerFactory emf;

    private static MongoClient m;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
        m = new MongoClient(new ServerAddress("localhost", 27017));
        addUsers();
    }

    /**
     * Authenticate with valid credentials
     * 
     * 
     */
    // @Test
    public void authenticateWithValidCredentials()
    {
        try
        {
            _PU = "validAuthenticationMongoPu";
            String dbname = "KunderaAuthTests";
            String username = "kunderaUser";
            String password = "kunderapassword";

            MongoCredential credential = MongoCredential.createMongoCRCredential(username, dbname,
                    password.toCharArray());

            MongoClient m = new MongoClient(new ServerAddress("localhost", 27017), Arrays.asList(credential));

            DB db = m.getDB(dbname);

            Assert.assertNotNull(db.getCollectionNames());

            m.close();
            emf = Persistence.createEntityManagerFactory(_PU);
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
     * Authenticate with valid credentials in case of multiple DBs
     * 
     * Here user is in "admin" database assigned a role readWriteAnyDatabase. So
     * it can access "KunderaAuthTests" database too.
     */
    // @Test
    public void authenticateWithValidCredentialsMultipleDbs()
    {
        try
        {
            _PU = "validAuthenticationMongoPuAdminDb";
            String adminDb = "admin";
            String dbname = "KunderaAuthTests";
            String username = "admin";
            String password = "password";

            MongoCredential credential = MongoCredential.createMongoCRCredential(username, adminDb,
                    password.toCharArray());
            MongoClient m = new MongoClient(new ServerAddress("localhost", 27017), Arrays.asList(credential));

            DB db = m.getDB(dbname);
            DB db_admin = m.getDB(adminDb);

            Assert.assertNotNull(db.getCollectionNames());
            Assert.assertNotNull(db_admin.getCollectionNames());

            m.close();
            emf = Persistence.createEntityManagerFactory(_PU);
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
     * Authenticate with invalid credentials.
     * 
     */
    // @Test
    public void authenticateWithInValidCredentials()
    {
        Set<String> collectionList = new HashSet<String>();
        try
        {
            _PU = "validAuthenticationMongoPu";
            String dbname = "KunderaAuthTests";
            String username = "kunderaUser";
            String password = "wrongPassword";

            MongoCredential credential = MongoCredential.createMongoCRCredential(username, dbname,
                    password.toCharArray());

            MongoClient m = new MongoClient(new ServerAddress("localhost", 27017), Arrays.asList(credential));

            DB db = m.getDB(dbname);
            collectionList = db.getCollectionNames();

            Assert.fail("Shouldn't be called");

            m.close();
            emf = Persistence.createEntityManagerFactory(_PU);
            Assert.assertNotNull(emf);
            EntityManager em = emf.createEntityManager();
            Assert.assertNotNull(em);
        }
        catch (Exception e)
        {
            Assert.assertEquals(true, collectionList.isEmpty());
        }
    }

    /**
     * Authenticate with invalid credentials in Persistence Unit.
     */
    // @Test
    public void authenticateWithInValidCredentialsPu()
    {
        EntityManager em = null;
        try
        {
            _PU = "invalidAuthenticationMongoPu";
            emf = Persistence.createEntityManagerFactory(_PU);
            em = emf.createEntityManager();
            Assert.fail("Shouldn't be called");
        }
        catch (KunderaAuthenticationException e)
        {
            // Moved authentication at client level.
            // Assert.assertNull(emf);
            Assert.assertNull(em);
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
            _PU = "mongoTest";
            String dbname = "KunderaAuthTests";
            MongoClient m = new MongoClient(new ServerAddress("localhost", 27017));
            DB db = m.getDB(dbname);

            Assert.assertNotNull(db.getCollectionNames());

            emf = Persistence.createEntityManagerFactory(_PU);
            Assert.assertNotNull(emf);
            EntityManager em = emf.createEntityManager();
            Assert.assertNotNull(em);
        }
        catch (Exception e)
        {
            Assert.fail(e.getMessage());
        }
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception
    {
        dropUsers();
        MongoUtils.dropDatabase(emf, _PU);
    }

    private static void addUsers()
    {
        DB db = m.getDB(DB);
        Map<String, Object> commandArguments = new BasicDBObject();
        commandArguments.put("createUser", "kunderaUser");
        commandArguments.put("pwd", "kunderapassword");
        String[] roles = { "readWrite" };
        commandArguments.put("roles", roles);
        BasicDBObject command = new BasicDBObject(commandArguments);
        db.command(command);

        DB db_admin = m.getDB(ADMIN_DB);
        commandArguments = new BasicDBObject();
        commandArguments.put("createUser", "admin");
        commandArguments.put("pwd", "password");
        String[] admin_roles = { "readWriteAnyDatabase" };
        commandArguments.put("roles", admin_roles);
        command = new BasicDBObject(commandArguments);
        db_admin.command(command);
    }

    private static void dropUsers()
    {
        DB db_admin = m.getDB(ADMIN_DB);
        db_admin.command(new BasicDBObject("dropUser", "admin"));

        DB db = m.getDB(DB);
        db.command(new BasicDBObject("dropUser", "kunderaUser"));
    }
}