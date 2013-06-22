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
package com.impetus.kundera.tests.embeddedRDBMS;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.crud.RDBMSCli;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.property.accessor.DateAccessor;
import com.impetus.kundera.tests.cli.CassandraCli;

/**
 * @author Kuldeep Mishra
 * 
 */
public class EmbeddedRDBMSUserTest
{
    private RDBMSCli cli;

    private static final String KEYSPACE = "KunderaTests";

    /** the log used by this class. */
    private static Logger log = LoggerFactory.getLogger(EmbeddedRDBMSUserTest.class);

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        KunderaMetadata.INSTANCE.setApplicationMetadata(null);
        try
        {
            CassandraCli.cassandraSetUp();
            CassandraCli.initClient();
            loadData();
            cli = new RDBMSCli(KEYSPACE);
            cli.createSchema(KEYSPACE);
            createTable();
        }
        catch (Exception e)
        {
            log.error("Error in RDBMS cli ", e);
        }
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        truncateRdbms();
        truncateColumnFamily();
        // HBaseCli.stopCluster();
    }

    @Test
    public void test()
    {

        EntityManagerFactory emf = Persistence
                .createEntityManagerFactory("rdbms,secIdxAddCassandra,piccandra,addMongo,picongo");
        EntityManager em = emf.createEntityManager();
        // em.getTransaction().begin();
        // em.setFlushMode(FlushModeType.COMMIT);

        // prepare user object.
        User user = addUser();

        // Add Personal Details
        addPersonalDetails(user);

        // Persist user with tweets.
        persist(user, em);
        em.clear();

        // Find by Id
        user = findByKey(em, user.getUserId());

        // Add tweets.(e.g. Association mapping)
        addTweets(user, "big data sample 1 for demo", DateAccessor.getDateByPattern("MAY/22/2012 8:12:30"), "tweet_1");
        addTweets(user, "big data sample 2 for demo", DateAccessor.getDateByPattern("22-MAY-2012"), "tweet_2");

        // Persist user with tweets.
        persist(user, em);
        em.clear();

        // Execute query.
        String query = "Select u from User u";
        findByQuery(em, query);

        // Query by parameter
        query = "Select u from User u where u.emailId =?1";

        // find by named parameter(e.g. email)
        findByEmail(em, query, "bigdata@impetus.com");

        // em.getTransaction().commit();

    }

    private static void addPersonalDetails(User user)
    {
        PersonalDetail personalDetail = new PersonalDetail();
        personalDetail.setName("bigdata");
        personalDetail.setPassword("xxxxxx");
        personalDetail.setPersonalDetailId("1");
        personalDetail.setAge(null);
        user.setPersonalDetail(personalDetail);
    }

    /**
     * @param user
     */
    private static void addTweets(User user, String body, Date tweetDate, String tweetId)
    {
        Tweets tweet = new Tweets();
        tweet.setTweetId(tweetId);
        tweet.setBody(body);
        tweet.setTweetDate(tweetDate);
        if (user.getTweets() == null)
        {
            Set<Tweets> tweets = new HashSet<Tweets>();
            tweets.add(tweet);
            user.setTweets(tweets);
        }
        else
        {
            user.getTweets().add(tweet);
        }

    }

    /**
     * @return
     */
    private static User addUser()
    {
        User user = new User();
        user.setUserId("impetus_user");
        user.setEmailId("bigdata@impetus.com");
        user.setFirstName("bigdata");
        user.setLastName("impetus");
        user.setTweets(new HashSet<Tweets>());
        return user;
    }

    /**
     * @param user
     */
    private static void persist(User user, EntityManager em)
    {
        em.persist(user);
    }

    /**
     * @param em
     * @param userId
     */
    private static User findByKey(EntityManager em, String userId)
    {
        User user = em.find(User.class, userId);
        System.out.println("[On Find by key]");
        System.out.println("#######################START##########################################");
        System.out.println("\n");
        System.out.println("\t\t User's first name:" + user.getFirstName());
        System.out.println("\t\t User's emailId:" + user.getEmailId());
        System.out.println("\t\t User's total tweets:" + user.getTweets());
        System.out.println("\n");
        System.out.println("#######################END############################################");
        System.out.println("\n");
        return user;
    }

    /**
     * @param em
     * @param query
     */
    private static void findByQuery(EntityManager em, String query)
    {
        Query q = em.createNamedQuery(query);

        System.out.println("[On Find All by Query]");
        List<User> users = q.getResultList();

        if (users == null)
        {
            System.out.println("0 Users Returned");
            return;
        }

        System.out.println("#######################START##########################################");
        System.out.println("\t\t Total number of users:" + users.size());
        System.out.println("\t\t User's total tweets:" + users.get(0).getTweets().size());
        printTweets(users);
        System.out.println("\n");
        // System.out.println("First tweet:" users.get(0).getTweets().);
        System.out.println("#######################END############################################");
        System.out.println("\n");
    }

    private static void findByEmail(EntityManager em, String query, String parameter)
    {
        Query q = em.createNamedQuery(query);
        System.out.println("[On Find by Email]");
        System.out.println("#######################START##########################################");
        q.setParameter(1, parameter);

        List<User> users = q.getResultList();

        if (users == null)
        {
            System.out.println("0 Users Returned");
            return;
        }

        printTweets(users);
        System.out.println("\n");
        System.out.println("#######################END############################################");
    }

    private static void printTweets(List<User> users)
    {
        // No null check as already knew it will not be null.
        Iterator<Tweets> tweets = users.get(0).getTweets().iterator();

        int counter = 1;
        while (tweets.hasNext())
        {
            System.out.println("\n");
            System.out.println("\t\t Tweet No:#" + counter++);
            Tweets rec = tweets.next();
            System.out.println("\t\t tweet is ->" + rec.getBody());
            System.out.println("\t\t Tweeted at ->" + rec.getTweetDate());
        }
    }

    private void createTable() throws SQLException
    {
        try
        {
            cli.update("CREATE TABLE KUNDERATESTS.user (emailId VARCHAR(150), first_name VARCHAR(150), last_name VARCHAR(150), name VARCHAR(150), password VARCHAR(150), personal_detail_id VARCHAR(150), rel_status VARCHAR(150), user_id VARCHAR(150),age INT)");
        }
        catch (Exception e)
        {
            cli.update("DELETE FROM KUNDERATESTS.user");
            cli.update("DROP TABLE KUNDERATESTS.user");
            cli.update("CREATE TABLE KUNDERATESTS.user (emailId VARCHAR(150), first_name VARCHAR(150), last_name VARCHAR(150), name VARCHAR(150), password VARCHAR(150), personal_detail_id VARCHAR(150), rel_status VARCHAR(150), user_id VARCHAR(150),age INT)");
        }
    }

    private void truncateRdbms()
    {
        try
        {
            cli.update("DELETE FROM KUNDERATESTS.user");
            cli.update("DROP TABLE KUNDERATESTS.user");
            cli.shutdown();
        }
        catch (Exception e)
        {
            // do nothing..weird!!
        }

    }

    private void truncateColumnFamily()
    {
        String[] columnFamily = new String[] { "tweets" };
        CassandraCli.truncateColumnFamily(KEYSPACE, columnFamily);
    }

    private void loadData() throws InvalidRequestException, TException, SchemaDisagreementException
    {
        KsDef ksDef = null;

        CfDef cfDef = new CfDef();
        cfDef.name = "tweets";
        cfDef.keyspace = KEYSPACE;
        // cfDef.column_type = "Super";
        cfDef.setComparator_type("UTF8Type");
        cfDef.setDefault_validation_class("UTF8Type");
        ColumnDef columnDefPersonName = new ColumnDef(ByteBuffer.wrap("body".getBytes()), "UTF8Type");
        columnDefPersonName.index_type = IndexType.KEYS;

        ColumnDef columnDefAddressId = new ColumnDef(ByteBuffer.wrap("tweeted_at".getBytes()), "DateType");
        columnDefAddressId.index_type = IndexType.KEYS;

        ColumnDef columnDefUserId = new ColumnDef(ByteBuffer.wrap("user_id".getBytes()), "UTF8Type");
        columnDefUserId.index_type = IndexType.KEYS;

        cfDef.addToColumn_metadata(columnDefPersonName);
        cfDef.addToColumn_metadata(columnDefAddressId);
        cfDef.addToColumn_metadata(columnDefUserId);

        List<CfDef> cfDefs = new ArrayList<CfDef>();
        cfDefs.add(cfDef);

        try
        {
            ksDef = com.impetus.kundera.tests.cli.CassandraCli.client.describe_keyspace(KEYSPACE);
            CassandraCli.client.set_keyspace(KEYSPACE);

            List<CfDef> cfDefn = ksDef.getCf_defs();

            // CassandraCli.client.set_keyspace("KunderaTests");
            for (CfDef cfDef1 : cfDefn)
            {

                if (cfDef1.getName().equalsIgnoreCase("tweets"))
                {

                    CassandraCli.client.system_drop_column_family("tweets");

                }
            }
            CassandraCli.client.system_add_column_family(cfDef);

        }
        catch (NotFoundException e)
        {
            addKeyspace(ksDef, cfDefs);
        }

        CassandraCli.client.set_keyspace(KEYSPACE);

    }

    private void addKeyspace(KsDef ksDef, List<CfDef> cfDefs) throws InvalidRequestException,
            SchemaDisagreementException, TException
    {
        ksDef = new KsDef(KEYSPACE, SimpleStrategy.class.getSimpleName(), cfDefs);
        // Set replication factor
        if (ksDef.strategy_options == null)
        {
            ksDef.strategy_options = new LinkedHashMap<String, String>();
        }
        // Set replication factor, the value MUST be an integer
        ksDef.strategy_options.put("replication_factor", "1");
        CassandraCli.client.system_add_keyspace(ksDef);
    }
}
