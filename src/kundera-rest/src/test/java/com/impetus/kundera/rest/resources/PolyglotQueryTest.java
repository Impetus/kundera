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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
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
import org.apache.commons.lang.StringUtils;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.client.cassandra.persistence.CassandraCli;
import com.impetus.kundera.rest.common.Constants;
import com.impetus.kundera.rest.common.JAXBUtils;
import com.impetus.kundera.rest.common.PersonalDetailCassandra;
import com.impetus.kundera.rest.common.Professional;
import com.impetus.kundera.rest.common.PreferenceCassandra;
import com.impetus.kundera.rest.common.ExternalLink;
import com.impetus.kundera.rest.common.ProfessionalDetailCassandra;
import com.impetus.kundera.rest.common.StreamUtils;
import com.impetus.kundera.rest.common.TweetCassandra;
import com.impetus.kundera.rest.common.UserCassandra;
import com.impetus.kundera.rest.dao.RESTClient;
import com.impetus.kundera.rest.dao.RESTClientImpl;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.test.framework.JerseyTest;

/**
 * Test for all data types using {@link Professional} entity Cassandra-CLI
 * Commands to run for non-embedded mode: create keyspace KunderaExamples; use
 * KunderaExamples; drop column family PROFESSIONAL; create column family
 * PROFESSIONAL with comparator=UTF8Type and key_validation_class=UTF8Type and
 * column_metadata=[ {column_name: DEPARTMENT_ID, validation_class:LongType,
 * index_type: KEYS}, {column_name: IS_EXCEPTIONAL,
 * validation_class:BooleanType, index_type: KEYS}, {column_name: AGE,
 * validation_class:IntegerType, index_type: KEYS}, {column_name: GRADE,
 * validation_class:UTF8Type, index_type: KEYS}, {column_name:
 * DIGITAL_SIGNATURE, validation_class:BytesType, index_type: KEYS},
 * {column_name: RATING, validation_class:IntegerType, index_type: KEYS},
 * {column_name: COMPLIANCE, validation_class:FloatType, index_type: KEYS},
 * {column_name: HEIGHT, validation_class:DoubleType, index_type: KEYS},
 * {column_name: ENROLMENT_DATE, validation_class:DateType, index_type: KEYS},
 * {column_name: ENROLMENT_TIME, validation_class:DateType, index_type: KEYS},
 * {column_name: JOINING_DATE_TIME, validation_class:DateType, index_type:
 * KEYS}, {column_name: YEARS_SPENT, validation_class:IntegerType, index_type:
 * KEYS}, {column_name: UNIQUE_ID, validation_class:LongType, index_type: KEYS},
 * {column_name: MONTHLY_SALARY, validation_class:DoubleType, index_type: KEYS},
 * {column_name: JOB_ATTEMPTS, validation_class:IntegerType, index_type: KEYS},
 * {column_name: ACCUMULATED_WEALTH, validation_class:DecimalType, index_type:
 * KEYS}, {column_name: GRADUATION_DAY, validation_class:DateType, index_type:
 * KEYS} ]; describe KunderaExamples;
 * 
 * @author amresh.singh
 */
public class PolyglotQueryTest extends JerseyTest
{
    private static final String _KEYSPACE = "KunderaExamples";
    
    private static final String _PU = "twissandra,twiMongo";

    private static Logger log = LoggerFactory.getLogger(DataTypeTest.class);

    static String mediaType = MediaType.APPLICATION_JSON;

    static RESTClient restClient;

    String applicationToken = null;

    String sessionToken = null;

    String userString;

    String pk1;

    String pk2;
    
    String userId1;
    
    String userId2;

    

    

    private final static boolean AUTO_MANAGE_SCHEMA = true;

    public PolyglotQueryTest() throws Exception
    {
        super(Constants.KUNDERA_REST_RESOURCES_PACKAGE);
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
    public void testUserCRUD()
    {
        WebResource webResource = resource();
        restClient = new RESTClientImpl();
        restClient.initialize(webResource, mediaType);

        buildUser1Str();
        // Get Application Token
        applicationToken = restClient.getApplicationToken(_PU);
        Assert.assertNotNull(applicationToken);
        Assert.assertTrue(applicationToken.startsWith("AT_"));

        // Get Session Token
        sessionToken = restClient.getSessionToken(applicationToken);
        Assert.assertNotNull(sessionToken);
        Assert.assertTrue(sessionToken.startsWith("ST_"));

        // Insert Record
        String insertResponse1 = restClient.insertEntity(sessionToken, userString, UserCassandra.class.getSimpleName());
       

        Assert.assertNotNull(insertResponse1);
        Assert.assertTrue(insertResponse1.indexOf("200") > 0);
       

        // Find Record
        String foundUser = restClient.findEntity(sessionToken, "0001", UserCassandra.class.getSimpleName());
        Assert.assertNotNull(foundUser);
        Assert.assertTrue(foundUser.indexOf("0001") > 0);
        

        // Update Record
        foundUser = foundUser.replaceAll("163.12", "165.21");
        
        String updatedUser = restClient.updateEntity(sessionToken, foundUser, UserCassandra.class.getSimpleName());
        
        Assert.assertNotNull(updatedUser);
        Assert.assertTrue(updatedUser.indexOf("165.21") > 0);

        /** JPA Query - Select */
        // Get All Professionals
        String jpaQuery = "select p from " + UserCassandra.class.getSimpleName() + " p";
        String queryResult = restClient.runJPAQuery(sessionToken, jpaQuery, new HashMap<String, Object>());
        log.debug("Query Result:" + queryResult);
        
        Assert.assertNotNull(queryResult);
        Assert.assertFalse(StringUtils.isEmpty(queryResult));
        Assert.assertTrue(queryResult.indexOf("usercassandra") > 0);
        Assert.assertTrue(queryResult.indexOf("0001") > 0);
        Assert.assertTrue(queryResult.indexOf("0002") > 0);
        Assert.assertTrue(queryResult.indexOf("0003") > 0);
        
        Assert.assertTrue(queryResult.indexOf("Motif") < 0);
       
        jpaQuery = "select p from " + UserCassandra.class.getSimpleName() + " p WHERE p.userId = :userId";
        Map<String, Object> params = new HashMap<String, Object>();
        
        params.put("userId", "0001");
        
        
        queryResult = restClient.runObjectJPAQuery(sessionToken, jpaQuery, params);
        log.debug("Query Result:" + queryResult);
        
        Assert.assertNotNull(queryResult);
        Assert.assertFalse(StringUtils.isEmpty(queryResult));
        Assert.assertTrue(queryResult.indexOf("usercassandra") > 0);
        Assert.assertTrue(queryResult.indexOf("0001") > 0);

        
        Assert.assertTrue(queryResult.indexOf("Motif") < 0);


        /** JPA Query - Select All */
        // Get All Professionals
        String allUsers = restClient.getAllEntities(sessionToken, UserCassandra.class.getSimpleName());
        log.debug(allUsers);
        Assert.assertNotNull(allUsers);
       

        // Close Session
        restClient.closeSession(sessionToken);

        // Close Application
        restClient.closeApplication(applicationToken);
    }
    
    private void buildUser1Str()
    {
    	 
    	userId1 = "0001";
    	userId2 = "0002";
    	
    	List<UserCassandra> friendList = new ArrayList<UserCassandra>();
    	
    	List<UserCassandra> followers = new ArrayList<UserCassandra>();
    	
    	 UserCassandra user1 = new UserCassandra(userId1, "Amresh", "password1", "married");
    	 
         UserCassandra user2 = new UserCassandra(userId2, "Vivek", "password1", "married");
         
         UserCassandra user3 = new UserCassandra("0003", "Kuldeep", "password1", "single");

         friendList.add(user2);
         Calendar cal = Calendar.getInstance();
         cal.setTime(new Date(1344079067777l));

         
         user1.setProfessionalDetail(new ProfessionalDetailCassandra(1234567, true, 31, 'C', (byte) 8, (short) 5, (float) 10.0, 163.12, new Date(
                 Long.parseLong("1351667541111")), new Date(Long.parseLong("1351667542222")), new Date(
                 Long.parseLong("1351667543333")), 2, new Long(3634521523423L), new Double(7.23452342343),
     
         new BigInteger("123456789"), new BigDecimal(123456789), cal));
         
         user2.setProfessionalDetail(new ProfessionalDetailCassandra(1234568, true, 21, 'C', (byte) 8, (short) 5, (float) 10.0, 163.12, new Date(
                 Long.parseLong("1351667541111")), new Date(Long.parseLong("1351667542222")), new Date(
                 Long.parseLong("1351667543333")), 2, new Long(3634521523423L), new Double(7.23452342343),
     
         new BigInteger("123456789"), new BigDecimal(123456789), cal));

         user1.setPreference(new PreferenceCassandra("P1", "Motif", "2"));
         
         followers.add(user3);
         
         friendList.add(user2);
         
         user1.setFriends(friendList);
         
         user1.setFollowers(followers);
         
         Set<ExternalLink> externalLinks = new HashSet<ExternalLink>();
         List<TweetCassandra> tweetList = new ArrayList<TweetCassandra>();
         
         externalLinks.add(new ExternalLink("L1", "Facebook", "http://facebook.com/coolnerd"));
         externalLinks.add(new ExternalLink("L2", "LinkedIn", "http://linkedin.com/in/devilmate"));
         user1.setExternalLinks(externalLinks);
         
         
         tweetList.add(new TweetCassandra("Here is my first tweet", "Web"));
         tweetList.add(new TweetCassandra("Second Tweet from me", "Mobile"));
         user1.setTweets(tweetList);
         
        
         userString = JAXBUtils.toString(UserCassandra.class, user1, mediaType);
         
         

    }


   

}
