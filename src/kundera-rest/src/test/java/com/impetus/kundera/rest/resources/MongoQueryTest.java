/**
 * Copyright 2014 Impetus Infotech.
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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.ws.rs.core.MediaType;

import junit.framework.Assert;

import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.client.cassandra.persistence.CassandraCli;
import com.impetus.kundera.rest.common.Constants;
import com.impetus.kundera.rest.common.JAXBUtils;
import com.impetus.kundera.rest.common.MongoCompoundKey;
import com.impetus.kundera.rest.common.MongoPrimeUser;
import com.impetus.kundera.rest.common.Professional;
import com.impetus.kundera.rest.dao.RESTClient;
import com.impetus.kundera.rest.dao.RESTClientImpl;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.test.framework.JerseyTest;

/**
 * Test for all data types using {@link Professional} entity Cassandra-CLI Commands to run for non-embedded mode: create
 * keyspace KunderaExamples; use KunderaExamples; drop column family PROFESSIONAL; create column family PROFESSIONAL
 * with comparator=UTF8Type and key_validation_class=UTF8Type and column_metadata=[ {column_name: DEPARTMENT_ID,
 * validation_class:LongType, index_type: KEYS}, {column_name: IS_EXCEPTIONAL, validation_class:BooleanType, index_type:
 * KEYS}, {column_name: AGE, validation_class:IntegerType, index_type: KEYS}, {column_name: GRADE,
 * validation_class:UTF8Type, index_type: KEYS}, {column_name: DIGITAL_SIGNATURE, validation_class:BytesType,
 * index_type: KEYS}, {column_name: RATING, validation_class:IntegerType, index_type: KEYS}, {column_name: COMPLIANCE,
 * validation_class:FloatType, index_type: KEYS}, {column_name: HEIGHT, validation_class:DoubleType, index_type: KEYS},
 * {column_name: ENROLMENT_DATE, validation_class:DateType, index_type: KEYS}, {column_name: ENROLMENT_TIME,
 * validation_class:DateType, index_type: KEYS}, {column_name: JOINING_DATE_TIME, validation_class:DateType, index_type:
 * KEYS}, {column_name: YEARS_SPENT, validation_class:IntegerType, index_type: KEYS}, {column_name: UNIQUE_ID,
 * validation_class:LongType, index_type: KEYS}, {column_name: MONTHLY_SALARY, validation_class:DoubleType, index_type:
 * KEYS}, {column_name: JOB_ATTEMPTS, validation_class:IntegerType, index_type: KEYS}, {column_name: ACCUMULATED_WEALTH,
 * validation_class:DecimalType, index_type: KEYS}, {column_name: GRADUATION_DAY, validation_class:DateType, index_type:
 * KEYS} ]; describe KunderaExamples;
 * 
 * @author chhavi.gangwal
 */
public class MongoQueryTest extends JerseyTest {
    private static final String _KEYSPACE = "KunderaExamples";

    private static final String _PU = "mongoPu";

    private static Logger log = LoggerFactory.getLogger(MongoQueryTest.class);

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

    public MongoQueryTest() throws Exception {
        super(Constants.KUNDERA_REST_RESOURCES_PACKAGE);
    }

    @Before
    public void setup() throws Exception {

    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {

        if (AUTO_MANAGE_SCHEMA) {
            CassandraCli.dropKeySpace(_KEYSPACE);
        }
    }

    @Test
    public void testCompositeUserCRUD() throws JsonParseException, JsonMappingException, IOException {
        WebResource webResource = resource();
        restClient = new RESTClientImpl();
        restClient.initialize(webResource, mediaType);

        // Get Application Token
        applicationToken = restClient.getApplicationToken(_PU, null);
        Assert.assertNotNull(applicationToken);
        applicationToken = applicationToken.replaceAll("^\"|\"$", "");
        Assert.assertTrue(applicationToken.startsWith("AT_"));

        // Get Session Token
        sessionToken = restClient.getSessionToken(applicationToken);
        Assert.assertNotNull(sessionToken);
        sessionToken = sessionToken.replaceAll("^\"|\"$", "");
        Assert.assertTrue(sessionToken.startsWith("ST_"));

        UUID timeLineId = UUID.randomUUID();
        Date currentDate = new Date();
        MongoCompoundKey key = new MongoCompoundKey("mevivs", 1, timeLineId);

        MongoPrimeUser timeLine = new MongoPrimeUser(key);
        timeLine.setKey(key);
        timeLine.setTweetBody("my first tweet");
        timeLine.setTweetDate(currentDate);

        MongoCompoundKey key1 = new MongoCompoundKey("john", 2, timeLineId);

        MongoPrimeUser timeLine2 = new MongoPrimeUser(key1);
        timeLine2.setKey(key1);
        timeLine2.setTweetBody("my second tweet");
        timeLine2.setTweetDate(currentDate);

        String mongoUser = JAXBUtils.toString(timeLine, MediaType.APPLICATION_JSON);
        Assert.assertNotNull(mongoUser);

        String mongoUser1 = JAXBUtils.toString(timeLine2, MediaType.APPLICATION_JSON);
        Assert.assertNotNull(mongoUser1);

        // Insert Record
        String insertResponse1 = restClient.insertEntity(sessionToken, mongoUser, "MongoPrimeUser");
        String insertResponse2 = restClient.insertEntity(sessionToken, mongoUser1, "MongoPrimeUser");

        Assert.assertNotNull(insertResponse1);
        Assert.assertNotNull(insertResponse2);

        Assert.assertTrue(insertResponse1.indexOf("200") > 0);
        Assert.assertTrue(insertResponse2.indexOf("200") > 0);
        String encodepk1 = null;
        pk1 = JAXBUtils.toString(key, MediaType.APPLICATION_JSON);
        pk2 = JAXBUtils.toString(key1, MediaType.APPLICATION_JSON);
        try {
            encodepk1 = java.net.URLEncoder.encode(pk1, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            log.error(e.getMessage());
        }
        // Find Record
        String foundUser = restClient.findEntity(sessionToken, encodepk1, "MongoPrimeUser");
        Assert.assertNotNull(foundUser);

        Assert.assertNotNull(foundUser);
        Assert.assertTrue(foundUser.indexOf("mevivs") > 0);

        foundUser = foundUser.substring(1, foundUser.length() - 1);
        Map<String, Object> userDetails = new HashMap<String, Object>();
        ObjectMapper mapper = new ObjectMapper();
        userDetails = mapper.readValue(foundUser, userDetails.getClass());
        foundUser = mapper.writeValueAsString(userDetails.get("mongoprimeuser"));

        // Update Record
        foundUser = foundUser.replaceAll("first", "hundreth");

        String updatedUser = restClient.updateEntity(sessionToken, foundUser, MongoPrimeUser.class.getSimpleName());

        Assert.assertNotNull(updatedUser);
        Assert.assertTrue(updatedUser.indexOf("hundreth") > 0);

        /** JPA Query - Select */
        // Get All users
        String jpaQuery = "select p from " + MongoPrimeUser.class.getSimpleName() + " p";
        String queryResult = restClient.runJPAQuery(sessionToken, jpaQuery, new HashMap<String, Object>());
        log.debug("Query Result:" + queryResult);

        Assert.assertNotNull(queryResult);
        Assert.assertFalse(StringUtils.isEmpty(queryResult));

        Assert.assertTrue(queryResult.indexOf("mongoprimeuser") > 0);
        Assert.assertTrue(queryResult.indexOf(pk1) > 0);
        Assert.assertTrue(queryResult.indexOf(pk2) > 0);

        Assert.assertTrue(queryResult.indexOf("Motif") < 0);

        jpaQuery = "select p from " + MongoPrimeUser.class.getSimpleName() + " p WHERE p.key = :key";
        Map<String, Object> params = new HashMap<String, Object>();

        params.put("key", pk1);

        queryResult = restClient.runObjectJPAQuery(sessionToken, jpaQuery, params);
        log.debug("Query Result:" + queryResult);

        Assert.assertNotNull(queryResult);
        Assert.assertFalse(StringUtils.isEmpty(queryResult));
        Assert.assertTrue(queryResult.indexOf("mongoprimeuser") > 0);
        Assert.assertTrue(queryResult.indexOf(timeLineId.toString()) > 0);
        Assert.assertTrue(queryResult.indexOf(pk2) < 0);

        Assert.assertTrue(queryResult.indexOf("Motif") < 0);

        jpaQuery = "select p from " + MongoPrimeUser.class.getSimpleName() + " p";
        params = new HashMap<String, Object>();
        params.put("firstResult", 0);
        params.put("maxResult", 1);

        queryResult = restClient.runObjectJPAQuery(sessionToken, jpaQuery, params);

        log.debug("Query Result:" + queryResult);

        String userList = queryResult.substring(1, queryResult.length() - 1);

        mapper = new ObjectMapper();
        userDetails = new HashMap<String, Object>();
        userDetails = mapper.readValue(userList, userDetails.getClass());
        userList = mapper.writeValueAsString(userDetails.get("mongoprimeuser"));

        List<MongoPrimeUser> navigation =
            mapper.readValue(userList, mapper.getTypeFactory()
                .constructCollectionType(List.class, MongoPrimeUser.class));

        Assert.assertNotNull(queryResult);
        Assert.assertFalse(StringUtils.isEmpty(queryResult));
        Assert.assertTrue(queryResult.indexOf("mongoprimeuser") > 0);
        Assert.assertTrue(queryResult.indexOf(timeLineId.toString()) > 0);
        Assert.assertEquals(1, navigation.size());

        /** JPA Query - Select All */
        // Get All Users
        String allUsers = restClient.getAllEntities(sessionToken, MongoPrimeUser.class.getSimpleName());
        log.debug(allUsers);
        Assert.assertNotNull(allUsers);

        // Close Session
        restClient.closeSession(sessionToken);

        // Close Application
        restClient.closeApplication(applicationToken);
    }

    @Test
    public void testMongoNativeQuery() throws JsonParseException, JsonMappingException, IOException {
        WebResource webResource = resource();
        restClient = new RESTClientImpl();
        restClient.initialize(webResource, mediaType);

        // Get Application Token
        applicationToken = restClient.getApplicationToken(_PU, null);
        Assert.assertNotNull(applicationToken);
        applicationToken = applicationToken.replaceAll("^\"|\"$", "");
        Assert.assertTrue(applicationToken.startsWith("AT_"));

        // Get Session Token
        sessionToken = restClient.getSessionToken(applicationToken);
        Assert.assertNotNull(sessionToken);
        sessionToken = sessionToken.replaceAll("^\"|\"$", "");
        Assert.assertTrue(sessionToken.startsWith("ST_"));

        String dbResp = restClient.runNativeScript(sessionToken, "db.adminCommand('listDatabases').databases", _PU);
        Assert.assertNotNull(dbResp);
        Assert.assertTrue(dbResp.indexOf("local") > 0);

        // Close Session
        restClient.closeSession(sessionToken);

        // Close Application
        restClient.closeApplication(applicationToken);
    }

}
