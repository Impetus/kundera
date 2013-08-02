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
import java.util.List;
import java.util.Map;

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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.rest.common.CassandraCli;
import com.impetus.kundera.rest.common.Constants;
import com.impetus.kundera.rest.common.JAXBUtils;
import com.impetus.kundera.rest.common.Professional;
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
public class DataTypeTest extends JerseyTest
{
    private static final String _KEYSPACE = "KunderaExamples";

    private static final String COLUMN_FAMILY_PROFESSIONAL = "PROFESSIONAL";

    private static final String PROFESSIONAL_CLASS_NAME = "Professional";

    private static Logger log = LoggerFactory.getLogger(DataTypeTest.class);

    static String mediaType = MediaType.APPLICATION_XML;

    static RESTClient restClient;

    String applicationToken = null;

    String sessionToken = null;

    String professionalStr1;

    String professionalStr2;

    String pk1;

    String pk2;

    Professional prof1;

    Professional prof2;

    private final static boolean USE_EMBEDDED_SERVER = true;

    private final static boolean AUTO_MANAGE_SCHEMA = true;

    public DataTypeTest() throws Exception
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
            buildProfessional1Str();
            buildProfessional2Str();

        }
        else if (MediaType.APPLICATION_JSON.equals(mediaType))
        {
            Assert.fail("Incorrect Media Type:" + mediaType);
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
        String insertResponse1 = restClient.insertEntity(sessionToken, professionalStr1, PROFESSIONAL_CLASS_NAME);
        String insertResponse2 = restClient.insertEntity(sessionToken, professionalStr2, PROFESSIONAL_CLASS_NAME);

        Assert.assertNotNull(insertResponse1);
        Assert.assertNotNull(insertResponse2);
        Assert.assertTrue(insertResponse1.indexOf("201") > 0);
        Assert.assertTrue(insertResponse2.indexOf("201") > 0);

        // Find Record
        String foundProfessional = restClient.findEntity(sessionToken, pk1, PROFESSIONAL_CLASS_NAME);
        Assert.assertNotNull(foundProfessional);
        Assert.assertTrue(foundProfessional.indexOf(pk1) > 0);

        // Update Record
        foundProfessional = foundProfessional.replaceAll("163.12", "165.21");
        String updatedProfessional = restClient.updateEntity(sessionToken, foundProfessional, PROFESSIONAL_CLASS_NAME);
        Assert.assertNotNull(updatedProfessional);
        Assert.assertTrue(updatedProfessional.indexOf("165.21") > 0);

        /** JPA Query - Select */
        // Get All Professionals
        String jpaQuery = "select p from " + PROFESSIONAL_CLASS_NAME + " p";
        String queryResult = restClient.runJPAQuery(sessionToken, jpaQuery, new HashMap<String, Object>());
        log.debug("Query Result:" + queryResult);
        assertAllProfessionalsString(queryResult);

        /** JPA Query - Select All */
        // Get All Professionals
        String allProfessionals = restClient.getAllEntities(sessionToken, PROFESSIONAL_CLASS_NAME);
        log.debug(allProfessionals);
        assertAllProfessionalsString(allProfessionals);

        /** Named JPA Query - Select */
        // Get Professionals for a specific department
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("departmentId", 23456789);
        String profByDepartment = restClient.runNamedJPAQuery(sessionToken, PROFESSIONAL_CLASS_NAME,
                "findByDepartment", params);
        Assert.assertTrue(profByDepartment.indexOf(pk1) > 0);
        log.debug(profByDepartment);

        // Get Professionals for a specific Enrolment date
        Map<String, Object> params2 = new HashMap<String, Object>();
        params2.put("1", prof1.getEnrolmentDate().getTime());
        String profByEnrolmentDate = restClient.runNamedJPAQuery(sessionToken, PROFESSIONAL_CLASS_NAME,
                "findByEnrolmentDate", params2);
        Assert.assertTrue(profByEnrolmentDate.indexOf(pk1) > 0);
        log.debug(profByEnrolmentDate);

        // Delete Records
        restClient.deleteEntity(sessionToken, updatedProfessional, pk1, PROFESSIONAL_CLASS_NAME);
        restClient.deleteEntity(sessionToken, updatedProfessional, pk2, PROFESSIONAL_CLASS_NAME);

        // Close Session
        restClient.closeSession(sessionToken);

        // Close Application
        restClient.closeApplication(applicationToken);
    }

    private void assertAllProfessionalsString(String queryResult)
    {
        Assert.assertFalse(StringUtils.isEmpty(queryResult));
        Assert.assertTrue(queryResult.indexOf("professionals") > 0);
        Assert.assertTrue(queryResult.indexOf(pk1) > 0);
        Assert.assertTrue(queryResult.indexOf(pk2) > 0);
    }

    private void buildProfessional1Str()
    {
        pk1 = "1111111111111";

        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date(1351667547777l));
        prof1 = new Professional(pk1, 23456789, true, 31, 'C', (byte) 8, (short) 5, (float) 10.0, 163.12, new Date(
                Long.parseLong("1351667541111")), new Date(Long.parseLong("1351667542222")), new Date(
                Long.parseLong("1351667543333")), 2, new Long(3634521523423L), new Double(7.23452342343),
        /*
         * new java.sql.Date(new
         * Date(Long.parseLong("1344079061111")).getTime()), new
         * java.sql.Time(new Date(Long.parseLong("1344079062222")).getTime()),
         * new java.sql.Timestamp(new
         * Date(Long.parseLong("13440790653333")).getTime()),
         */
        new BigInteger("123456789"), new BigDecimal(123456789), cal);

        professionalStr1 = JAXBUtils.toString(Professional.class, prof1, MediaType.APPLICATION_XML);

    }

    private void buildProfessional2Str()
    {
        pk2 = "2222222222222";

        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date(1351667557777l));
        prof2 = new Professional(pk2, 23456790, true, 33, 'A', (byte) 9, (short) 3, (float) 9.0, 167.75, new Date(
                Long.parseLong("1351667551111")), new Date(Long.parseLong("1351667552222")), new Date(
                Long.parseLong("1351667553333")), 2, new Long(3634521523423L), new Double(7.23452342343),
        /*
         * new java.sql.Date(new
         * Date(Long.parseLong("1344079061111")).getTime()), new
         * java.sql.Time(new Date(Long.parseLong("1344079062222")).getTime()),
         * new java.sql.Timestamp(new
         * Date(Long.parseLong("13440790653333")).getTime()),
         */
        new BigInteger("123456790"), new BigDecimal(123456790), cal);

        professionalStr2 = JAXBUtils.toString(Professional.class, prof2, MediaType.APPLICATION_XML);

    }

    private static void loadData() throws TException, InvalidRequestException, UnavailableException, TimedOutException,
            SchemaDisagreementException
    {
        KsDef ksDef = null;
        CfDef professionalCfDef = new CfDef();
        professionalCfDef.name = COLUMN_FAMILY_PROFESSIONAL;
        professionalCfDef.keyspace = _KEYSPACE;
        professionalCfDef.setComparator_type("UTF8Type");
        professionalCfDef.setKey_validation_class("UTF8Type");

        ColumnDef departmentDef = new ColumnDef(ByteBuffer.wrap("DEPARTMENT_ID".getBytes()), "LongType");
        departmentDef.index_type = IndexType.KEYS;
        ColumnDef exceptionalDef = new ColumnDef(ByteBuffer.wrap("IS_EXCEPTIONAL".getBytes()), "BooleanType");
        exceptionalDef.index_type = IndexType.KEYS;
        ColumnDef enrolmentDateDef = new ColumnDef(ByteBuffer.wrap("ENROLMENT_DATE".getBytes()), "DateType");
        enrolmentDateDef.index_type = IndexType.KEYS;

        professionalCfDef.addToColumn_metadata(departmentDef);
        professionalCfDef.addToColumn_metadata(exceptionalDef);
        professionalCfDef.addToColumn_metadata(enrolmentDateDef);

        List<CfDef> cfDefs = new ArrayList<CfDef>();
        cfDefs.add(professionalCfDef);

        try
        {
            ksDef = CassandraCli.client.describe_keyspace(_KEYSPACE);
            CassandraCli.client.set_keyspace(_KEYSPACE);

            List<CfDef> cfDefn = ksDef.getCf_defs();

            for (CfDef cfDef1 : cfDefn)
            {
                if (cfDef1.getName().equalsIgnoreCase(COLUMN_FAMILY_PROFESSIONAL))
                {
                    CassandraCli.client.system_drop_column_family(COLUMN_FAMILY_PROFESSIONAL);
                }
            }
            CassandraCli.client.system_add_column_family(professionalCfDef);
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
