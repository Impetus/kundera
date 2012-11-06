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
package com.impetus.client.crud.compositeType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import junit.framework.Assert;

import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.cassandra.CassandraClientBase;
import com.impetus.client.persistence.CassandraCli;
import com.impetus.kundera.client.Client;

/**
 * Junit test case for Compound/Composite key.
 * 
 * @author vivek.mishra
 * 
 */
public class CompositeDataTypeTest
{

    /**
     * 
     */
    private static final String PERSISTENCE_UNIT = "compositedatatype";

    private EntityManagerFactory emf;

    /** The enrolment date. */
    protected Date enrolmentDate = new Date(Long.parseLong("1344079065781"));

    /** The joining date and time. */
    protected Date joiningDateAndTime = new Date();

    /** The date. */
    protected long date = new Date().getTime();

    /** The new sql date. */
    protected java.sql.Date newSqlDate = new java.sql.Date(date);

    /** The enrolment time. */
    protected Date enrolmentTime = new Date();

    /** The sql time. */
    protected java.sql.Time sqlTime = new java.sql.Time(date);

    /** The sql timestamp. */
    protected java.sql.Timestamp sqlTimestamp = new java.sql.Timestamp(date);

    /** The big decimal. */
    protected BigDecimal bigDecimal = new BigDecimal(123456789);

    /** The big integer. */
    protected BigInteger bigInteger = new BigInteger("123456789");

    /** The number of students. */
    protected int numberOfStudents = 1000;

    /** The calendar. */
    protected Calendar calendar = Calendar.getInstance();
    /** The Constant logger. */
    private static final Log logger = LogFactory.getLog(CompositeDataTypeTest.class);

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        CassandraCli.cassandraSetUp();
        CassandraCli.initClient();
//        loadData();
        emf = Persistence.createEntityManagerFactory(PERSISTENCE_UNIT);
    }

    /**
     * CRUD over Compound primary Key.
     * @throws IllegalAccessException 
     * @throws InstantiationException 
     */
    @Test
    public void onCRUD() throws InstantiationException, IllegalAccessException
    {
        EntityManager em = emf.createEntityManager();

        UUID timeLineId = UUID.randomUUID();
        Date currentDate = new Date();
        CompoundKeyDataType key = prepareData(new Long(12345677), 78575785897L, "Amresh", false, 10, 'A', (byte) 5, (short) 8,
                (float) 69.3, 163.76765654, enrolmentDate, enrolmentTime, joiningDateAndTime, new Integer(3), new Long(
                        978423946455l), 135434.89, newSqlDate, sqlTime, sqlTimestamp, bigDecimal, bigInteger, calendar,
                CompoundKeyDataType.class.newInstance());
        
        Map<String,Client> clients = (Map<String, Client>) em.getDelegate();
        Client client = clients.get(PERSISTENCE_UNIT);
        ((CassandraClientBase)client).setCqlVersion("3.0.0");
        PrimeUserDataType user = new PrimeUserDataType(key);
        user.setTweetBody("my first tweet");
        user.setTweetDate(currentDate);
        em.persist(user);

        em.clear(); // optional,just to clear persistence cache.

        PrimeUserDataType result = em.find(PrimeUserDataType.class, key);
        Assert.assertNotNull(result);
        Assert.assertEquals("my first tweet", result.getTweetBody());
//        Assert.assertEquals(timeLineId, result.getKey().getTimeLineId());
        Assert.assertEquals(currentDate.getTime(), result.getTweetDate().getTime());

        em.clear();// optional,just to clear persistence cache.

        user.setTweetBody("After merge");
        em.merge(user);

        em.clear();// optional,just to clear persistence cache.

        result = em.find(PrimeUserDataType.class, key);
        Assert.assertNotNull(result);
        Assert.assertEquals("After merge", result.getTweetBody());
//        Assert.assertEquals(timeLineId, result.getKey().getTimeLineId());
        Assert.assertEquals(currentDate.getTime(), result.getTweetDate().getTime());

         // deleting composite
        em.remove(result);

        em.clear();// optional,just to clear persistence cache.

        result = em.find(PrimeUserDataType.class, key);
        Assert.assertNull(result); 
   }

    @Test
    public void onQuery() throws InstantiationException, IllegalAccessException
    {
        EntityManager em = emf.createEntityManager();

        UUID timeLineId = UUID.randomUUID();
        Date currentDate = new Date();
        CompoundKeyDataType key = prepareData(new Long(12345677), 78575785897L, "Amresh", false, 10, 'A', (byte) 5, (short) 8,
                (float) 69.3, 163.76765654, enrolmentDate, enrolmentTime, joiningDateAndTime, new Integer(3), new Long(
                        978423946455l), 135434.89, newSqlDate, sqlTime, sqlTimestamp, bigDecimal, bigInteger, calendar,
                CompoundKeyDataType.class.newInstance());

        Map<String,Client> clients = (Map<String, Client>) em.getDelegate();
        Client client = clients.get(PERSISTENCE_UNIT);
        ((CassandraClientBase)client).setCqlVersion("3.0.0");

        PrimeUserDataType user = new PrimeUserDataType(key);
        user.setTweetBody("my first tweet");
        user.setTweetDate(new Date());
        em.persist(user);

        em.clear(); // optional,just to clear persistence cache.
        final String noClause = "Select u from PrimeUserDataType u";

        final String withFirstCompositeColClause = "Select u from PrimeUserDataType u where u.key.studentId = :studentId";

        // secondary index support over compound key is not enabled in cassandra composite keys yet. DO NOT DELETE/UNCOMMENT.
        
/*//        final String withClauseOnNoncomposite = "Select u from PrimeUserDataType u where u.tweetDate = ?1";
//
        
        final String withSecondCompositeColClause = "Select u from PrimeUserDataType u where u.key.studentId = :studentId";
        final String withBothCompositeColClause = "Select u from PrimeUserDataType u where u.key.userId = :userId and u.key.tweetId = :tweetId";
        final String withAllCompositeColClause = "Select u from PrimeUserDataType u where u.key.userId = :userId and u.key.tweetId = :tweetId and u.key.timeLineId = :timeLineId";
        final String withLastCompositeColGTClause = "Select u from PrimeUserDataType u where u.key.userId = :userId and u.key.tweetId = :tweetId and u.key.timeLineId >= :timeLineId";

        final String withSelectiveCompositeColClause = "Select u.key from PrimeUserDataType u where u.key.userId = :userId and u.key.tweetId = :tweetId and u.key.timeLineId = :timeLineId";
*/
        // query over 1 composite and 1 non-column

        // query with no clause.
        Query q = em.createQuery(noClause);
        List<PrimeUserDataType> results = q.getResultList();
        Assert.assertEquals(1, results.size());

        // Query with composite key clause.
        q = em.createQuery(withFirstCompositeColClause);
        q.setParameter("studentId", new Long(12345677));
        results = q.getResultList();
        Assert.assertEquals(1, results.size());
/*
        // secondary index support over compound key is not enabled in cassandra composite keys yet. DO NOT DELETE/UNCOMMENT.

        // Query with composite key clause.
        q = em.createQuery(withClauseOnNoncomposite);
        q.setParameter(1, currentDate);
        results = q.getResultList();
        Assert.assertEquals(1, results.size());

        // Query with composite key clause.
        q = em.createQuery(withSecondCompositeColClause);
        q.setParameter("tweetId", 1);
        results = q.getResultList();
        Assert.assertEquals(1, results.size());

        // Query with composite key clause.
        q = em.createQuery(withBothCompositeColClause);
        q.setParameter("userId", "mevivs");
        q.setParameter("tweetId", 1);
        results = q.getResultList();
        Assert.assertEquals(1, results.size());

                // Query with composite key clause.
        q = em.createQuery(withAllCompositeColClause);
        q.setParameter("userId", "mevivs");
        q.setParameter("tweetId", 1);
        q.setParameter("timeLineId", timeLineId);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());

        // Query with composite key clause.
        q = em.createQuery(withLastCompositeColGTClause);
        q.setParameter("userId", "mevivs");
        q.setParameter("tweetId", 1);
        q.setParameter("timeLineId", timeLineId);
        results = q.getResultList();

        Assert.assertEquals(1, results.size());

         
        // Query with composite key with selective clause.
        q = em.createQuery(withSelectiveCompositeColClause);
        q.setParameter("userId", "mevivs");
        q.setParameter("tweetId", 1);
        q.setParameter("timeLineId", timeLineId);
        results = q.getResultList();
        Assert.assertEquals(1, results.size());
        Assert.assertNull(results.get(0).getTweetBody());

        final String selectiveColumnTweetBodyWithAllCompositeColClause = "Select u.tweetBody from PrimeUserDataType u where u.key.userId = :userId and u.key.tweetId = :tweetId and u.key.timeLineId = :timeLineId";
        // Query for selective column tweetBody with composite key clause.
        q = em.createQuery(selectiveColumnTweetBodyWithAllCompositeColClause);
        q.setParameter("userId", "mevivs");
        q.setParameter("tweetId", 1);
        q.setParameter("timeLineId", timeLineId);
        results = q.getResultList();
        Assert.assertEquals(1, results.size());
        Assert.assertEquals("my first tweet", results.get(0).getTweetBody());
        Assert.assertNull(results.get(0).getTweetDate());

        final String selectiveColumnTweetDateWithAllCompositeColClause = "Select u.tweetDate from PrimeUserDataType u where u.key.userId = :userId and u.key.tweetId = :tweetId and u.key.timeLineId = :timeLineId";
        // Query for selective column tweetDate with composite key clause.
        q = em.createQuery(selectiveColumnTweetDateWithAllCompositeColClause);
        q.setParameter("userId", "mevivs");
        q.setParameter("tweetId", 1);
        q.setParameter("timeLineId", timeLineId);
        results = q.getResultList();
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(currentDate.getTime(), results.get(0).getTweetDate().getTime());
        Assert.assertNull(results.get(0).getTweetBody());

        final String withCompositeKeyClause = "Select u from PrimeUserDataType u where u.key = :key";
        // Query with composite key clause.
        q = em.createQuery(withCompositeKeyClause);
        q.setParameter("key", key);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
*/
        em.remove(user);

        em.clear();// optional,just to clear persistence cache.
    }

    @Test
    public void onNamedQueryTest() throws InstantiationException, IllegalAccessException
    {
        updateNamed();
        deleteNamed();

    }

    /**
     * Update by Named Query.
     * 
     * @return
     * @throws IllegalAccessException 
     * @throws InstantiationException 
     */
    private void updateNamed() throws InstantiationException, IllegalAccessException
    {
        EntityManager em = emf.createEntityManager();

        Map<String,Client> clients = (Map<String, Client>) em.getDelegate();
        Client client = clients.get(PERSISTENCE_UNIT);
        ((CassandraClientBase)client).setCqlVersion("3.0.0");

        UUID timeLineId = UUID.randomUUID();
        Date currentDate = new Date();
        CompoundKeyDataType key = prepareData(new Long(12345677), 78575785897L, "Amresh", false, 10, 'A', (byte) 5, (short) 8,
                (float) 69.3, 163.76765654, enrolmentDate, enrolmentTime, joiningDateAndTime, new Integer(3), new Long(
                        978423946455l), 135434.89, newSqlDate, sqlTime, sqlTimestamp, bigDecimal, bigInteger, calendar,
                CompoundKeyDataType.class.newInstance());
        PrimeUserDataType user = new PrimeUserDataType(key);
        user.setTweetBody("my first tweet");
        user.setTweetDate(currentDate);
        em.persist(user);

        em = emf.createEntityManager();
        clients = (Map<String, Client>) em.getDelegate();
        client = clients.get(PERSISTENCE_UNIT);
        ((CassandraClientBase)client).setCqlVersion("3.0.0");

        String updateQuery = "Update PrimeUserDataType u SET u.tweetBody=after merge where u.key= :beforeUpdate";
        Query q = em.createQuery(updateQuery);
        q.setParameter("beforeUpdate", key);
        q.executeUpdate();

        PrimeUserDataType result = em.find(PrimeUserDataType.class, key);
        Assert.assertNotNull(result);
        Assert.assertEquals("after merge", result.getTweetBody());
//        Assert.assertEquals(timeLineId, result.getKey().getTimeLineId());
        Assert.assertEquals(currentDate.getTime(), result.getTweetDate().getTime());
        em.close();
    }

    /**
     * delete by Named Query.
     * @throws IllegalAccessException 
     * @throws InstantiationException 
     */
    private void deleteNamed() throws InstantiationException, IllegalAccessException
    {
        UUID timeLineId = UUID.randomUUID();
        Date currentDate = new Date();
        CompoundKeyDataType key = prepareData(new Long(12345677), 78575785897L, "Amresh", false, 10, 'A', (byte) 5, (short) 8,
                (float) 69.3, 163.76765654, enrolmentDate, enrolmentTime, joiningDateAndTime, new Integer(3), new Long(
                        978423946455l), 135434.89, newSqlDate, sqlTime, sqlTimestamp, bigDecimal, bigInteger, calendar,
                CompoundKeyDataType.class.newInstance());

        String deleteQuery = "Delete From PrimeUserDataType u where u.key= :key";
        EntityManager em = emf.createEntityManager();
        Map<String,Client> clients = (Map<String, Client>) em.getDelegate();
        Client client = clients.get(PERSISTENCE_UNIT);
        ((CassandraClientBase)client).setCqlVersion("3.0.0");

        Query q = em.createQuery(deleteQuery);
        q.setParameter("key", key);
        q.executeUpdate();

        PrimeUserDataType result = em.find(PrimeUserDataType.class, key);
        Assert.assertNull(result);
        em.close();
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        CassandraCli.dropKeySpace("CompositeCassandra");
        emf.close();
    }
// DO NOT DELETE IT!! though it is automated with schema creation option.
    /**
     *  create column family script for compound key.
     */
    private void loadData()
    {
        CassandraCli.createKeySpace("CompositeCassandra");
        String cql_Query = "create columnfamily \"CompositeUser\" (\"userId\" text, \"tweetId\" int, \"timeLineId\" uuid, \"tweetBody\" text," +
        		    " \"tweetDate\" timestamp, PRIMARY KEY(\"userId\",\"tweetId\",\"timeLineId\"))";
        try
        {
            CassandraCli.getClient().set_keyspace("CompositeCassandra");
        }
        catch (InvalidRequestException e)
        {
            logger.error(e.getMessage());
        }
        catch (TException e)
        {
            logger.error(e.getMessage());
        }
        CassandraCli.executeCqlQuery(cql_Query);
        
        
    }

    /**
     * Prepare data.
     * 
     * @param studentId
     *            the student id
     * @param uniqueId
     *            the unique id
     * @param studentName
     *            the student name
     * @param isExceptional
     *            the is exceptional
     * @param age
     *            the age
     * @param semester
     *            the semester
     * @param digitalSignature
     *            the digital signature
     * @param cgpa
     *            the cgpa
     * @param percentage
     *            the percentage
     * @param height
     *            the height
     * @param enrolmentDate
     *            the enrolment date
     * @param enrolmentTime
     *            the enrolment time
     * @param joiningDateAndTime
     *            the joining date and time
     * @param yearsSpent
     *            the years spent
     * @param rollNumber
     *            the roll number
     * @param monthlyFee
     *            the monthly fee
     * @param newSqlDate
     *            the new sql date
     * @param sqlTime
     *            the sql time
     * @param sqlTimestamp
     *            the sql timestamp
     * @param bigDecimal
     *            the big decimal
     * @param bigInteger
     *            the big integer
     * @param calendar
     *            the calendar
     * @param o
     *            the o
     * @return the person
     */
    private CompoundKeyDataType prepareData(long studentId, long uniqueId, String studentName, boolean isExceptional, int age,
            char semester, byte digitalSignature, short cgpa, float percentage, double height,
            java.util.Date enrolmentDate, java.util.Date enrolmentTime, java.util.Date joiningDateAndTime,
            Integer yearsSpent, Long rollNumber, Double monthlyFee, java.sql.Date newSqlDate, java.sql.Time sqlTime,
            java.sql.Timestamp sqlTimestamp, BigDecimal bigDecimal, BigInteger bigInteger, Calendar calendar, CompoundKeyDataType o)
    {
        o.setStudentId((Long) studentId);
        o.setUniqueId(uniqueId);
        o.setStudentName(studentName);
        o.setExceptional(isExceptional);
        o.setAge(age);
        o.setSemester(semester);
        o.setDigitalSignature(digitalSignature);
        o.setCgpa(cgpa);
        o.setPercentage(percentage);
        o.setHeight(height);

        o.setEnrolmentDate(enrolmentDate);
        o.setEnrolmentTime(enrolmentTime);
        o.setJoiningDateAndTime(joiningDateAndTime);

        o.setYearsSpent(yearsSpent);
        o.setRollNumber(rollNumber);
        o.setMonthlyFee(monthlyFee);
        o.setSqlDate(newSqlDate);
        o.setSqlTime(sqlTime);
        o.setSqlTimestamp(sqlTimestamp);
//        o.setBigDecimal(bigDecimal);
        o.setBigInteger(bigInteger);
        o.setCalendar(calendar);
        return  o;
    }
}
