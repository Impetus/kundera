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
package com.impetus.client.hbase.crud.datatypes;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;

import javax.persistence.Query;

import junit.framework.Assert;

import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.hbase.junits.HBaseCli;

/**
 * The Class Student test case for HBase.
 * 
 * @author Kuldeep.mishra
 */
public class StudentHBaseTest extends StudentBase<StudentHBase>
{
    private String persistenceUnit = "hbaseTest";

    private HBaseCli cli;

    /**
     * Sets the up.
     * 
     * @throws Exception
     *             the exception
     */
    @Before
    public void setUp() throws Exception
    {
        setupInternal(persistenceUnit);
    }

    /**
     * Tear down.
     * 
     * @throws Exception
     *             the exception
     */
    @After
    public void tearDown() throws Exception
    {
        teardownInternal(persistenceUnit);

    }

    @Test
    public void executeTests()
    {
        onInsert();
        onMerge();
    }

    /**
     * Test method for.
     * 
     * @throws InstantiationException
     *             the instantiation exception
     * @throws IllegalAccessException
     *             the illegal access exception
     *             {@link com.impetus.kundera.examples.student.StudentDao#saveStudent(com.impetus.kundera.StudentHBase.crud.datatype.entities.StudentHbase)}
     *             .
     */

    public void onInsert()
    {
        try
        {
            onInsert(new StudentHBase());
        }
        catch (InstantiationException e)
        {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
        catch (IllegalAccessException e)
        {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }

        // find by id.
        StudentEntityDef s = em.find(StudentHBase.class, studentId1);
        assertOnDataTypes((StudentHBase) s);

        // // find by name.
        assertFindByName(em, "StudentHBase", StudentHBase.class, "Amresh", "studentName");

        // find by name and age.
        assertFindByNameAndAge(em, "StudentHBase", StudentHBase.class, "Amresh", "10", "studentName");

        // find by name, age clause
        assertFindByNameAndAgeGTAndLT(em, "StudentHBase", StudentHBase.class, "Amresh", "10", "20", "studentName");
        //
        // // find by between clause
        assertFindByNameAndAgeBetween(em, "StudentHBase", StudentHBase.class, "Amresh", "10", "15", "studentName");

        // find by Range.
        assertFindByRange(em, "StudentHBase", StudentHBase.class, "12345677", "12345679", "studentId");

        // find by without where clause.
        assertFindWithoutWhereClause(em, "StudentHBase", StudentHBase.class);

        // Query on Date.
        String query = "Select s from StudentHBase s where s.enrolmentDate =:enrolmentDate";
        Query q = em.createQuery(query);
        q.setParameter("enrolmentDate", enrolmentDate);
        List<StudentHBase> results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());

        // Query on long.
        /* String */query = "Select s from StudentHBase s where s.uniqueId =?1";
        /* Query */q = em.createQuery(query);
        q.setParameter(1, 78575785897L);

        /* List<StudentHBase> */results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(78575785897L, results.get(0).getUniqueId());

        // Assert on boolean.
        query = "Select s from StudentHBase s where s.isExceptional =?1";
        q = em.createQuery(query);
        q.setParameter(1, true);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());
        Assert.assertEquals(true, results.get(0).isExceptional());

        // with false.
        query = "Select s from StudentHBase s where s.isExceptional =?1";
        q = em.createQuery(query);
        q.setParameter(1, false);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(false, results.get(0).isExceptional());

        // query on int.

        query = "Select s from StudentHBase s where s.age =?1";
        q = em.createQuery(query);
        q.setParameter(1, 10);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(false, results.get(0).isExceptional());
        Assert.assertEquals(10, results.get(0).getAge());

        // query on char (semester)

        query = "Select s from StudentHBase s where s.semester =?1";
        q = em.createQuery(query);
        q.setParameter(1, 'A');
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(false, results.get(0).isExceptional());
        Assert.assertEquals(10, results.get(0).getAge());
        Assert.assertEquals('A', results.get(0).getSemester());

        // query on float (percentage)
        query = "Select s from StudentHBase s where s.percentage =?1";
        q = em.createQuery(query);
        q.setParameter(1, 61.6);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(false, results.get(0).isExceptional());
        Assert.assertEquals(61.6f, results.get(0).getPercentage());

        // query on double (height)

        query = "Select s from StudentHBase s where s.height =?1";
        q = em.createQuery(query);
        q.setParameter(1, 163.76765654);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(false, results.get(0).isExceptional());
        Assert.assertEquals(163.76765654, results.get(0).getHeight());

        // query on cgpa.
        query = "Select s from StudentHBase s where s.cgpa =?1";
        q = em.createQuery(query);
        q.setParameter(1, (short) 8);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());
        Assert.assertEquals(false, results.get(0).isExceptional());
        Assert.assertEquals(8, results.get(0).getCgpa());

        // query on yearsSpent.
        Integer i = new Integer(3);
        query = "Select s from StudentHBase s where s.yearsSpent = 3";
        q = em.createQuery(query);
        // q.setParameter(1, new Integer(3));
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());
        Assert.assertEquals(false, results.get(0).isExceptional());
        Assert.assertEquals(i, results.get(0).getYearsSpent());

        // query on yearsSpent.
        query = "Select s from StudentHBase s where s.yearsSpent =?1";
        q = em.createQuery(query);
        q.setParameter(1, new Integer(3));
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());
        Assert.assertEquals(false, results.get(0).isExceptional());
        Assert.assertEquals(new Integer(3), results.get(0).getYearsSpent());

        // query on digitalSignature.
        query = "Select s from StudentHBase s where s.digitalSignature =?1";
        q = em.createQuery(query);
        q.setParameter(1, (byte) 50);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());
        Assert.assertEquals(true, results.get(0).isExceptional());
        Assert.assertEquals((byte) 50, results.get(0).getDigitalSignature());

        // query on cpga and digitalSignature.
        query = "Select s from StudentHBase s where s.cgpa =?1 and s.digitalSignature >= ?2 and s.digitalSignature <= ?3";
        q = em.createQuery(query);
        q.setParameter(1, (short) 8);
        q.setParameter(2, (byte) 5);
        q.setParameter(3, (byte) 50);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());
        Assert.assertEquals(false, results.get(0).isExceptional());
        Assert.assertEquals(true, results.get(1).isExceptional());
        Assert.assertEquals((short) 8, results.get(0).getCgpa());
        Assert.assertEquals((byte) 5, results.get(0).getDigitalSignature());
        Assert.assertEquals((byte) 50, results.get(1).getDigitalSignature());

        // query on cpga and digitalSignature parameter appended with String
        // .
        query = "Select s from StudentHBase s where s.cgpa = 8 and s.digitalSignature >= 5 and s.digitalSignature <= 50";
        q = em.createQuery(query);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());
        Assert.assertEquals(false, results.get(0).isExceptional());
        Assert.assertEquals(true, results.get(1).isExceptional());
        Assert.assertEquals((short) 8, results.get(0).getCgpa());
        Assert.assertEquals((byte) 5, results.get(0).getDigitalSignature());
        Assert.assertEquals((byte) 50, results.get(1).getDigitalSignature());

        // query on cpga and digitalSignature.
        query = "Select s from StudentHBase s where s.digitalSignature >= ?2 and s.digitalSignature <= ?3 and s.cgpa =?1";
        q = em.createQuery(query);
        q.setParameter(1, (short) 8);
        q.setParameter(2, (byte) 5);
        q.setParameter(3, (byte) 50);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());
        Assert.assertEquals(false, results.get(0).isExceptional());
        Assert.assertEquals(true, results.get(1).isExceptional());
        Assert.assertEquals((short) 8, results.get(0).getCgpa());
        Assert.assertEquals((byte) 5, results.get(0).getDigitalSignature());
        Assert.assertEquals((byte) 50, results.get(1).getDigitalSignature());

        // query on percentage and height.
        query = "Select s from StudentHBase s where s.percentage >= ?2 and s.percentage <= ?3 and s.height =?1";
        q = em.createQuery(query);
        q.setParameter(1, 163.76765654);
        q.setParameter(2, 61.6);
        q.setParameter(3, 69.3);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(false, results.get(0).isExceptional());
        Assert.assertEquals((short) 8, results.get(0).getCgpa());
        Assert.assertEquals((byte) 5, results.get(0).getDigitalSignature());
        Assert.assertEquals(61.6f, results.get(0).getPercentage());
        Assert.assertEquals(163.76765654, results.get(0).getHeight());

        // query on percentage and height parameter appended in string.
        query = "Select s from StudentHBase s where s.percentage >= 61.6 and s.percentage <= 69.3 and s.height = 163.76765654";
        q = em.createQuery(query);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(false, results.get(0).isExceptional());
        Assert.assertEquals((short) 8, results.get(0).getCgpa());
        Assert.assertEquals((byte) 5, results.get(0).getDigitalSignature());
        Assert.assertEquals(61.6f, results.get(0).getPercentage());
        Assert.assertEquals(163.76765654, results.get(0).getHeight());

        // query on cpga and uniqueId.
        query = "Select s from StudentHBase s where s.cgpa =?1 and s.uniqueId >= ?2 and s.uniqueId <= ?3";
        q = em.createQuery(query);
        q.setParameter(1, (short) 8);
        q.setParameter(2, 78575785897L);
        q.setParameter(3, 78575785899L);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());
        Assert.assertEquals(false, results.get(0).isExceptional());
        Assert.assertEquals(true, results.get(1).isExceptional());
        Assert.assertEquals((short) 8, results.get(0).getCgpa());
        Assert.assertEquals((byte) 5, results.get(0).getDigitalSignature());
        Assert.assertEquals((byte) 50, results.get(1).getDigitalSignature());
        Assert.assertEquals(78575785897L, results.get(0).getUniqueId());
        Assert.assertEquals(78575785898L, results.get(1).getUniqueId());

        // query on cpga and semester.
        query = "Select s from StudentHBase s where s.cgpa =?1 and s.semester >= ?2 and s.semester < ?3";
        q = em.createQuery(query);
        q.setParameter(1, (short) 8);
        q.setParameter(2, 'A');
        q.setParameter(3, 'C');
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());
        Assert.assertEquals(false, results.get(0).isExceptional());
        Assert.assertEquals(true, results.get(1).isExceptional());
        Assert.assertEquals((short) 8, results.get(0).getCgpa());
        Assert.assertEquals((byte) 5, results.get(0).getDigitalSignature());
        Assert.assertEquals((byte) 50, results.get(1).getDigitalSignature());
        Assert.assertEquals(78575785897L, results.get(0).getUniqueId());
        Assert.assertEquals(78575785898L, results.get(1).getUniqueId());
        Assert.assertEquals(10, results.get(0).getAge());
        Assert.assertEquals(20, results.get(1).getAge());

        // query on cpga and semester with appending in string.
        query = "Select s from StudentHBase s where s.cgpa = 8 and s.semester >= A and s.semester < C";
        q = em.createQuery(query);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.size());
        Assert.assertEquals(false, results.get(0).isExceptional());
        Assert.assertEquals(true, results.get(1).isExceptional());
        Assert.assertEquals((short) 8, results.get(0).getCgpa());
        Assert.assertEquals((byte) 5, results.get(0).getDigitalSignature());
        Assert.assertEquals((byte) 50, results.get(1).getDigitalSignature());
        Assert.assertEquals(78575785897L, results.get(0).getUniqueId());
        Assert.assertEquals(78575785898L, results.get(1).getUniqueId());
        Assert.assertEquals(10, results.get(0).getAge());
        Assert.assertEquals(20, results.get(1).getAge());

        // query on invalid cpga and uniqueId.
        query = "Select s from StudentHBase s where s.cgpa =?1 and s.uniqueId >= ?2 and s.uniqueId <= ?3";
        q = em.createQuery(query);
        q.setParameter(1, (short) 2);
        q.setParameter(2, 78575785897L);
        q.setParameter(3, 78575785899L);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertTrue(results.isEmpty());

        // query on big integer.
        query = "Select s from StudentHBase s where s.bigInteger =?1";
        q = em.createQuery(query);
        q.setParameter(1, bigInteger);
        results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(3, results.size());
        Assert.assertEquals(false, results.get(0).isExceptional());
        Assert.assertEquals(163.76765654, results.get(0).getHeight());
        Assert.assertEquals(true, results.get(1).isExceptional());
        Assert.assertEquals(163.76765655, results.get(1).getHeight());
        Assert.assertEquals(true, results.get(2).isExceptional());
        Assert.assertEquals(163.76765656, results.get(2).getHeight());
        Assert.assertEquals(10, results.get(0).getAge());
        Assert.assertEquals(20, results.get(1).getAge());
        Assert.assertEquals(15, results.get(2).getAge());
        Assert.assertEquals('A', results.get(0).getSemester());
        Assert.assertEquals('B', results.get(1).getSemester());
        Assert.assertEquals('C', results.get(2).getSemester());

        // invalid.
        q.setParameter(1, new BigInteger("1234567823"));
        results = q.getResultList();

        Assert.assertNotNull(results);
        Assert.assertTrue(results.isEmpty());

        updateQueryTest();

    }

    private void updateQueryTest()
    {/*
      * Query q = em.createQuery(
      * "update StudentHBase s set s.studentName = :oldName where s.studentName = :newName"
      * ); q.setParameter("newName", "NewAmresh"); q.setParameter("oldName",
      * "Amresh"); int results = q.executeUpdate();
      * Assert.assertNotNull(results); Assert.assertEquals(1, results);
      */
    }

    /**
     * On merge.
     */
    public void onMerge()
    {
        em.persist(prepareData((Long) studentId1, 78575785897L, "Amresh", true, 10, 'C', (byte) 5, (short) 8,
                (float) 69.6, 163.76765654, enrolmentDate, enrolmentTime, joiningDateAndTime, new Integer(3), new Long(
                        978423946455l), 135434.89, newSqlDate, sqlTime, sqlTimestamp, bigDecimal, bigInteger, calendar,
                new StudentHBase()));
        StudentHBase s = em.find(StudentHBase.class, studentId1);
        Assert.assertNotNull(s);
        Assert.assertEquals("Amresh", s.getStudentName());
        // modify record.
        s.setStudentName("NewAmresh");
        em.merge(s);
        // emf.close();
        Query q = em.createQuery("Select p from StudentHBase p where p.studentName = NewAmresh");
        List<StudentHBase> results = q.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(1, results.size());
    }

    @Override
    void startServer() throws InterruptedException
    {
        cli = new HBaseCli();
        cli.startCluster();
    }

    @Override
    void stopServer() throws InterruptedException, MasterNotRunningException, ZooKeeperConnectionException, IOException
    {
    }

    @Override
    void createSchema()
    {
        cli.createTable("STUDENT");
    }

    @Override
    void deleteSchema()
    {
        cli.dropTable("NETSTAT_DTL_SMRY");
        cli.dropTable("STUDENT");
        cli.dropTable("PERSON_HBASE");
        cli.dropTable("PERSON");
        cli.dropTable("Address");
    }

}
