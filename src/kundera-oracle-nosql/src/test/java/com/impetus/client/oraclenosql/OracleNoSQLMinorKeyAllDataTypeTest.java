/**
 * Copyright 2013 Impetus Infotech.
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
package com.impetus.client.oraclenosql;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Calendar;
import java.util.Date;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.oraclenosql.entities.PersonOracleNoSQLAllDataType;

/**
 * Test case for validating correct persistence for all supported data types
 * 
 * @author amresh.singh
 */
public class OracleNoSQLMinorKeyAllDataTypeTest
{

    /** The emf. */
    private static EntityManagerFactory emf;

    /** The em. */
    private static EntityManager em;

    Calendar cal1;

    Calendar cal2;

    /**
     * Sets the up.
     * 
     * @throws Exception
     *             the exception
     */
    @Before
    public void setUp() throws Exception
    {
        emf = Persistence.createEntityManagerFactory("twikvstore");
        em = emf.createEntityManager();
        cal1 = Calendar.getInstance();
        cal2 = Calendar.getInstance();

        cal1.setTime(new Date(1344079067777l));
        cal2.setTime(new Date(1344079068888l));
    }

    @After
    public void tearDown()
    {
        em.close();
        emf.close();
        
    }

    @Test
    public void executeTest()
    {
        PersonOracleNoSQLAllDataType person1 = buildPerson1();
        PersonOracleNoSQLAllDataType person2 = buildPerson2();

        // Insert Records
        persistPerson(person1);
        persistPerson(person2);

        // Find Records
        em.clear();
        PersonOracleNoSQLAllDataType p1 = findById(1234567l);
        PersonOracleNoSQLAllDataType p2 = findById(1234568l);
        assertPerson1(p1);
        assertPerson2(p2);

        // Delete records
        em.remove(p1);
        em.remove(p2);
        em.clear();
        Assert.assertNull(findById(1234567l));
        Assert.assertNull(findById(1234568l));

    }

    protected void persistPerson(PersonOracleNoSQLAllDataType p)
    {
        em.persist(p);
    }

    protected PersonOracleNoSQLAllDataType findById(Object personId)
    {
        return em.find(PersonOracleNoSQLAllDataType.class, personId);
    }

    protected void updatePerson(PersonOracleNoSQLAllDataType person)
    {
        em.merge(person);
    }

    protected void deletePerson(PersonOracleNoSQLAllDataType person)
    {
        em.remove(person);
    }

    private PersonOracleNoSQLAllDataType buildPerson1()
    {

        PersonOracleNoSQLAllDataType person = new PersonOracleNoSQLAllDataType(1234567l, "Labs", false, 31, 'C',
                (byte) 8, (short) 5, (float) 10.0, 163.12, new Date(Long.parseLong("1344079065781")), new Date(
                        Long.parseLong("1344079067623")), new Date(Long.parseLong("1344079069105")), 2, new Long(
                        3634521523423L), new Double(0.23452342343), new java.sql.Date(new Date(
                        Long.parseLong("1344079061111")).getTime()), new java.sql.Time(new Date(
                        Long.parseLong("1344079062222")).getTime()), new java.sql.Timestamp(new Date(
                        Long.parseLong("13440790653333")).getTime()), new BigInteger("123456789"), new BigDecimal(
                        123456789), cal1);
        return person;
    }

    private void assertPerson1(PersonOracleNoSQLAllDataType p)
    {
        Assert.assertNotNull(p);
        Assert.assertEquals(1234567l, p.getPersonId());
        Assert.assertEquals("Labs", p.getPersonName());
        Assert.assertEquals(false, p.isExceptional());
        Assert.assertEquals(31, p.getAge());
        Assert.assertEquals('C', p.getGrade());
        Assert.assertEquals((byte) 8, p.getDigitalSignature());
        Assert.assertEquals((short) 5, p.getRating());
        Assert.assertEquals((float) 10.0, p.getCompliance(), 0.0);
        Assert.assertEquals(163.12, p.getHeight(), 0.0);
        Assert.assertEquals(new Date(Long.parseLong("1344079065781")), p.getEnrolmentDate());
        Assert.assertEquals(new Date(Long.parseLong("1344079067623")), p.getEnrolmentTime());
        Assert.assertEquals(new Date(Long.parseLong("1344079069105")), p.getJoiningDateAndTime());
        Assert.assertEquals(new Integer(2), p.getYearsSpent());
        Assert.assertEquals(new Long(3634521523423L), p.getUniqueId());
        Assert.assertEquals(new Double(0.23452342343), p.getMonthlySalary());
        Assert.assertEquals(new java.sql.Date(new Date(Long.parseLong("1344079061111")).getTime()), p.getBirthday());
        Assert.assertEquals(new java.sql.Time(new Date(Long.parseLong("1344079062222")).getTime()), p.getBirthtime());
        Assert.assertEquals(new java.sql.Timestamp(new Date(Long.parseLong("13440790653333")).getTime()),
                p.getAnniversary());
        Assert.assertEquals(new BigInteger("123456789"), p.getJobAttempts());
        Assert.assertEquals(new BigDecimal(123456789), p.getAccumulatedWealth());
        Assert.assertEquals(cal1, p.getGraduationDay());
    }

    private PersonOracleNoSQLAllDataType buildPerson2()
    {

        PersonOracleNoSQLAllDataType person = new PersonOracleNoSQLAllDataType(1234568l, "ODC", true, 32, 'A',
                (byte) 10, (short) 8, (float) 9.80, 323.3, new Date(Long.parseLong("1344079063412")), new Date(
                        Long.parseLong("1344079068266")), new Date(Long.parseLong("1344079061078")), 5, new Long(
                        25423452343L), new Double(0.76452343), new java.sql.Date(new Date(
                        Long.parseLong("1344079064444")).getTime()), new java.sql.Time(new Date(
                        Long.parseLong("1344079065555")).getTime()), new java.sql.Timestamp(new Date(
                        Long.parseLong("1344079066666")).getTime()), new BigInteger("123456790"), new BigDecimal(
                        123456790), cal2);
        return person;
    }

    private void assertPerson2(PersonOracleNoSQLAllDataType p)
    {
        Assert.assertNotNull(p);
        Assert.assertEquals(1234568l, p.getPersonId());
        Assert.assertEquals("ODC", p.getPersonName());
        Assert.assertEquals(true, p.isExceptional());
        Assert.assertEquals(32, p.getAge());
        Assert.assertEquals('A', p.getGrade());
        Assert.assertEquals((byte) 10, p.getDigitalSignature());
        Assert.assertEquals((short) 8, p.getRating());
        Assert.assertEquals((float) 9.80, p.getCompliance(), 0.0);
        Assert.assertEquals(323.3, p.getHeight(), 0.0);
        Assert.assertEquals(new Date(Long.parseLong("1344079063412")), p.getEnrolmentDate());
        Assert.assertEquals(new Date(Long.parseLong("1344079068266")), p.getEnrolmentTime());
        Assert.assertEquals(new Date(Long.parseLong("1344079061078")), p.getJoiningDateAndTime());
        Assert.assertEquals(new Integer(5), p.getYearsSpent());
        Assert.assertEquals(new Long(25423452343L), p.getUniqueId());
        Assert.assertEquals(new Double(0.76452343), p.getMonthlySalary());
        Assert.assertEquals(new java.sql.Date(new Date(Long.parseLong("1344079064444")).getTime()), p.getBirthday());
        Assert.assertEquals(new java.sql.Time(new Date(Long.parseLong("1344079065555")).getTime()), p.getBirthtime());
        Assert.assertEquals(new java.sql.Timestamp(new Date(Long.parseLong("1344079066666")).getTime()),
                p.getAnniversary());
        Assert.assertEquals(new BigInteger("123456790"), p.getJobAttempts());
        Assert.assertEquals(new BigDecimal(123456790), p.getAccumulatedWealth());
        Assert.assertEquals(cal2, p.getGraduationDay());
    }

}
