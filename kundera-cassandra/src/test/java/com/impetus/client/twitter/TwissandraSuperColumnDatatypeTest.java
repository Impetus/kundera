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
package com.impetus.client.twitter;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.cassandra.locator.SimpleStrategy;
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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.persistence.CassandraCli;
import com.impetus.client.twitter.entities.ProfessionalDetail;
import com.impetus.client.twitter.entities.UserCassandra;
import com.impetus.kundera.Constants;

/**
 * Test case for data type compatibility testing for Twissandra
 * @author amresh.singh
 */
public class TwissandraSuperColumnDatatypeTest extends TwitterTestBase
{
    /** The Constant LOG. */
    private static final Log log = LogFactory.getLog(TwissandraSuperColumnDatatypeTest.class);

    @Before
    public void setUp() throws Exception
    {
        setUpInternal(persistenceUnit);
    }

    @Test
    public void onExecute() throws Exception
    {
        // Insert, Find and Update
        addAllUserInfo();
        getUserById();
        updateUser();
        
        //Queries for all data types
        getUserByProfessionId();
        getUserByDepartmentName();
        getExceptionalUsers();
        getUserByAge();
        getUserByGrade();
        getUserByDigitalSignature();
        getUserByRating();
        getUserByCompliance();
        getUserByHeight();
        getUserByEnrolmentDate();
        getUserByEnrolmentTime();
        getUserByJoiningDateAndTime();
        
        getUserByYearsSpent();
        getUserByUniqueId();
        getUserByMonthlySalary();
        getUserByBirthday();
        getUserByBirthtime();
        getUserByAnniversary();
        getUserByJobAttempts();
        getUserByAccumulatedWealth();
        getUserByGraduationDay();
        
        // Remove Users
        removeUsers();
    }

    @After
    public void tearDown() throws Exception
    {
        tearDownInternal();
    }

    @Override
    void startServer()
    {
        try
        {
            CassandraCli.cassandraSetUp();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        catch (TException e)
        {
            e.printStackTrace();
        }
        catch (InvalidRequestException e)
        {
            e.printStackTrace();
        }
        catch (UnavailableException e)
        {
            e.printStackTrace();
        }
        catch (TimedOutException e)
        {
            e.printStackTrace();
        }
        catch (SchemaDisagreementException e)
        {
            e.printStackTrace();
        }
    }
    
    
    /************** Queries on Professional Data ****************/
    void getUserByProfessionId() {
        //User 1
        twitter.createEntityManager();        
        List<UserCassandra> users = twitter.findUserByProfessionId(1234567);
        Assert.assertNotNull(users);
        Assert.assertFalse(users.isEmpty());
        Assert.assertTrue(users.size() == 1);
        UserCassandra user = users.get(0);
        Assert.assertNotNull(user);
        Assert.assertEquals(userId1, user.getUserId());
        ProfessionalDetail pd = user.getProfessionalDetail();
        Assert.assertNotNull(pd);
        Assert.assertEquals(1234567, pd.getProfessionId());        
        twitter.closeEntityManager();
        
        //User2
        twitter.createEntityManager();        
        List<UserCassandra> users2 = twitter.findUserByProfessionId(1234568);
        Assert.assertNotNull(users2);
        Assert.assertFalse(users2.isEmpty());
        Assert.assertTrue(users2.size() == 1);
        UserCassandra user2 = users2.get(0);
        Assert.assertNotNull(user2);
        Assert.assertEquals(userId2, user2.getUserId());
        ProfessionalDetail pd2 = user2.getProfessionalDetail();
        Assert.assertNotNull(pd2);
        Assert.assertEquals(1234568, pd2.getProfessionId());        
        twitter.closeEntityManager();
    }
    
    void getUserByDepartmentName()
    {
        //User 1
        twitter.createEntityManager();        
        List<UserCassandra> users = twitter.findUserByDepartment("Labs");
        Assert.assertNotNull(users);
        Assert.assertFalse(users.isEmpty());
        Assert.assertTrue(users.size() == 1);
        UserCassandra user = users.get(0);
        Assert.assertNotNull(user);
        Assert.assertEquals(userId1, user.getUserId());
        ProfessionalDetail pd = user.getProfessionalDetail();
        Assert.assertNotNull(pd);
        Assert.assertEquals("Labs", pd.getDepartmentName());        
        twitter.closeEntityManager();
        
        //User2
        twitter.createEntityManager();        
        List<UserCassandra> users2 = twitter.findUserByDepartment("ODC");
        Assert.assertNotNull(users2);
        Assert.assertFalse(users2.isEmpty());
        Assert.assertTrue(users2.size() == 1);
        UserCassandra user2 = users2.get(0);
        Assert.assertNotNull(user2);
        Assert.assertEquals(userId2, user2.getUserId());
        ProfessionalDetail pd2 = user2.getProfessionalDetail();
        Assert.assertNotNull(pd2);
        Assert.assertEquals("ODC", pd2.getDepartmentName());        
        twitter.closeEntityManager();  
    }
    
    void getExceptionalUsers()
    {
        //User 1
        twitter.createEntityManager();        
        List<UserCassandra> users = twitter.findExceptionalUsers();
        Assert.assertNotNull(users);
        Assert.assertFalse(users.isEmpty());
        Assert.assertTrue(users.size() == 1);
        UserCassandra user = users.get(0);
        Assert.assertNotNull(user);
        Assert.assertEquals(userId2, user.getUserId());
        ProfessionalDetail pd = user.getProfessionalDetail();
        Assert.assertNotNull(pd);
        Assert.assertEquals(true, pd.isExceptional());        
        twitter.closeEntityManager();  
        
    }
    
    void getUserByAge() {
        twitter.createEntityManager();
        List<UserCassandra> users = twitter.findUserByAge(31);
        Assert.assertNotNull(users);
        Assert.assertFalse(users.isEmpty());
        Assert.assertTrue(users.size() == 1);
        UserCassandra user = users.get(0);
        Assert.assertNotNull(user);
        Assert.assertEquals(userId1, user.getUserId());
        ProfessionalDetail pd = user.getProfessionalDetail();
        Assert.assertNotNull(pd);
        Assert.assertEquals(31, pd.getAge());          
        twitter.closeEntityManager();
        
        twitter.createEntityManager();
        List<UserCassandra> users2 = twitter.findUserByAge(32);
        Assert.assertNotNull(users2);
        Assert.assertFalse(users2.isEmpty());
        Assert.assertTrue(users2.size() == 1);
        UserCassandra user2 = users2.get(0);
        Assert.assertNotNull(user2);
        Assert.assertEquals(userId2, user2.getUserId());
        ProfessionalDetail pd2 = user2.getProfessionalDetail();
        Assert.assertNotNull(pd2);
        Assert.assertEquals(32, pd2.getAge());          
        twitter.closeEntityManager();
    }
    
    void getUserByGrade() {
        twitter.createEntityManager();
        List<UserCassandra> users = twitter.findUserByGrade('C');
        Assert.assertNotNull(users);
        Assert.assertFalse(users.isEmpty());
        Assert.assertTrue(users.size() == 1);
        UserCassandra user = users.get(0);
        Assert.assertNotNull(user);
        Assert.assertEquals(userId1, user.getUserId());
        ProfessionalDetail pd = user.getProfessionalDetail();
        Assert.assertNotNull(pd);
        Assert.assertEquals('C', pd.getGrade());          
        twitter.closeEntityManager();
        
        twitter.createEntityManager();
        List<UserCassandra> users2 = twitter.findUserByGrade('A');
        Assert.assertNotNull(users2);
        Assert.assertFalse(users2.isEmpty());
        Assert.assertTrue(users2.size() == 1);
        UserCassandra user2 = users2.get(0);
        Assert.assertNotNull(user2);
        Assert.assertEquals(userId2, user2.getUserId());
        ProfessionalDetail pd2 = user2.getProfessionalDetail();
        Assert.assertNotNull(pd2);
        Assert.assertEquals('A', pd2.getGrade());          
        twitter.closeEntityManager();
    }
    
    void getUserByDigitalSignature() {
        twitter.createEntityManager();
        List<UserCassandra> users = twitter.findUserByDigitalSignature((byte)8);
        Assert.assertNotNull(users);
        Assert.assertFalse(users.isEmpty());
        Assert.assertTrue(users.size() == 1);
        UserCassandra user = users.get(0);
        Assert.assertNotNull(user);
        Assert.assertEquals(userId1, user.getUserId());
        ProfessionalDetail pd = user.getProfessionalDetail();
        Assert.assertNotNull(pd);
        Assert.assertEquals((byte)8, pd.getDigitalSignature());          
        twitter.closeEntityManager();
        
        twitter.createEntityManager();
        List<UserCassandra> users2 = twitter.findUserByDigitalSignature((byte)10);
        Assert.assertNotNull(users2);
        Assert.assertFalse(users2.isEmpty());
        Assert.assertTrue(users2.size() == 1);
        UserCassandra user2 = users2.get(0);
        Assert.assertNotNull(user2);
        Assert.assertEquals(userId2, user2.getUserId());
        ProfessionalDetail pd2 = user2.getProfessionalDetail();
        Assert.assertNotNull(pd2);
        Assert.assertEquals((byte)10, pd2.getDigitalSignature());          
        twitter.closeEntityManager();
    }   
    
    void getUserByRating() {
        twitter.createEntityManager();
        List<UserCassandra> users = twitter.findUserByRating((short)5);
        Assert.assertNotNull(users);
        Assert.assertFalse(users.isEmpty());
        Assert.assertTrue(users.size() == 1);
        UserCassandra user = users.get(0);
        Assert.assertNotNull(user);
        Assert.assertEquals(userId1, user.getUserId());
        ProfessionalDetail pd = user.getProfessionalDetail();
        Assert.assertNotNull(pd);
        Assert.assertEquals((short)5, pd.getRating());          
        twitter.closeEntityManager();
        
        twitter.createEntityManager();
        List<UserCassandra> users2 = twitter.findUserByRating((short)8);
        Assert.assertNotNull(users2);
        Assert.assertFalse(users2.isEmpty());
        Assert.assertTrue(users2.size() == 1);
        UserCassandra user2 = users2.get(0);
        Assert.assertNotNull(user2);
        Assert.assertEquals(userId2, user2.getUserId());
        ProfessionalDetail pd2 = user2.getProfessionalDetail();
        Assert.assertNotNull(pd2);
        Assert.assertEquals((short)8, pd2.getRating());          
        twitter.closeEntityManager();
    }   
    
    void getUserByCompliance() {
        twitter.createEntityManager();
        List<UserCassandra> users = twitter.findUserByCompliance((float)10.0);
        Assert.assertNotNull(users);
        Assert.assertFalse(users.isEmpty());
        Assert.assertTrue(users.size() == 1);
        UserCassandra user = users.get(0);
        Assert.assertNotNull(user);
        Assert.assertEquals(userId1, user.getUserId());
        ProfessionalDetail pd = user.getProfessionalDetail();
        Assert.assertNotNull(pd);
        Assert.assertEquals((float)10.0, pd.getCompliance(), 0.0);          
        twitter.closeEntityManager();
        
        twitter.createEntityManager();
        List<UserCassandra> users2 = twitter.findUserByCompliance((float)9.80);
        Assert.assertNotNull(users2);
        Assert.assertFalse(users2.isEmpty());
        Assert.assertTrue(users2.size() == 1);
        UserCassandra user2 = users2.get(0);
        Assert.assertNotNull(user2);
        Assert.assertEquals(userId2, user2.getUserId());
        ProfessionalDetail pd2 = user2.getProfessionalDetail();
        Assert.assertNotNull(pd2);
        Assert.assertEquals((float)9.80, pd2.getCompliance(), 0.0);          
        twitter.closeEntityManager();
    }  
    
    void getUserByHeight()
    {
        twitter.createEntityManager();
        List<UserCassandra> users = twitter.findUserByHeight(163.12);
        Assert.assertNotNull(users);
        Assert.assertFalse(users.isEmpty());
        Assert.assertTrue(users.size() == 1);
        UserCassandra user = users.get(0);
        Assert.assertNotNull(user);
        Assert.assertEquals(userId1, user.getUserId());
        ProfessionalDetail pd = user.getProfessionalDetail();
        Assert.assertNotNull(pd);
        Assert.assertEquals(163.12, pd.getHeight(), 0.0);          
        twitter.closeEntityManager();
        
        twitter.createEntityManager();
        List<UserCassandra> users2 = twitter.findUserByHeight(323.3);
        Assert.assertNotNull(users2);
        Assert.assertFalse(users2.isEmpty());
        Assert.assertTrue(users2.size() == 1);
        UserCassandra user2 = users2.get(0);
        Assert.assertNotNull(user2);
        Assert.assertEquals(userId2, user2.getUserId());
        ProfessionalDetail pd2 = user2.getProfessionalDetail();
        Assert.assertNotNull(pd2);
        Assert.assertEquals(323.3, pd2.getHeight(), 0.0);          
        twitter.closeEntityManager();
        
    }
    
    void getUserByEnrolmentDate()
    {
        twitter.createEntityManager();
        List<UserCassandra> users = twitter.findUserByEnrolmentDate(new Date(Long.parseLong("1344079065781")));
        Assert.assertNotNull(users);
        Assert.assertFalse(users.isEmpty());
        Assert.assertTrue(users.size() == 1);
        UserCassandra user = users.get(0);
        Assert.assertNotNull(user);
        Assert.assertEquals(userId1, user.getUserId());
        ProfessionalDetail pd = user.getProfessionalDetail();
        Assert.assertNotNull(pd);
        Assert.assertEquals(new Date(Long.parseLong("1344079065781")), pd.getEnrolmentDate());          
        twitter.closeEntityManager();
        
        twitter.createEntityManager();
        List<UserCassandra> users2 = twitter.findUserByEnrolmentDate(new Date(Long.parseLong("1344079063412")));
        Assert.assertNotNull(users2);
        Assert.assertFalse(users2.isEmpty());
        Assert.assertTrue(users2.size() == 1);
        UserCassandra user2 = users2.get(0);
        Assert.assertNotNull(user2);
        Assert.assertEquals(userId2, user2.getUserId());
        ProfessionalDetail pd2 = user2.getProfessionalDetail();
        Assert.assertNotNull(pd2);
        Assert.assertEquals(new Date(Long.parseLong("1344079063412")), pd2.getEnrolmentDate());          
        twitter.closeEntityManager();
        
    }
    
    void getUserByEnrolmentTime()
    {
        twitter.createEntityManager();
        List<UserCassandra> users = twitter.findUserByEnrolmentTime(new Date(Long.parseLong("1344079067623")));
        Assert.assertNotNull(users);
        Assert.assertFalse(users.isEmpty());
        Assert.assertTrue(users.size() == 1);
        UserCassandra user = users.get(0);
        Assert.assertNotNull(user);
        Assert.assertEquals(userId1, user.getUserId());
        ProfessionalDetail pd = user.getProfessionalDetail();
        Assert.assertNotNull(pd);
        Assert.assertEquals(new Date(Long.parseLong("1344079067623")), pd.getEnrolmentTime());          
        twitter.closeEntityManager();
        
        twitter.createEntityManager();
        List<UserCassandra> users2 = twitter.findUserByEnrolmentTime(new Date(Long.parseLong("1344079068266")));
        Assert.assertNotNull(users2);
        Assert.assertFalse(users2.isEmpty());
        Assert.assertTrue(users2.size() == 1);
        UserCassandra user2 = users2.get(0);
        Assert.assertNotNull(user2);
        Assert.assertEquals(userId2, user2.getUserId());
        ProfessionalDetail pd2 = user2.getProfessionalDetail();
        Assert.assertNotNull(pd2);
        Assert.assertEquals(new Date(Long.parseLong("1344079068266")), pd2.getEnrolmentTime());          
        twitter.closeEntityManager();
        
    }
    
    void getUserByJoiningDateAndTime()
    {
        twitter.createEntityManager();
        List<UserCassandra> users = twitter.findUserByJoiningDateAndTime(new Date(Long.parseLong("1344079069105")));
        Assert.assertNotNull(users);
        Assert.assertFalse(users.isEmpty());
        Assert.assertTrue(users.size() == 1);
        UserCassandra user = users.get(0);
        Assert.assertNotNull(user);
        Assert.assertEquals(userId1, user.getUserId());
        ProfessionalDetail pd = user.getProfessionalDetail();
        Assert.assertNotNull(pd);
        Assert.assertEquals(new Date(Long.parseLong("1344079069105")), pd.getJoiningDateAndTime());          
        twitter.closeEntityManager();
        
        twitter.createEntityManager();
        List<UserCassandra> users2 = twitter.findUserByJoiningDateAndTime(new Date(Long.parseLong("1344079061078")));
        Assert.assertNotNull(users2);
        Assert.assertFalse(users2.isEmpty());
        Assert.assertTrue(users2.size() == 1);
        UserCassandra user2 = users2.get(0);
        Assert.assertNotNull(user2);
        Assert.assertEquals(userId2, user2.getUserId());
        ProfessionalDetail pd2 = user2.getProfessionalDetail();
        Assert.assertNotNull(pd2);
        Assert.assertEquals(new Date(Long.parseLong("1344079061078")), pd2.getJoiningDateAndTime());          
        twitter.closeEntityManager();        
    }
    
    void getUserByYearsSpent()
    {
        twitter.createEntityManager();
        List<UserCassandra> users = twitter.findUserByYearsSpent(new Integer(2));
        Assert.assertNotNull(users);
        Assert.assertFalse(users.isEmpty());
        Assert.assertTrue(users.size() == 1);
        UserCassandra user = users.get(0);
        Assert.assertNotNull(user);
        Assert.assertEquals(userId1, user.getUserId());
        ProfessionalDetail pd = user.getProfessionalDetail();
        Assert.assertNotNull(pd);
        Assert.assertEquals(new Integer(2), pd.getYearsSpent());          
        twitter.closeEntityManager();
        
        twitter.createEntityManager();
        List<UserCassandra> users2 = twitter.findUserByYearsSpent(new Integer(5));
        Assert.assertNotNull(users2);
        Assert.assertFalse(users2.isEmpty());
        Assert.assertTrue(users2.size() == 1);
        UserCassandra user2 = users2.get(0);
        Assert.assertNotNull(user2);
        Assert.assertEquals(userId2, user2.getUserId());
        ProfessionalDetail pd2 = user2.getProfessionalDetail();
        Assert.assertNotNull(pd2);
        Assert.assertEquals(new Integer(5), pd2.getYearsSpent());          
        twitter.closeEntityManager();        
    }
    
    void getUserByUniqueId()
    {
        twitter.createEntityManager();
        List<UserCassandra> users = twitter.findUserByUniqueId(new Long(3634521523423L));
        Assert.assertNotNull(users);
        Assert.assertFalse(users.isEmpty());
        Assert.assertTrue(users.size() == 1);
        UserCassandra user = users.get(0);
        Assert.assertNotNull(user);
        Assert.assertEquals(userId1, user.getUserId());
        ProfessionalDetail pd = user.getProfessionalDetail();
        Assert.assertNotNull(pd);
        Assert.assertEquals(new Long(3634521523423L), pd.getUniqueId());          
        twitter.closeEntityManager();
        
        twitter.createEntityManager();
        List<UserCassandra> users2 = twitter.findUserByUniqueId(new Long(25423452343L));
        Assert.assertNotNull(users2);
        Assert.assertFalse(users2.isEmpty());
        Assert.assertTrue(users2.size() == 1);
        UserCassandra user2 = users2.get(0);
        Assert.assertNotNull(user2);
        Assert.assertEquals(userId2, user2.getUserId());
        ProfessionalDetail pd2 = user2.getProfessionalDetail();
        Assert.assertNotNull(pd2);
        Assert.assertEquals(new Long(25423452343L), pd2.getUniqueId());          
        twitter.closeEntityManager();        
    }
    
    void getUserByMonthlySalary()
    {
        twitter.createEntityManager();
        List<UserCassandra> users = twitter.findUserByMonthlySalary(new Double(0.23452342343));
        Assert.assertNotNull(users);
        Assert.assertFalse(users.isEmpty());
        Assert.assertTrue(users.size() == 1);
        UserCassandra user = users.get(0);
        Assert.assertNotNull(user);
        Assert.assertEquals(userId1, user.getUserId());
        ProfessionalDetail pd = user.getProfessionalDetail();
        Assert.assertNotNull(pd);
        Assert.assertEquals(new Double(0.23452342343), pd.getMonthlySalary());          
        twitter.closeEntityManager();
        
        twitter.createEntityManager();
        List<UserCassandra> users2 = twitter.findUserByMonthlySalary(new Double(0.76452343));
        Assert.assertNotNull(users2);
        Assert.assertFalse(users2.isEmpty());
        Assert.assertTrue(users2.size() == 1);
        UserCassandra user2 = users2.get(0);
        Assert.assertNotNull(user2);
        Assert.assertEquals(userId2, user2.getUserId());
        ProfessionalDetail pd2 = user2.getProfessionalDetail();
        Assert.assertNotNull(pd2);
        Assert.assertEquals(new Double(0.76452343), pd2.getMonthlySalary());          
        twitter.closeEntityManager();        
    }
    
    void getUserByBirthday()
    {
        twitter.createEntityManager();
        List<UserCassandra> users = twitter.findUserByBirthday(new java.sql.Date(new Date(Long.parseLong("1344079061111")).getTime()));
        Assert.assertNotNull(users);
        Assert.assertFalse(users.isEmpty());
        Assert.assertTrue(users.size() == 1);
        UserCassandra user = users.get(0);
        Assert.assertNotNull(user);
        Assert.assertEquals(userId1, user.getUserId());
        ProfessionalDetail pd = user.getProfessionalDetail();
        Assert.assertNotNull(pd);
        Assert.assertEquals(new java.sql.Date(new Date(Long.parseLong("1344079061111")).getTime()), pd.getBirthday());          
        twitter.closeEntityManager();
        
        twitter.createEntityManager();
        List<UserCassandra> users2 = twitter.findUserByBirthday(new java.sql.Date(new Date(Long.parseLong("1344079064444")).getTime()));
        Assert.assertNotNull(users2);
        Assert.assertFalse(users2.isEmpty());
        Assert.assertTrue(users2.size() == 1);
        UserCassandra user2 = users2.get(0);
        Assert.assertNotNull(user2);
        Assert.assertEquals(userId2, user2.getUserId());
        ProfessionalDetail pd2 = user2.getProfessionalDetail();
        Assert.assertNotNull(pd2);
        Assert.assertEquals(new java.sql.Date(new Date(Long.parseLong("1344079064444")).getTime()), pd2.getBirthday());          
        twitter.closeEntityManager();        
    }
    
    void getUserByBirthtime()
    {
        twitter.createEntityManager();
        List<UserCassandra> users = twitter.findUserByBirthtime(new java.sql.Time(new Date(Long.parseLong("1344079062222")).getTime()));
        Assert.assertNotNull(users);
        Assert.assertFalse(users.isEmpty());
        Assert.assertTrue(users.size() == 1);
        UserCassandra user = users.get(0);
        Assert.assertNotNull(user);
        Assert.assertEquals(userId1, user.getUserId());
        ProfessionalDetail pd = user.getProfessionalDetail();
        Assert.assertNotNull(pd);
        Assert.assertEquals(new java.sql.Time(new Date(Long.parseLong("1344079062222")).getTime()), pd.getBirthtime());          
        twitter.closeEntityManager();
        
        twitter.createEntityManager();
        List<UserCassandra> users2 = twitter.findUserByBirthtime(new java.sql.Time(new Date(Long.parseLong("1344079065555")).getTime()));
        Assert.assertNotNull(users2);
        Assert.assertFalse(users2.isEmpty());
        Assert.assertTrue(users2.size() == 1);
        UserCassandra user2 = users2.get(0);
        Assert.assertNotNull(user2);
        Assert.assertEquals(userId2, user2.getUserId());
        ProfessionalDetail pd2 = user2.getProfessionalDetail();
        Assert.assertNotNull(pd2);
        Assert.assertEquals(new java.sql.Time(new Date(Long.parseLong("1344079065555")).getTime()), pd2.getBirthtime());          
        twitter.closeEntityManager();        
    }
    
    void getUserByAnniversary()
    {
        twitter.createEntityManager();
        List<UserCassandra> users = twitter.findUserByAnniversary(new java.sql.Timestamp(new Date(Long.parseLong("13440790653333")).getTime()));
        Assert.assertNotNull(users);
        Assert.assertFalse(users.isEmpty());
        Assert.assertTrue(users.size() == 1);
        UserCassandra user = users.get(0);
        Assert.assertNotNull(user);
        Assert.assertEquals(userId1, user.getUserId());
        ProfessionalDetail pd = user.getProfessionalDetail();
        Assert.assertNotNull(pd);
        Assert.assertEquals(new java.sql.Timestamp(new Date(Long.parseLong("13440790653333")).getTime()), pd.getAnniversary());          
        twitter.closeEntityManager();
        
        twitter.createEntityManager();
        List<UserCassandra> users2 = twitter.findUserByAnniversary(new java.sql.Timestamp(new Date(Long.parseLong("1344079066666")).getTime()));
        Assert.assertNotNull(users2);
        Assert.assertFalse(users2.isEmpty());
        Assert.assertTrue(users2.size() == 1);
        UserCassandra user2 = users2.get(0);
        Assert.assertNotNull(user2);
        Assert.assertEquals(userId2, user2.getUserId());
        ProfessionalDetail pd2 = user2.getProfessionalDetail();
        Assert.assertNotNull(pd2);
        Assert.assertEquals(new java.sql.Timestamp(new Date(Long.parseLong("1344079066666")).getTime()), pd2.getAnniversary());          
        twitter.closeEntityManager();        
    }
    
    void getUserByJobAttempts()
    {
        twitter.createEntityManager();
        List<UserCassandra> users = twitter.findUserByJobAttempts(new BigInteger("123456789"));
        Assert.assertNotNull(users);
        Assert.assertFalse(users.isEmpty());
        Assert.assertTrue(users.size() == 1);
        UserCassandra user = users.get(0);
        Assert.assertNotNull(user);
        Assert.assertEquals(userId1, user.getUserId());
        ProfessionalDetail pd = user.getProfessionalDetail();
        Assert.assertNotNull(pd);
        Assert.assertEquals(new BigInteger("123456789"), pd.getJobAttempts());          
        twitter.closeEntityManager();
        
        twitter.createEntityManager();
        List<UserCassandra> users2 = twitter.findUserByJobAttempts(new BigInteger("123456790"));
        Assert.assertNotNull(users2);
        Assert.assertFalse(users2.isEmpty());
        Assert.assertTrue(users2.size() == 1);
        UserCassandra user2 = users2.get(0);
        Assert.assertNotNull(user2);
        Assert.assertEquals(userId2, user2.getUserId());
        ProfessionalDetail pd2 = user2.getProfessionalDetail();
        Assert.assertNotNull(pd2);
        Assert.assertEquals(new BigInteger("123456790"), pd2.getJobAttempts());          
        twitter.closeEntityManager();        
    }
    
    void getUserByAccumulatedWealth()
    {
        twitter.createEntityManager();
        List<UserCassandra> users = twitter.findUserByAccumulatedWealth(new BigDecimal(123456789));
        Assert.assertNotNull(users);
        Assert.assertFalse(users.isEmpty());
        Assert.assertTrue(users.size() == 1);
        UserCassandra user = users.get(0);
        Assert.assertNotNull(user);
        Assert.assertEquals(userId1, user.getUserId());
        ProfessionalDetail pd = user.getProfessionalDetail();
        Assert.assertNotNull(pd);
        Assert.assertEquals(new BigDecimal(123456789), pd.getAccumulatedWealth());          
        twitter.closeEntityManager();
        
        twitter.createEntityManager();
        List<UserCassandra> users2 = twitter.findUserByAccumulatedWealth(new BigDecimal(123456790));
        Assert.assertNotNull(users2);
        Assert.assertFalse(users2.isEmpty());
        Assert.assertTrue(users2.size() == 1);
        UserCassandra user2 = users2.get(0);
        Assert.assertNotNull(user2);
        Assert.assertEquals(userId2, user2.getUserId());
        ProfessionalDetail pd2 = user2.getProfessionalDetail();
        Assert.assertNotNull(pd2);
        Assert.assertEquals(new BigDecimal(123456790), pd2.getAccumulatedWealth());          
        twitter.closeEntityManager();        
    }
    
    void getUserByGraduationDay()
    {
        twitter.createEntityManager();
        
        Calendar cal = Calendar.getInstance(); cal.setTime(new Date(1344079067777l));
        List<UserCassandra> users = twitter.findUserByGraduationDay(cal);
        Assert.assertNotNull(users);
        Assert.assertFalse(users.isEmpty());
        Assert.assertTrue(users.size() == 1);
        UserCassandra user = users.get(0);
        Assert.assertNotNull(user);
        Assert.assertEquals(userId1, user.getUserId());
        ProfessionalDetail pd = user.getProfessionalDetail();
        Assert.assertNotNull(pd);
        Assert.assertEquals(cal, pd.getGraduationDay());          
        twitter.closeEntityManager();
        
        twitter.createEntityManager();
        
        Calendar cal2 = Calendar.getInstance(); cal2.setTime(new Date(1344079068888l));
        List<UserCassandra> users2 = twitter.findUserByGraduationDay(cal2);
        Assert.assertNotNull(users2);
        Assert.assertFalse(users2.isEmpty());
        Assert.assertTrue(users2.size() == 1);
        UserCassandra user2 = users2.get(0);
        Assert.assertNotNull(user2);
        Assert.assertEquals(userId2, user2.getUserId());
        ProfessionalDetail pd2 = user2.getProfessionalDetail();
        Assert.assertNotNull(pd2);
        Assert.assertEquals(cal2, pd2.getGraduationDay());          
        twitter.closeEntityManager();        
    }
    

    @Override
    void stopServer()
    {
    }

    @Override
    void createSchema() throws InvalidRequestException, SchemaDisagreementException, TException
    {
        KsDef ksDef = null;

        CfDef userCfDef = new CfDef();
        userCfDef.name = "USER";
        userCfDef.keyspace = keyspace;
        userCfDef.column_type = "Super";
        userCfDef.setComparator_type("UTF8Type");        
        userCfDef.setSubcomparator_type("AsciiType");
        userCfDef.setKey_validation_class("UTF8Type");

        CfDef userIndexCfDef = new CfDef();
        userIndexCfDef.name = "USER" + Constants.INDEX_TABLE_SUFFIX;
        userIndexCfDef.keyspace = keyspace;
        userIndexCfDef.setKey_validation_class("UTF8Type");

        CfDef prefrenceCfDef = new CfDef();
        prefrenceCfDef.name = "PREFERENCE";
        prefrenceCfDef.keyspace = keyspace;
        prefrenceCfDef.setComparator_type("UTF8Type");
        prefrenceCfDef.setDefault_validation_class("UTF8Type");
        ColumnDef columnDef = new ColumnDef(ByteBuffer.wrap("WEBSITE_THEME".getBytes()), "UTF8Type");
        columnDef.index_type = IndexType.KEYS;
        ColumnDef columnDef3 = new ColumnDef(ByteBuffer.wrap("PRIVACY_LEVEL".getBytes()), "UTF8Type");
        columnDef3.index_type = IndexType.KEYS;
        prefrenceCfDef.addToColumn_metadata(columnDef);
        prefrenceCfDef.addToColumn_metadata(columnDef3);

        CfDef externalLinkCfDef = new CfDef();
        externalLinkCfDef.name = "EXTERNAL_LINK";
        externalLinkCfDef.keyspace = keyspace;
        externalLinkCfDef.setComparator_type("UTF8Type");
        externalLinkCfDef.setDefault_validation_class("UTF8Type");
        ColumnDef columnDef1 = new ColumnDef(ByteBuffer.wrap("LINK_TYPE".getBytes()), "UTF8Type");
        columnDef1.index_type = IndexType.KEYS;
        ColumnDef columnDef2 = new ColumnDef(ByteBuffer.wrap("LINK_ADDRESS".getBytes()), "UTF8Type");
        columnDef2.index_type = IndexType.KEYS;
        ColumnDef columnDef4 = new ColumnDef(ByteBuffer.wrap("USER_ID".getBytes()), "UTF8Type");
        columnDef4.index_type = IndexType.KEYS;
        externalLinkCfDef.addToColumn_metadata(columnDef1);
        externalLinkCfDef.addToColumn_metadata(columnDef2);
        externalLinkCfDef.addToColumn_metadata(columnDef4);

        List<CfDef> cfDefs = new ArrayList<CfDef>();
        cfDefs.add(userCfDef);
        cfDefs.add(userIndexCfDef);
        cfDefs.add(prefrenceCfDef);
        cfDefs.add(externalLinkCfDef);
        try
        {
            ksDef = CassandraCli.client.describe_keyspace(keyspace);
            CassandraCli.client.set_keyspace(keyspace);
            List<CfDef> cfDefn = ksDef.getCf_defs();

            for (CfDef cfDef1 : cfDefn)
            {

                if (cfDef1.getName().equalsIgnoreCase("USER"))
                {
                    CassandraCli.client.system_drop_column_family("USER");
                }
                if (cfDef1.getName().equalsIgnoreCase("USER" + Constants.INDEX_TABLE_SUFFIX))
                {
                    CassandraCli.client.system_drop_column_family("USER" + Constants.INDEX_TABLE_SUFFIX);
                }
                if (cfDef1.getName().equalsIgnoreCase("PREFERENCE"))
                {
                    CassandraCli.client.system_drop_column_family("PREFERENCE");
                }
                if (cfDef1.getName().equalsIgnoreCase("EXTERNAL_LINK"))
                {
                    CassandraCli.client.system_drop_column_family("EXTERNAL_LINK");
                }
            }
            CassandraCli.client.system_add_column_family(userCfDef);
            CassandraCli.client.system_add_column_family(userIndexCfDef);
            CassandraCli.client.system_add_column_family(externalLinkCfDef);
            CassandraCli.client.system_add_column_family(prefrenceCfDef);
        }
        catch (NotFoundException e)
        {
            ksDef = new KsDef(keyspace, SimpleStrategy.class.getSimpleName(), cfDefs);

            // Set replication factor
            if (ksDef.strategy_options == null)
            {
                ksDef.strategy_options = new LinkedHashMap<String, String>();
            }
            // Set replication factor, the value MUST be an integer
            ksDef.strategy_options.put("replication_factor", "1");

            CassandraCli.client.system_add_keyspace(ksDef);
        }
        catch (InvalidRequestException e)
        {
            log.error(e.getMessage());
        }
        catch (TException e)
        {
            log.error(e.getMessage());
        }
    }

    @Override
    void deleteSchema()
    {
        /*
         * LOG.warn(
         * "Truncating Column families and finally dropping Keyspace KunderaExamples in Cassandra...."
         * ); CassandraCli.dropColumnFamily("USER", keyspace);
         * CassandraCli.dropColumnFamily("PREFERENCE", keyspace);
         * CassandraCli.dropColumnFamily("EXTERNAL_LINKS", keyspace);
         * CassandraCli.dropKeySpace(keyspace);
         */
    }
    

}
