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
package com.impetus.client.cassandra.generatedId.entites;

import java.util.UUID;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.client.cassandra.persistence.CassandraCli;

/**
 * Employee info jun
 * @author vivek.mishra
 *
 */
public class EmployeeInfoTest
{
    private EntityManagerFactory emf;

    @Before
    public void setUp() throws Exception
    {
        CassandraCli.cassandraSetUp();
        CassandraCli.createKeySpace("KunderaExamples");
        emf = Persistence.createEntityManagerFactory("ds_pu");
    }

    @Test
    public void test()
    {
        EntityManager em = emf.createEntityManager();
        EmployeeInfo emp_info = new EmployeeInfo();
        EmployeeAddress address_info = new EmployeeAddress();
        address_info.setStreet("street");
        emp_info.setAddress(address_info);
        em.persist(emp_info);
        
        UUID key = emp_info.getUserid();
        
        em.clear();

        EmployeeInfo result = em.find(EmployeeInfo.class, key);

        Assert.assertNotNull(result);
        Assert.assertNotNull(result.getAddress());
        Assert.assertNotNull(result.getAddress().getStreet());
        Assert.assertNotNull(result.getAddress().getAddress());
        Assert.assertEquals("street", result.getAddress().getStreet());
        
        result.getAddress().setStreet("newStreet");
        em.merge(result);
        
        em.clear();
        
        result = em.find(EmployeeInfo.class, key);
        Assert.assertNotNull(result);
        Assert.assertNotNull(result.getAddress());
        Assert.assertNotNull(result.getAddress().getStreet());
        Assert.assertNotNull(result.getAddress().getAddress());
        Assert.assertEquals("newStreet", result.getAddress().getStreet());
        
        em.remove(result);
        em.clear();
        result = em.find(EmployeeInfo.class, key);
        Assert.assertNull(result);
        
    }

    @After
    public void tearDown() throws Exception
    {
        CassandraCli.dropKeySpace("KunderaExamples");
        emf.close();
    }

}
