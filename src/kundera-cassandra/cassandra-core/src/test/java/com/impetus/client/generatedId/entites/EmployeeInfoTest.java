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
package com.impetus.client.generatedId.entites;

import java.util.HashMap;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.cassandra.common.CassandraConstants;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.cassandra.persistence.CassandraCli;

public class EmployeeInfoTest
{
    private EntityManagerFactory emf;

    @Before
    public void setUp() throws Exception
    {
        CassandraCli.cassandraSetUp();
        HashMap propertyMap = new HashMap();
        propertyMap.put(PersistenceProperties.KUNDERA_DDL_AUTO_PREPARE, "create");
        propertyMap.put(CassandraConstants.CQL_VERSION,	CassandraConstants.CQL_VERSION_3_0);
        emf = Persistence.createEntityManagerFactory("cassandra_generated_id", propertyMap);
    }

    //@Test
    //TODO: not working on CQL3 (issue with updating kundera_sequences, expected id:1 found:2)
    public void test()
    {
        EntityManager em = emf.createEntityManager();
        EmployeeInfo emp_info = new EmployeeInfo();
        EmployeeAddress address_info = new EmployeeAddress();
        address_info.setStreet("street");
        emp_info.setAddress(address_info);
//        emp_info.setEmployeeName("vivek");
        em.persist(emp_info);

        em.clear();

        EmployeeInfo result = em.find(EmployeeInfo.class, 1l);

        Assert.assertNotNull(result);
        Assert.assertNotNull(result.getAddress());
        Assert.assertNotNull(result.getAddress().getStreet());
        Assert.assertNotNull(result.getAddress().getAddress());
        Assert.assertEquals("street", result.getAddress().getStreet());
        
        result.getAddress().setStreet("newStreet");
        em.merge(result);
        
        em.clear();
        
        result = em.find(EmployeeInfo.class, 1l);
        Assert.assertNotNull(result);
        Assert.assertNotNull(result.getAddress());
        Assert.assertNotNull(result.getAddress().getStreet());
        Assert.assertNotNull(result.getAddress().getAddress());
        Assert.assertEquals("newStreet", result.getAddress().getStreet());
        
        em.remove(result);
        em.clear();
        result = em.find(EmployeeInfo.class, 1l);
        Assert.assertNull(result);
        
    }

    @After
    public void tearDown() throws Exception
    {
        CassandraCli.dropKeySpace("kunderaGeneratedId");
        emf.close();
    }

}
