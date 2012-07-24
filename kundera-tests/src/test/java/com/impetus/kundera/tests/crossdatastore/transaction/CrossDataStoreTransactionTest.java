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
package com.impetus.kundera.tests.crossdatastore.transaction;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.tests.cli.CassandraCli;
import com.impetus.kundera.tests.crossdatastore.useraddress.entities.HabitatOToOFKEntity;
import com.impetus.kundera.tests.crossdatastore.useraddress.entities.PersonnelOToOFKEntity;

/**
 * @author vivek.mishra
 * 
 */
public class CrossDataStoreTransactionTest
{

    private EntityManagerFactory emf;

    private EntityManager em;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        emf = Persistence.createEntityManagerFactory("secIdxAddCassandra,addMongo");
        em = emf.createEntityManager();

        // CassandraCli.cassandraSetUp();
        // CassandraCli.createKeySpace("KunderaTests");
    }

    @Test
    public void testRollback()
    {
        PersonnelOToOFKEntity person = new PersonnelOToOFKEntity();
        person.setPersonId("1_p");
        person.setPersonName("crossdata-store");
        HabitatOToOFKEntity address = new HabitatOToOFKEntity();
        address.setAddressId("1_a");
        address.setStreet("my street");
        person.setAddress(address);
        try
        {
            em.persist(person);
        }
        catch (Exception ex)
        {
            HabitatOToOFKEntity found = em.find(HabitatOToOFKEntity.class, "1_a");
            Assert.assertNull(found);
        }
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        // CassandraCli.dropKeySpace("KunderaExamples");
    }

}
