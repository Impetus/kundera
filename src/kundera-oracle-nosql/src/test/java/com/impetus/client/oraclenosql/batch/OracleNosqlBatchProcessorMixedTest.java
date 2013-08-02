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
package com.impetus.client.oraclenosql.batch;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test case for more than one entities persisted in the same batch
 * 
 * @author amresh.singh
 */
public class OracleNosqlBatchProcessorMixedTest
{
    private static final String PERSISTENCE_UNIT = "oracleNosqlBatchTestSizeTwenty";

    /** The emf. */
    private static EntityManagerFactory emf;

    /** The em. */
    private static EntityManager em;

    private List<PersonBatchOracleNosql> allPersons;

    private List<AddressBatchOracleNosql> allAddresses;

    @Before
    public void setUp() throws Exception
    {
        emf = Persistence.createEntityManagerFactory(PERSISTENCE_UNIT);
        em = emf.createEntityManager();
    }

    @Test
    public void onBatch()
    {
        List<PersonBatchOracleNosql> persons = preparePersonData(11);
        List<AddressBatchOracleNosql> addresses = prepareAddressData(11);

        // Insert Persons
        for (PersonBatchOracleNosql person : persons)
        {
            em.persist(person);
        }

        // Insert Addresses
        for (AddressBatchOracleNosql address : addresses)
        {
            em.persist(address);
        }

        // flush all on close.
        // explicit flush on close
        em.clear();
        em.close();

        em = emf.createEntityManager();

        // Query on Persons
        String personsQueryStr = " Select p from PersonBatchOracleNosql p";
        Query personsQuery = em.createQuery(personsQueryStr);
        allPersons = personsQuery.getResultList();
        Assert.assertNotNull(allPersons);
        Assert.assertEquals(11, allPersons.size());

        // Query on Addresses
        String addressQueryStr = " Select a from AddressBatchOracleNosql a";
        Query addressQuery = em.createQuery(addressQueryStr);
        allAddresses = addressQuery.getResultList();
        Assert.assertNotNull(allAddresses);
        Assert.assertEquals(11, allAddresses.size());

    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        for (PersonBatchOracleNosql entity : allPersons)
        {
            em.remove(entity);
        }
        for (AddressBatchOracleNosql entity : allAddresses)
        {
            em.remove(entity);
        }
        em.close();
        emf.close();
    }

    private List<PersonBatchOracleNosql> preparePersonData(Integer noOfRecords)
    {
        List<PersonBatchOracleNosql> persons = new ArrayList<PersonBatchOracleNosql>();
        for (int i = 1; i <= noOfRecords; i++)
        {
            PersonBatchOracleNosql o = new PersonBatchOracleNosql();
            o.setPersonId(i + "");
            o.setPersonName("Name " + i);
            o.setAge(10);
            persons.add(o);
        }
        return persons;
    }

    private List<AddressBatchOracleNosql> prepareAddressData(Integer noOfRecords)
    {
        List<AddressBatchOracleNosql> addresses = new ArrayList<AddressBatchOracleNosql>();
        for (int i = 1; i <= noOfRecords; i++)
        {
            AddressBatchOracleNosql o = new AddressBatchOracleNosql();
            o.setAddressId(i + "");
            o.setStreet("My Street " + i);
            addresses.add(o);
        }
        return addresses;
    }

}
