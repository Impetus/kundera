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
package com.impetus.client.crud.batch;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

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
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.persistence.CassandraCli;

/**
 * Test case for more than one entities persisted in the same batch
 * 
 * @author amresh.singh
 */
public class CassandraBatchProcessorMixedTest
{

    private static final String CF_ADDRESS_BATCH = "ADDRESS_BATCH";

    private static final String CF_PERSON_BATCH = "PERSON_BATCH";

    private static final String KEYSPACE_KUNDERA_EXAMPLES = "KunderaExamples";

    private static final String PERSISTENCE_UNIT = "batchTestSizeTwenty";

    /** The emf. */
    private static EntityManagerFactory emf;

    /** The em. */
    private static EntityManager em;

    @Before
    public void setUp() throws Exception
    {

        CassandraCli.cassandraSetUp();
        CassandraCli.createKeySpace(KEYSPACE_KUNDERA_EXAMPLES);
        loadData();

        emf = Persistence.createEntityManagerFactory(PERSISTENCE_UNIT);
        em = emf.createEntityManager();
    }

    @Test
    public void onBatch()
    {
        List<PersonBatch> persons = preparePersonData(10);
        List<AddressBatch> addresses = prepareAddressData(10);

        // Insert Persons
        for (PersonBatch person : persons)
        {
            em.persist(person);
        }

        // Insert Addresses
        for (AddressBatch address : addresses)
        {
            em.persist(address);
        }

        // flush all on close.
        // explicit flush on close
        em.clear();
        em.close();

        em = emf.createEntityManager();

        // Query on Persons
        String personsQueryStr = " Select p from PersonBatch p";
        Query personsQuery = em.createQuery(personsQueryStr);
        List<PersonBatch> allPersons = personsQuery.getResultList();
        Assert.assertNotNull(allPersons);
        Assert.assertEquals(10, allPersons.size());

        // Query on Addresses
        String addressQueryStr = " Select a from AddressBatch a";
        Query addressQuery = em.createQuery(addressQueryStr);
        List<AddressBatch> allAddresses = addressQuery.getResultList();
        Assert.assertNotNull(allAddresses);
        Assert.assertEquals(10, allAddresses.size());

    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        emf.close();
        CassandraCli.dropKeySpace(KEYSPACE_KUNDERA_EXAMPLES);
    }

    /**
     * Load cassandra specific data.
     * 
     * @throws TException
     *             the t exception
     * @throws InvalidRequestException
     *             the invalid request exception
     * @throws UnavailableException
     *             the unavailable exception
     * @throws TimedOutException
     *             the timed out exception
     * @throws SchemaDisagreementException
     *             the schema disagreement exception
     */
    private void loadData() throws TException, InvalidRequestException, UnavailableException, TimedOutException,
            SchemaDisagreementException
    {

        KsDef ksDef = null;

        CfDef user_Def = new CfDef();
        user_Def.name = CF_PERSON_BATCH;
        user_Def.keyspace = KEYSPACE_KUNDERA_EXAMPLES;
        user_Def.setComparator_type("UTF8Type");
        user_Def.setKey_validation_class("UTF8Type");
        ColumnDef columnDef = new ColumnDef(ByteBuffer.wrap("PERSON_NAME".getBytes()), "UTF8Type");
        columnDef.index_type = IndexType.KEYS;
        user_Def.addToColumn_metadata(columnDef);
        ColumnDef columnDef1 = new ColumnDef(ByteBuffer.wrap("AGE".getBytes()), "Int32Type");
        columnDef1.index_type = IndexType.KEYS;
        user_Def.addToColumn_metadata(columnDef1);
        ColumnDef columnDef2 = new ColumnDef(ByteBuffer.wrap("AGEss".getBytes()), "BytesType");
        columnDef2.index_type = IndexType.KEYS;
        user_Def.addToColumn_metadata(columnDef2);

        CfDef address_Def = new CfDef();
        address_Def.name = CF_ADDRESS_BATCH;
        address_Def.keyspace = KEYSPACE_KUNDERA_EXAMPLES;
        address_Def.setComparator_type("UTF8Type");
        address_Def.setKey_validation_class("UTF8Type");
        ColumnDef columnDefStreet = new ColumnDef(ByteBuffer.wrap("STREET".getBytes()), "UTF8Type");
        columnDefStreet.index_type = IndexType.KEYS;
        address_Def.addToColumn_metadata(columnDefStreet);

        List<CfDef> cfDefs = new ArrayList<CfDef>();
        cfDefs.add(user_Def);
        cfDefs.add(address_Def);

        try
        {
            ksDef = CassandraCli.client.describe_keyspace(KEYSPACE_KUNDERA_EXAMPLES);
            CassandraCli.client.set_keyspace(KEYSPACE_KUNDERA_EXAMPLES);

            List<CfDef> cfDefn = ksDef.getCf_defs();

            for (CfDef cfDef1 : cfDefn)
            {

                if (cfDef1.getName().equalsIgnoreCase(CF_PERSON_BATCH))
                {
                    CassandraCli.client.system_drop_column_family(CF_PERSON_BATCH);
                }
                else if (cfDef1.getName().equalsIgnoreCase(CF_ADDRESS_BATCH))
                {
                    CassandraCli.client.system_drop_column_family(CF_ADDRESS_BATCH);
                }
            }
            CassandraCli.client.system_add_column_family(user_Def);
            CassandraCli.client.system_add_column_family(address_Def);

        }
        catch (NotFoundException e)
        {

            ksDef = new KsDef(KEYSPACE_KUNDERA_EXAMPLES, "org.apache.cassandra.locator.SimpleStrategy", cfDefs);
            // Set replication factor
            if (ksDef.strategy_options == null)
            {
                ksDef.strategy_options = new LinkedHashMap<String, String>();
            }
            // Set replication factor, the value MUST be an integer
            ksDef.strategy_options.put("replication_factor", "1");

            CassandraCli.client.system_add_keyspace(ksDef);
        }

        CassandraCli.client.set_keyspace(KEYSPACE_KUNDERA_EXAMPLES);

    }

    private List<PersonBatch> preparePersonData(Integer noOfRecords)
    {
        List<PersonBatch> persons = new ArrayList<PersonBatch>();
        for (int i = 1; i <= noOfRecords; i++)
        {
            PersonBatch o = new PersonBatch();
            o.setPersonId(i + "");
            o.setPersonName("Name " + i);
            o.setAge(10);
            persons.add(o);
        }
        return persons;
    }

    private List<AddressBatch> prepareAddressData(Integer noOfRecords)
    {
        List<AddressBatch> addresses = new ArrayList<AddressBatch>();
        for (int i = 1; i <= noOfRecords; i++)
        {
            AddressBatch o = new AddressBatch();
            o.setAddressId(i + "");
            o.setStreet("My Street " + i);
            addresses.add(o);
        }
        return addresses;
    }

}
