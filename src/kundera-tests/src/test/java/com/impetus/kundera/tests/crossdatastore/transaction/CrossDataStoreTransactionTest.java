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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

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
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.persistence.CassandraCli;
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

        CassandraCli.cassandraSetUp();
        CassandraCli.createKeySpace("KunderaTests");
        loadData();
        loadDataForHABITAT();

        emf = Persistence.createEntityManagerFactory("secIdxAddCassandra,addMongo");
        em = emf.createEntityManager();
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
     * @throws TException
     * @throws InvalidRequestException
     * @throws SchemaDisagreementException
     * 
     */
    private void loadData() throws InvalidRequestException, TException, SchemaDisagreementException
    {

        KsDef ksDef = null;

        CfDef cfDef = new CfDef();
        cfDef.name = "PERSONNEL";
        cfDef.keyspace = "KunderaTests";
        // cfDef.column_type = "Super";
        cfDef.setComparator_type("UTF8Type");
        cfDef.setDefault_validation_class("UTF8Type");
        ColumnDef columnDefPersonName = new ColumnDef(ByteBuffer.wrap("PERSON_NAME".getBytes()), "UTF8Type");
        columnDefPersonName.index_type = IndexType.KEYS;

        ColumnDef columnDefAddressId = new ColumnDef(ByteBuffer.wrap("ADDRESS_ID".getBytes()), "UTF8Type");
        columnDefAddressId.index_type = IndexType.KEYS;

        cfDef.addToColumn_metadata(columnDefPersonName);
        cfDef.addToColumn_metadata(columnDefAddressId);

        List<CfDef> cfDefs = new ArrayList<CfDef>();
        cfDefs.add(cfDef);

        try
        {
            ksDef = CassandraCli.client.describe_keyspace("KunderaTests");
            CassandraCli.client.set_keyspace("KunderaTests");
            if (!CassandraCli.columnFamilyExist("PERSONNEL", "KunderaTests")) {
                CassandraCli.client.system_add_column_family(cfDef);
            } else {
                CassandraCli.truncateColumnFamily("KunderaTests", "PERSONNEL");
            }

//            List<CfDef> cfDefn = ksDef.getCf_defs();
//
//            // CassandraCli.client.set_keyspace("KunderaTests");
//            for (CfDef cfDef1 : cfDefn)
//            {
//
//                if (cfDef1.getName().equalsIgnoreCase("PERSONNEL"))
//                {
//
//                    CassandraCli.truncateColumnFamily("KunderaTests", "PERSONNEL");
//
//                } else{
//                    CassandraCli.client.system_add_column_family(cfDef);
//                }
//            }
//            

        }
        catch (NotFoundException e)
        {
            addKeyspace(ksDef, cfDefs);
        }

        CassandraCli.client.set_keyspace("KunderaTests");

    }

    private void addKeyspace(KsDef ksDef, List<CfDef> cfDefs) throws InvalidRequestException,
            SchemaDisagreementException, TException
    {
        ksDef = new KsDef("KunderaTests", SimpleStrategy.class.getSimpleName(), cfDefs);
        // Set replication factor
        if (ksDef.strategy_options == null)
        {
            ksDef.strategy_options = new LinkedHashMap<String, String>();
        }
        // Set replication factor, the value MUST be an integer
        ksDef.strategy_options.put("replication_factor", "1");
        CassandraCli.client.system_add_keyspace(ksDef);
    }

    private void loadDataForHABITAT() throws TException, InvalidRequestException, UnavailableException,
            TimedOutException, SchemaDisagreementException
    {

        KsDef ksDef = null;
        CfDef cfDef2 = new CfDef();
        cfDef2.name = "ADDRESS";
        cfDef2.keyspace = "KunderaTests";

        ColumnDef columnDef2 = new ColumnDef(ByteBuffer.wrap("STREET".getBytes()), "UTF8Type");
        columnDef2.index_type = IndexType.KEYS;
        cfDef2.addToColumn_metadata(columnDef2);

        List<CfDef> cfDefs = new ArrayList<CfDef>();
        cfDefs.add(cfDef2);

        try
        {
            ksDef = CassandraCli.client.describe_keyspace("KunderaTests");
            CassandraCli.client.set_keyspace("KunderaTests");
            if (!CassandraCli.columnFamilyExist("ADDRESS", "KunderaTests")) {
                CassandraCli.client.system_add_column_family(cfDef2);
            } else {
                CassandraCli.truncateColumnFamily("KunderaTests", "ADDRESS");
            }
//            List<CfDef> cfDefss = ksDef.getCf_defs();
//            for (CfDef cfDef : cfDefss)
//            {
//
//                if (cfDef.getName().equalsIgnoreCase("ADDRESS"))
//                {
//                    CassandraCli.truncateColumnFamily("KunderaTests", "ADDRESS");
//
//                } else{
//                    CassandraCli.client.system_add_column_family(cfDef2);
//                }
//            }
            
        }
        catch (NotFoundException e)
        {
            addKeyspace(ksDef, cfDefs);
        }
        CassandraCli.client.set_keyspace("KunderaTests");

    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        //CassandraCli.dropKeySpace("KunderaTests");
        CassandraCli.truncateColumnFamily("KunderaTests", "ADDRESS", "PERSONNEL");
    }

}
