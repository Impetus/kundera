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
package com.impetus.client.crud.batch;


import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
import com.impetus.kundera.client.Client;
import com.impetus.kundera.persistence.api.Batcher;

/**
 * Batch processing test case for cassandra.
 * 
 * @author vivek.mishra
 *
 */
public class CassandraBatchProcessorTest
{

    /**
     * 
     */
    private static final String PERSISTENCE_UNIT = "secIdxBatchTest";

    /** The emf. */
    private static EntityManagerFactory emf;

    /** The em. */
    private static EntityManager em;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {

        // cassandraSetUp();
        CassandraCli.cassandraSetUp();
//        CassandraCli.initClient();
        CassandraCli.createKeySpace("KunderaExamples");
        loadData();

        emf = Persistence.createEntityManagerFactory(PERSISTENCE_UNIT);
        em = emf.createEntityManager();
    }

    @Test
    public void onBatch()
    {
        int counter = 0;
        List<PersonBatchCassandraEntity> rows = prepareData(10);
        for(PersonBatchCassandraEntity entity: rows)
        {
            em.persist(entity);
            
            // check for implicit flush.
            if(++counter == 5)
            {
                Map<String, Client> clients =  (Map<String, Client>) em.getDelegate();
                
                Batcher client = (Batcher) clients.get(PERSISTENCE_UNIT);
                Assert.assertEquals(5, client.getBatchSize());
                em.clear();
                for(int i = 0 ;i <5;i++)
                {
                    
                    // assert on each batch size record
                    Assert.assertNotNull(em.find(PersonBatchCassandraEntity.class, rows.get(i).getPersonId()));
                    
                    // as batch size is 5.
                    Assert.assertNull(em.find(PersonBatchCassandraEntity.class, rows.get(6).getPersonId()));
                }
                // means implicit flush must happen
            }
        }
        
        //flush all on close.
        // explicit flush on close
        em.clear();
        em.close();
        
        em = emf.createEntityManager();
        
        String sql = " Select p from PersonBatchCassandraEntity p";
        Query query = em.createQuery(sql);
        List<PersonBatchCassandraEntity> results = query.getResultList();
        Assert.assertNotNull(results);
        Assert.assertEquals(10, results.size());
    }
    
    
    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        emf.close();
        CassandraCli.dropKeySpace("KunderaExamples");
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
        user_Def.name = "PERSON_BATCH";
        user_Def.keyspace = "KunderaExamples";
        user_Def.setComparator_type("UTF8Type");
//        user_Def.setDefault_validation_class("UTF8Type");
        ColumnDef columnDef = new ColumnDef(ByteBuffer.wrap("PERSON_NAME".getBytes()), "UTF8Type");
        columnDef.index_type = IndexType.KEYS;
        user_Def.addToColumn_metadata(columnDef);
        ColumnDef columnDef1 = new ColumnDef(ByteBuffer.wrap("AGE".getBytes()), "IntegerType");
        columnDef1.index_type = IndexType.KEYS;
        user_Def.addToColumn_metadata(columnDef1);

        List<CfDef> cfDefs = new ArrayList<CfDef>();
        cfDefs.add(user_Def);

        try
        {
            ksDef = CassandraCli.client.describe_keyspace("KunderaExamples");
            CassandraCli.client.set_keyspace("KunderaExamples");

            List<CfDef> cfDefn = ksDef.getCf_defs();

            for (CfDef cfDef1 : cfDefn)
            {

                if (cfDef1.getName().equalsIgnoreCase("PERSON_BATCH"))
                {

                    CassandraCli.client.system_drop_column_family("PERSON_BATCH");

                }
            }
            CassandraCli.client.system_add_column_family(user_Def);

        }
        catch (NotFoundException e)
        {

            ksDef = new KsDef("KunderaExamples", "org.apache.cassandra.locator.SimpleStrategy", cfDefs);
            // Set replication factor
            if (ksDef.strategy_options == null)
            {
                ksDef.strategy_options = new LinkedHashMap<String, String>();
            }
            // Set replication factor, the value MUST be an integer
            ksDef.strategy_options.put("replication_factor", "1");

            CassandraCli.client.system_add_keyspace(ksDef);
        }

        CassandraCli.client.set_keyspace("KunderaExamples");

    }


    private List<PersonBatchCassandraEntity> prepareData(Integer noOfRecords)
    {
        List<PersonBatchCassandraEntity> persons = new ArrayList<PersonBatchCassandraEntity>();
        for(int i=1 ; i<=noOfRecords;i++)
        {
            PersonBatchCassandraEntity o = new PersonBatchCassandraEntity();
            o.setPersonId(i+"");
            o.setPersonName("vivek" + i);
            o.setAge(10);
            persons.add(o);
        }
        
        return persons;
    }

}
