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
package com.impetus.client.cassandra.generatedId;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.cassandra.generatedId.entites.CassandraGeneratedIdDefault;
import com.impetus.client.cassandra.generatedId.entites.CassandraGeneratedIdStrategyAuto;
import com.impetus.kundera.client.cassandra.persistence.CassandraCli;

/**
 * Cassandra generated id junit.
 * 
 * @author vivek.mishra
 *
 */
public class CassandraGeneratedIdTest
{
    private EntityManagerFactory emf;

    @Before
    public void setUp() throws Exception
    {
        CassandraCli.cassandraSetUp();
        CassandraCli.createKeySpace("KunderaExamples");
        emf = Persistence.createEntityManagerFactory("ds_pu");
    }

    @After
    public void tearDown() throws Exception
    {
        CassandraCli.dropKeySpace("KunderaExamples");
        emf.close();
    }

    @Test
    public void testPersist()
    {
        EntityManager em = emf.createEntityManager();

        CassandraGeneratedIdDefault idDefault = new CassandraGeneratedIdDefault();
        idDefault.setName("kuldeep");
        em.persist(idDefault);
        CassandraGeneratedIdStrategyAuto strategyAuto = new CassandraGeneratedIdStrategyAuto();
        strategyAuto.setName("kuldeep");
        em.persist(strategyAuto);
    }
}
