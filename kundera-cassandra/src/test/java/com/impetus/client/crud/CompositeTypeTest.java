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
package com.impetus.client.crud;


import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.entity.CompositeTypeExample;
import com.impetus.client.entity.EmbeddedCompositeType;
import com.impetus.client.persistence.CassandraCli;

/**
 * @author vivek.mishra
 *
 */
public class CompositeTypeTest
{
    private EntityManagerFactory entityManagerFactory;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        CassandraCli.cassandraSetUp();
        CassandraCli.createKeySpace("KunderaExamples");
        entityManagerFactory = Persistence.createEntityManagerFactory("secIdxCassandraTest");
//        entityManager = 
    }

    @Test
    public void testDummy()
    {
        // just a dummy test . Uncomment given below test once compound primary key support is in place.
    }
//    @Test
    public void testRun()
    {
        EntityManager em = entityManagerFactory.createEntityManager();
        CompositeTypeExample o = new CompositeTypeExample();
        EmbeddedCompositeType ec = new EmbeddedCompositeType();
        ec.setKey1(1);
        ec.setKey2("2");
        ec.setKey3(3d);
        o.setId(ec);
        em.persist(o);
        em.clear();  
    }
    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        CassandraCli.dropKeySpace("KunderaExamples");
        entityManagerFactory.close();
    }

}
