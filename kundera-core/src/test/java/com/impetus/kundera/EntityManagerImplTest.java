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
package com.impetus.kundera;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.metadata.entities.SampleEntity;
import com.impetus.kundera.metadata.model.KunderaMetadata;

/**
 * @author vivek.mishra
 * 
 */
public class EntityManagerImplTest
{

    private EntityManager em;

    /** The log. */
    private static Logger log = LoggerFactory.getLogger(EntityManagerImplTest.class);

    private EntityManagerFactory emf;

    @Before
    public void setUp()
    {
        KunderaMetadata.INSTANCE.unloadKunderaMetadata("kunderatest");
        emf = Persistence.createEntityManagerFactory("kunderatest");

        em = emf.createEntityManager();
    }

    /**
     * On test persist.
     * 
     */
    @Test
    public void testPersist()
    {
        long t1 = System.currentTimeMillis();

        try
        {
            for (int i = 1; i <= 1000000; i++)
            {
                final SampleEntity entity = new SampleEntity();
                entity.setKey(i);
                entity.setName("name" + i);
                if (i % 5000 == 0)
                {
                    em.clear();
                }
                
                em.persist(entity);

            }
            long t2 = System.currentTimeMillis();
            System.out.println("Time taken for 1 million dummy insert: "+ (t2 - t1));
        }
        catch (Exception e)
        {
            Assert.fail(e.getMessage());
        }
    }

    @After
    public void tearDown()
    {
        if (em != null)
            em.close();
        if (emf != null)
            emf.close();

    }
}
