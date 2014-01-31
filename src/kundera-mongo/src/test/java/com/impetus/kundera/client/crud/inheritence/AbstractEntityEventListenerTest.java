/*******************************************************************************
 * * Copyright 2013 Impetus Infotech.
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
package com.impetus.kundera.client.crud.inheritence;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.persistence.event.EntityEventDispatcher;

/**
 * Test case for {@link AbstractEntityEventListenerTest}
 * 
 * @author chhavi.gangwal
 * 
 */
public class AbstractEntityEventListenerTest
{

    private EntityEventDispatcher eventDispatcher;

    private EntityManager em;

    private EntityManagerFactory emf;

    private static String _PU = "mongoTest";

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        eventDispatcher = new EntityEventDispatcher();
        
        emf = Persistence.createEntityManagerFactory(_PU);
        em = emf.createEntityManager();
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        eventDispatcher = null;
        em.close();
        emf.close();
    }

    /**
     * Test method for
     * {@link com.impetus.kundera.persistence.event.EntityEventDispatcher#fireEventListeners(com.impetus.kundera.metadata.model.EntityMetadata, java.lang.Object, java.lang.Class)}
     * .
     * 
     * @throws SecurityException
     * @throws NoSuchMethodException
     */
    @Test
    public void testEventListeners() throws NoSuchMethodException, SecurityException
    {
        EventEntity entity = new EventEntity();
        entity.setId(10L);
        em.persist(entity);

        // +2 for @PrePersist -1 for @PostPersist
        Assert.assertEquals(1, entity.invocationCounter);
        
        EventEntity found = em.find(EventEntity.class, 10L);
        found.setEntityType("Updated");
        found.invocationCounter = 0;
        em.merge(found);

        found = em.find(EventEntity.class, 10L);

        // +1 for @PreUpdate +1 for @PostUpdate
        Assert.assertEquals(2, entity.invocationCounter);

    }

}
