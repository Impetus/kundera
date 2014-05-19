/*******************************************************************************
 * * Copyright 2014 Impetus Infotech.
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

package com.impetus.client.neo4j.imdb;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.kernel.impl.util.FileUtils;

/**
 * 
 * @author Kuldeep.Mishra
 * 
 */
public class SimpleCRUDTest
{
    private EntityManagerFactory emf;

    private EntityManager em;

    private static final String IMDB_PU = "neo4jTest";

    private String datastoreFilePath = "target/newpath.db";

    @Before
    public void setup() throws Exception
    {
        Map<String, String> propertyMap = new HashMap<String, String>();
        propertyMap.put("kundera.datastore.file.path", datastoreFilePath);

        emf = Persistence.createEntityManagerFactory(IMDB_PU, propertyMap);
        em = emf.createEntityManager();
    }

    // @Test
    public void testDummy()
    {

    }

    @Test
    public void should_save_entity() throws Exception
    {
        Actor actor = new Actor();
        actor.setId(1);
        actor.setName("Tom cruise");

        em.getTransaction().begin();

        em.persist(actor);

        em.getTransaction().commit();

        em.clear();

        Actor foundActor = em.find(Actor.class, 1);
        Assert.assertNotNull(foundActor);
        Assert.assertEquals("Tom cruise", foundActor.getName());
        Assert.assertEquals(1, foundActor.getId());

    }

    @After
    public void tearDown() throws Exception
    {
        em.close();
        emf.close();

        if (datastoreFilePath != null)
        {
            FileUtils.deleteRecursively(new File(datastoreFilePath));
        }
    }
}
