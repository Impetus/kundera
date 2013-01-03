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
package com.impetus.client.neo4j.imdb;


import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.metamodel.Metamodel;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;

/**
 * Test case for validating correctness of Metamodel for Map data type 
 * @author amresh.singh
 */
public class IMDBMapMetamodelTest
{

    EntityManagerFactory emf;
    EntityManager em;   
    


    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        emf = Persistence.createEntityManagerFactory("imdb");
        em = emf.createEntityManager();        
        
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        em.close();
        emf.close();
    }
    
    @Test
    public void testMetamodel()
    {       
        EntityMetadata m1 = KunderaMetadataManager.getEntityMetadata(Actor.class);
        Assert.assertNotNull(m1);
        Assert.assertEquals(Actor.class, m1.getEntityClazz());
        Assert.assertEquals(Role.class, m1.getRelation("movies").getMapKeyJoinClass());
        Assert.assertEquals(Movie.class, m1.getRelation("movies").getTargetEntity());
        Assert.assertEquals("ACTS_IN", m1.getRelation("movies").getJoinColumnName());
        
        
        EntityMetadata m2 = KunderaMetadataManager.getEntityMetadata(Movie.class);
        Assert.assertNotNull(m2);
        Assert.assertEquals(Movie.class, m2.getEntityClazz());
        Assert.assertEquals(Role.class, m2.getRelation("actors").getMapKeyJoinClass());
        Assert.assertEquals(Actor.class, m2.getRelation("actors").getTargetEntity());
        Assert.assertNull(m2.getRelation("actors").getJoinColumnName());
        
        EntityMetadata m3 = KunderaMetadataManager.getEntityMetadata(Role.class);
        Assert.assertNotNull(m3);
        Assert.assertEquals(Role.class, m3.getEntityClazz());        
        
        
        Metamodel mm = KunderaMetadataManager.getMetamodel("imdb");
        Assert.assertNotNull(mm);    
        
    }   

}
