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
package com.impetus.kundera.index;

import java.util.List;
import java.util.Map;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;
import com.impetus.kundera.query.Person;
import com.impetus.kundera.query.Person.Day;
import com.impetus.kundera.utils.LuceneCleanupUtilities;

/**
 * Junit for {@link LuceneIndexer}
 * 
 * @author vivek.mishra
 * 
 */
public class LuceneIndexerTest
{
    private EntityManagerFactory emf;

    private static final String LUCENE_DIR_PATH = "./lucene";

    @Before
    public void setup()
    {
        emf = Persistence.createEntityManagerFactory("patest");
    }

    @Test
    public void testGetInstance()
    {
        LuceneIndexer indexer = LuceneIndexer.getInstance(LUCENE_DIR_PATH);
        Assert.assertNotNull(indexer);
        indexer.close();
    }

    @Test
    public void testSearchWithNoResult()
    {
        EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(
                ((EntityManagerFactoryImpl) emf).getKunderaMetadataInstance(), Person.class);
        
        LuceneIndexer indexer = LuceneIndexer.getInstance(LUCENE_DIR_PATH);
        Assert.assertNotNull(indexer);

        String luceneQuery = LuceneQueryUtils.getQuery("addressId", "address", "addressId", "1");

        try
        {
            indexer.search(luceneQuery, 0, 10, false, ((EntityManagerFactoryImpl) emf).getKunderaMetadataInstance(), metadata);
        }
        catch (LuceneIndexingException liex)
        {
            Assert.assertNotNull(liex.getMessage()); // as there is no index
                                                     // directory created.
        }

        indexer.close();
    }

    @Test
    public void invalidLuceneQueryTest()
    {
        LuceneIndexer indexer = LuceneIndexer.getInstance(LUCENE_DIR_PATH);

        EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(
                ((EntityManagerFactoryImpl) emf).getKunderaMetadataInstance(), Person.class);
        Person p = new Person();
        p.setAge(32);
        p.setDay(Day.TUESDAY);
        p.setPersonId("p1");
        indexer.index(metadata, (MetamodelImpl) ((EntityManagerFactoryImpl) emf).getKunderaMetadataInstance()
                .getApplicationMetadata().getMetamodel("patest"), p);
        Assert.assertNotNull(indexer);

        final String luceneQuery = "Invalid lucene query";
        try
        {
            indexer.search(luceneQuery, 0, 10, false, ((EntityManagerFactoryImpl) emf).getKunderaMetadataInstance(), metadata);
        }
        catch (LuceneIndexingException liex)
        {
            Assert.assertEquals("Error while parsing Lucene Query " + luceneQuery, liex.getMessage());
        }

        indexer.close();
    }

    @Test
    public void invalidValidQueryTest()
    {
        LuceneIndexer indexer = LuceneIndexer.getInstance(LUCENE_DIR_PATH);

        EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(
                ((EntityManagerFactoryImpl) emf).getKunderaMetadataInstance(), Person.class);
        Person p = new Person();
        p.setAge(32);
        p.setDay(Day.TUESDAY);
        p.setPersonId("p1");
        indexer.index(metadata, (MetamodelImpl) ((EntityManagerFactoryImpl) emf).getKunderaMetadataInstance()
                .getApplicationMetadata().getMetamodel("patest"), p);

        indexer.flush();
        Assert.assertNotNull(indexer);

        String luceneQuery = "+Person.AGE:32 AND +entity.class:com.impetus.kundera.query.Person";

        try
        {
            Map<String, Object> results = indexer.search(luceneQuery, 0, 10, false, ((EntityManagerFactoryImpl) emf).getKunderaMetadataInstance(), metadata);
            Assert.assertTrue(!results.isEmpty());
        }
        catch (LuceneIndexingException liex)
        {
            Assert.fail();
        }

        indexer.close();
    }

    @Test
    public void testOnUnsupportedMethods()
    {
        String luceneQuery = "+Person.AGE:32 AND +entity.class:com.impetus.kundera.query.Person";
        Indexer indexer = LuceneIndexer.getInstance(LUCENE_DIR_PATH);
        EntityMetadata m = KunderaMetadataManager.getEntityMetadata(
                ((EntityManagerFactoryImpl) emf).getKunderaMetadataInstance(), Person.class);
        try
        {
            indexer.index(Person.class, m, null, null, null);
            Assert.fail("Should have gone to catch block!");
        }
        catch (UnsupportedOperationException uoex)
        {
            Assert.assertNotNull(uoex);
        }

        try
        {
            indexer.search(m.getEntityClazz(), m, luceneQuery, 0, 100);
            Assert.fail("Should have gone to catch block!");
        }
        catch (UnsupportedOperationException uoex)
        {
            Assert.assertNotNull(uoex);
        }

        try
        {
            indexer.unIndex(Person.class, null, m, (MetamodelImpl) ((EntityManagerFactoryImpl) emf)
                    .getKunderaMetadataInstance().getApplicationMetadata().getMetamodel("patest"));
            Assert.fail("Should have gone to catch block!");
        }
        catch (UnsupportedOperationException uoex)
        {
            Assert.assertNotNull(uoex);
        }

    }

    @After
    public void tearDown()
    {
        LuceneCleanupUtilities.cleanDir(LUCENE_DIR_PATH);
    }

}
