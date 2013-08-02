package com.impetus.kundera.index;

import java.util.Map;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.util.Version;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.query.Person;
import com.impetus.kundera.query.Person.Day;
import com.impetus.kundera.utils.LuceneCleanupUtilities;

public class LuceneIndexerTest
{
    private EntityManagerFactory emf;
    
    private static final String LUCENE_DIR_PATH = "./lucene";
//    private EntityManager em;
  
    @Before
    public void setup()
    {
        emf = Persistence.createEntityManagerFactory("patest");
//        em = emf.createEntityManager();
    }
    
    @Test
    public void testGetInstance()
    {
        LuceneIndexer indexer = LuceneIndexer.getInstance(new StandardAnalyzer(Version.LUCENE_34), LUCENE_DIR_PATH);
        Assert.assertNotNull(indexer);
        indexer.close();
    }

    @Test
    public void testSearchWithNoResult()
    {
        LuceneIndexer indexer = LuceneIndexer.getInstance(new StandardAnalyzer(Version.LUCENE_34), LUCENE_DIR_PATH);
        Assert.assertNotNull(indexer);
        
        String luceneQuery = LuceneQueryUtils.getQuery("addressId", "address", "addressId", "1");
        
        try
        {
            indexer.search(luceneQuery, 0, 10,false);
        }catch(LuceneIndexingException liex)
        {
            Assert.assertNotNull(liex.getMessage()); // as there is no index directory created.
        }
        
        indexer.close();
    }

    @Test
    public void invalidLuceneQueryTest()
    {
        LuceneIndexer indexer = LuceneIndexer.getInstance(new StandardAnalyzer(Version.LUCENE_34), LUCENE_DIR_PATH);
        
        EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(Person.class);
        Person p = new Person();
        p.setAge(32);
        p.setDay(Day.TUESDAY);
        p.setPersonId("p1");
        indexer.index(metadata, p);
        Assert.assertNotNull(indexer);
        
        final String luceneQuery = "Invalid lucene query";
        try
        {
            indexer.search(luceneQuery, 0, 10,false);
        }catch(LuceneIndexingException liex)
        {
            Assert.assertEquals("Error while parsing Lucene Query " + luceneQuery, liex.getMessage());
        }
        
        indexer.close();
    }

    @Test
    public void invalidValidQueryTest()
    {
        LuceneIndexer indexer = LuceneIndexer.getInstance(new StandardAnalyzer(Version.LUCENE_34), LUCENE_DIR_PATH);
        
        EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(Person.class);
        Person p = new Person();
        p.setAge(32);
        p.setDay(Day.TUESDAY);
        p.setPersonId("p1");
        indexer.index(metadata, p);
        
        indexer.flush();
        Assert.assertNotNull(indexer);
        
        String luceneQuery = "+Person.AGE:32 AND +entity.class:com.impetus.kundera.query.Person";

        try
        {
            Map<String, Object> results = indexer.search(luceneQuery, 0, 10,false);
            Assert.assertTrue(!results.isEmpty());
        }catch(LuceneIndexingException liex)
        {
            Assert.fail();
        }
        
        indexer.close();
    }
    
    @After
    public void tearDown()
    {
        LuceneCleanupUtilities.cleanDir(LUCENE_DIR_PATH);
    }

}
