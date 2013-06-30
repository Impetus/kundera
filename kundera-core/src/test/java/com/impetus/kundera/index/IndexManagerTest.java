package com.impetus.kundera.index;

import java.util.Map;

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

public class IndexManagerTest
{

    private static final String LUCENE_DIR_PATH = "./lucene";

    @Before
    public void setup()
    {
        Persistence.createEntityManagerFactory("patest");
    }

    @Test
    public void testCRUD()
    {
        LuceneIndexer indexer = LuceneIndexer.getInstance(new StandardAnalyzer(Version.LUCENE_34), LUCENE_DIR_PATH);
        IndexManager ixManager = new IndexManager(indexer);

        EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(Person.class);
        Person p = new Person();
        p.setAge(32);
        p.setDay(Day.TUESDAY);
        p.setPersonId("p1");

        Assert.assertNotNull(ixManager.getIndexer());
        Assert.assertEquals(indexer, ixManager.getIndexer());
        
        ixManager.write(metadata, p);

        String luceneQuery = "+Person.AGE:32 AND +entity.class:com.impetus.kundera.query.Person";

        try
        {
            Map<String, Object> results = ixManager.search(luceneQuery, 0, 10, false);
            Assert.assertTrue(!results.isEmpty());
        }
        catch (LuceneIndexingException liex)
        {
            Assert.fail();
        }

        p.setAge(35);
        
        ixManager.update(metadata, p, null, Person.class);
        
        luceneQuery = "+Person.AGE:35 AND +entity.class:com.impetus.kundera.query.Person";
        
        try
        {
            Map<String, Object> results = ixManager.search(luceneQuery,1);
            Assert.assertTrue(!results.isEmpty());
        }
        catch (LuceneIndexingException liex)
        {
            Assert.fail();
        }
        
        // Remove indexes.
        ixManager.remove(metadata, p, "p1");
        
        luceneQuery = "+Person.AGE:32 AND +entity.class:com.impetus.kundera.query.Person";
        
        try
        {
            Map<String, Object> results = ixManager.search(luceneQuery, 0, 10, false);
            Assert.assertTrue(results.isEmpty());
        }
        catch (LuceneIndexingException liex)
        {
            Assert.fail();
        }
    }

    @After
    public void tearDown()
    {
        LuceneCleanupUtilities.cleanDir(LUCENE_DIR_PATH);
    }

}
