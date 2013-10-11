package com.impetus.client;

import javax.persistence.Query;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.client.crud.mappedsuperclass.MappedSuperClassBase;
import com.impetus.kundera.metadata.model.KunderaMetadata;

public class RedisMappedSuperClassTest extends MappedSuperClassBase
{

    @Before
    public void setUp() throws Exception
    {
        _PU = "redis_pu";
        setUpInternal();
    }
    
    @Test
    public void test()
    {
        assertInternal(true);
    }

    
    @After
    public void tearDown() throws Exception
    {
        // Delete by query.
        String deleteQuery = "Delete from CreditTransaction p";
        
        Query query = em.createQuery(deleteQuery);
        query.executeUpdate();

        deleteQuery = "Delete from DebitTransaction p";
        
        query = em.createQuery(deleteQuery);
        query.executeUpdate();

        tearDownInternal();
        KunderaMetadata.INSTANCE.setApplicationMetadata(null);

    }
}
