/*******************************************************************************
 *  * Copyright 2013 Impetus Infotech.
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
package com.impetus.kundera.query;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.metadata.model.KunderaMetadata;

/**
 * @author vivek.mishra
 *  Junit test case for invalid lucene query scenarios. Rest are tested via EM and persistence delegator.
 */
public class LuceneQueryTest
{

    private static final String PU = "patest";

    private EntityManagerFactory emf;

    private EntityManager em;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        KunderaMetadata.INSTANCE.setApplicationMetadata(null);        
        emf = Persistence.createEntityManagerFactory(PU);
        em = emf.createEntityManager();

    }

    @Test
    public void test()
    {
        String query = "Select p from Person p";
        KunderaQuery kunderaQuery = new KunderaQuery();
        KunderaQueryParser queryParser = new KunderaQueryParser(kunderaQuery, query);
        queryParser.parse();
        kunderaQuery.postParsingInit();

        LuceneQuery luceneQuery = new LuceneQuery(query,kunderaQuery,null);
        
        try
        {
            luceneQuery.populateEntities(null, null);
        }catch(UnsupportedOperationException uoex)
        {
            Assert.assertEquals("Method not supported for Lucene indexing", uoex.getMessage());
        }
        
        try
        {
            luceneQuery.recursivelyPopulateEntities(null, null);
        }catch(UnsupportedOperationException uoex)
        {
            Assert.assertEquals("Method not supported for Lucene indexing", uoex.getMessage());
        }
        
        try
        {
            luceneQuery.getReader();
        }catch(UnsupportedOperationException uoex)
        {
            Assert.assertEquals("Method not supported for Lucene indexing", uoex.getMessage());
        }
     
        Assert.assertEquals(0,luceneQuery.onExecuteUpdate());
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

}
