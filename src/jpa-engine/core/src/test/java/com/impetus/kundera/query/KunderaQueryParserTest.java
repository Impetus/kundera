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
package com.impetus.kundera.query;

import java.util.List;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.persistence.EntityManagerFactoryImpl;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.query.KunderaQuery.SortOrder;
import com.impetus.kundera.query.KunderaQuery.SortOrdering;
import com.impetus.kundera.query.KunderaQuery.UpdateClause;

/**
 * The Class KunderaQueryParserTest.
 * 
 * @author vivek.mishra
 */
public class KunderaQueryParserTest
{

    private EntityManagerFactory emf;

    private KunderaMetadata kunderaMetadata;

    /**
     * Sets the up.
     * 
     * @throws Exception
     *             the exception
     */
    @Before
    public void setUp() throws Exception
    {
        emf = Persistence.createEntityManagerFactory("kunderatest");
        kunderaMetadata = ((EntityManagerFactoryImpl) emf).getKunderaMetadataInstance();
    }

    /**
     * On query parse.
     */
    @Test
    public void onQueryParse()
    {

        // Valid Query with clause.
        String validQuery = "SELECT c FROM Country c ORDER BY c.currency, c.population DESC";
        KunderaQuery kunderQuery = new KunderaQuery(validQuery, kunderaMetadata);

        KunderaQueryParser parser = new KunderaQueryParser(kunderQuery);
        parser.parse();

        List<SortOrdering> sortOrders = kunderQuery.getOrdering();
        Assert.assertNotNull(sortOrders);
        Assert.assertEquals(2, sortOrders.size());
        Assert.assertEquals("c.currency", sortOrders.get(0).getColumnName());
        Assert.assertEquals(SortOrder.ASC.name(), sortOrders.get(0).getOrder().name());
        Assert.assertEquals("c.population", sortOrders.get(1).getColumnName());
        Assert.assertEquals(SortOrder.DESC.name(), sortOrders.get(1).getOrder().name());

        // valid query with default ASC clause.
        String validQueryWithDefaultClause = "SELECT c FROM Country c ORDER BY c.currency, c.population";

        kunderQuery = new KunderaQuery(validQueryWithDefaultClause, kunderaMetadata);
        parser = new KunderaQueryParser(kunderQuery);
        parser.parse();

        sortOrders = kunderQuery.getOrdering();
        Assert.assertNotNull(sortOrders);
        Assert.assertEquals(2, sortOrders.size());
        Assert.assertEquals("c.currency", sortOrders.get(0).getColumnName());
        Assert.assertEquals(SortOrder.ASC.name(), sortOrders.get(0).getOrder().name());
        Assert.assertEquals("c.population", sortOrders.get(1).getColumnName());
        Assert.assertEquals(SortOrder.ASC.name(), sortOrders.get(1).getOrder().name());

        String invalidQuery = "SELECT c FROM Country c where currency, c.population DESCS";

        kunderQuery = new KunderaQuery(invalidQuery, kunderaMetadata);
        parser = new KunderaQueryParser(kunderQuery);
        parser.parse();

    }

    @Test
    public void onUpdateDeleteQueryParse()
    {

        // update with single set value in SET clause.
        String updateQuery = "UPDATE Country SET population = 10 where currency = INR";

        KunderaQuery kunderaQuery = new KunderaQuery(updateQuery, kunderaMetadata);
        KunderaQueryParser parser = new KunderaQueryParser(kunderaQuery);
        parser.parse();

        Assert.assertEquals("Country", kunderaQuery.getFrom());
        Assert.assertEquals("currency = INR", kunderaQuery.getFilter());
        Assert.assertNull(kunderaQuery.getResult());
        Assert.assertTrue(kunderaQuery.isUpdateClause());
        Assert.assertEquals(true, kunderaQuery.isDeleteUpdate());
        for (UpdateClause q : kunderaQuery.getUpdateClauseQueue())
        {
            Assert.assertEquals("population", q.getProperty());
            Assert.assertEquals("10", q.getValue());
        }

        // Update with multi valued SET clause.
        String multiValueUpdaeQuery = "UPDATE Country SET population = 10,name=vivek where currency = INR";
        kunderaQuery = new KunderaQuery(multiValueUpdaeQuery, kunderaMetadata);

        parser = new KunderaQueryParser(kunderaQuery);
        parser.parse();

        Assert.assertEquals("Country", kunderaQuery.getFrom());
        Assert.assertEquals("currency = INR", kunderaQuery.getFilter());
        Assert.assertNull(kunderaQuery.getResult());
        Assert.assertTrue(kunderaQuery.isUpdateClause());
        Assert.assertEquals(true, kunderaQuery.isDeleteUpdate());
        Assert.assertEquals(2, kunderaQuery.getUpdateClauseQueue().size());

        UpdateClause[] result = kunderaQuery.getUpdateClauseQueue().toArray(new UpdateClause[] {});
        Assert.assertEquals("population", result[0].getProperty());
        Assert.assertEquals("10", result[0].getValue());

        Assert.assertEquals("name", result[1].getProperty());
        Assert.assertEquals("vivek", result[1].getValue());

        // Delete query.
        String deleteQuery = "Delete from Country where currency = INR";

        kunderaQuery = new KunderaQuery(deleteQuery, kunderaMetadata);
        parser = new KunderaQueryParser(kunderaQuery);
        parser.parse();

        Assert.assertEquals("Country", kunderaQuery.getFrom());
        Assert.assertEquals("currency = INR", kunderaQuery.getFilter());
        Assert.assertNull(kunderaQuery.getResult());
        Assert.assertFalse(kunderaQuery.isUpdateClause());
        Assert.assertEquals(true, kunderaQuery.isDeleteUpdate());
    }

    @Test
    public void onInvalidQueryParse()
    {

        // Valid Query with where clause.
        String validQuery = "SELECT c,c.currency FROM Country c where c.currency = INR";

        KunderaQuery kunderQuery = new KunderaQuery(validQuery, kunderaMetadata);
        KunderaQueryParser parser = new KunderaQueryParser(kunderQuery);
        try
        {
            parser.parse();
        }
        catch (JPQLParseException jpqlpe)
        {
            Assert.assertTrue(jpqlpe.getMessage().startsWith("Bad query format"));
        }

        // Valid Query with where clause.
        validQuery = "SELECT c.currency,c FROM Country c where c.currency = INR";

        kunderQuery = new KunderaQuery(validQuery, kunderaMetadata);
        parser = new KunderaQueryParser(kunderQuery);
        try
        {
            parser.parse();
        }
        catch (JPQLParseException jpqlpe)
        {
            Assert.assertTrue(jpqlpe.getMessage().startsWith("Bad query format"));
        }

        // Valid Query with where clause.
        validQuery = "SELECT c. FROM Country c where c.currency = INR";

        kunderQuery = new KunderaQuery(validQuery, kunderaMetadata);

        parser = new KunderaQueryParser(kunderQuery);
        try
        {
            parser.parse();
        }
        catch (JPQLParseException jpqlpe)
        {

            Assert.assertTrue(jpqlpe.getMessage().startsWith(
                    "You have not given any column name after . ,Column name should not be empty"));
        }
    }

    /**
     * Tear down.
     * 
     * @throws Exception
     *             the exception
     */
    @After
    public void tearDown() throws Exception
    {

    }

}
