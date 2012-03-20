/*******************************************************************************
 * * Copyright 2011 Impetus Infotech.
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
package com.impetus.kundera.metadata.processor;


import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.metadata.model.ApplicationMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KunderaMetadata;

/**
 * Junit Test case for @See TableProcessor.
 * 
 * @author vivek.mishra
 *
 */
public class TableProcessorTest
{

    /**
     * Sets the up.
     *
     * @throws Exception the exception
     */
    @Before
    public void setUp() throws Exception
    {
        //Do nothing.
    }

    
    /**
     * Test process query metadata.
     *
     * @throws InstantiationException the instantiation exception
     * @throws IllegalAccessException the illegal access exception
     */
    @Test
    public void testProcessQueryMetadata() throws InstantiationException, IllegalAccessException
    {
        final String named_query = "Select t from TestEntity t where t.field = :field";
        final String named_query1 = "Select t1 from TestEntity t1 where t1.field = :field";
        final String named_query2 = "Select t2 from TestEntity t2 where t2.field = :field";
        final String native_query = "Select native from TestEntity native where native.field = :field";
        final String native_query1 = "Select native1 from TestEntity native1 where native1.field = :field";
        final String native_query2 = "Select native2 from TestEntity native2 where native2.field = :field";
        
        EntityMetadata metadata = new EntityMetadata(EntitySample.class);
        TableProcessor tableProcessor = new TableProcessor();
        tableProcessor.process(EntitySample.class, metadata);
        
        //Get application metadata
        ApplicationMetadata appMetadata = KunderaMetadata.INSTANCE.getApplicationMetadata();
        
        // Named query asserts.
        Assert.assertNotNull(appMetadata.getQuery("test.named.query"));
        Assert.assertEquals(appMetadata.getQuery("test.named.query"),named_query);
        Assert.assertNotNull(appMetadata.getQuery("test.named.queries1"));
        Assert.assertEquals(appMetadata.getQuery("test.named.queries1"),named_query1);
        Assert.assertNotNull(appMetadata.getQuery("test.named.queries2"));
        Assert.assertEquals(appMetadata.getQuery("test.named.queries2"),named_query2);
        Assert.assertNotNull(appMetadata.getQuery("test.named.queries2"));

        //Native query asserts
        Assert.assertNotNull(appMetadata.getQuery("test.native.query"));
        Assert.assertEquals(appMetadata.getQuery("test.native.query"),native_query);
        Assert.assertNotNull(appMetadata.getQuery("test.native.query1"));
        Assert.assertEquals(appMetadata.getQuery("test.native.query1"),native_query1);
        Assert.assertNotNull(appMetadata.getQuery("test.native.query2"));
        Assert.assertEquals(appMetadata.getQuery("test.native.query2"),native_query2);
    }
    
    /**
     * Tear down.
     *
     * @throws Exception the exception
     */
    @After
    public void tearDown() throws Exception
    {
        //Do nothing.
    }

}
