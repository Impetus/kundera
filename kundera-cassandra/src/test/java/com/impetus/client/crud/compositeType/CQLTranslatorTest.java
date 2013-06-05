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
package com.impetus.client.crud.compositeType;

import java.util.Date;
import java.util.UUID;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.cassandra.thrift.CQLTranslator;
import com.impetus.client.cassandra.thrift.CQLTranslator.TranslationType;
import com.impetus.client.persistence.CassandraCli;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;

/**
 * JUnit for CQL translator test
 * 
 * @author vivek.mishra
 * 
 */
public class CQLTranslatorTest
{
    private EntityManagerFactory emf;

    private static final Logger logger = LoggerFactory.getLogger(CassandraCompositeTypeTest.class);
    
    private static final String KEYSPACE = "CompositeCassandra";

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
    	CassandraCli.dropKeySpace(KEYSPACE);
        CassandraCli.cassandraSetUp();
        emf = Persistence.createEntityManagerFactory("composite_pu");
    }

    @Test
    public void testPrepareColumns()
    {
        logger.info("On prepare columns");
        CQLTranslator translator = new CQLTranslator();
        UUID timeLineId = UUID.randomUUID();
        Date currentDate = new Date();
        CassandraCompoundKey key = new CassandraCompoundKey("mevivs", 1, timeLineId);
        CassandraPrimeUser user = new CassandraPrimeUser(key);
        user.setTweetBody("my first tweet");
        user.setTweetDate(currentDate);
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(CassandraPrimeUser.class);
        String translatedSql = translator
                .prepareColumnOrColumnValues(user, entityMetadata, TranslationType.VALUE, null).get(
                        TranslationType.VALUE);
        String columnAsCsv = "'mevivs',1," + timeLineId + ",'my first tweet','" + currentDate.getTime() + "'";
        Assert.assertEquals(columnAsCsv, translatedSql);
    }

    @After
    public void tearDown()
    {
        CassandraCli.dropKeySpace(KEYSPACE);
        emf.close();
    }
}
