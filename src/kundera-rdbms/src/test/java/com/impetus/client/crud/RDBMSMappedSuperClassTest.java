/*******************************************************************************
 * * Copyright 2013 Impetus Infotech.
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
package com.impetus.client.crud;

import javax.persistence.MappedSuperclass;
import javax.persistence.Query;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.client.crud.mappedsuperclass.MappedSuperClassBase;
import com.impetus.kundera.metadata.model.KunderaMetadata;

/**
 * @author vivek.mishra
 * Junit for  {@link MappedSuperclass} in RDBMS
 *
 */
public class RDBMSMappedSuperClassTest extends MappedSuperClassBase
{

    private RDBMSCli cli;

    @Before
    public void setUp() throws Exception
    {
        _PU = "mappedPu";
        setUpInternal();
    }
    
    @Test
    public void test() throws Exception
    {
        cli = new RDBMSCli("testdb");
        cli.createSchema("testdb");

        assertInternal(true);
    }

    
    @After
    public void tearDown() throws Exception
    {
//        // Delete by query.
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
