/*******************************************************************************
 * * Copyright 2015 Impetus Infotech.
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
package com.impetus.client.crud.mappedsuperclass.inheritence;

import java.util.HashMap;

import javax.persistence.Persistence;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.cassandra.common.CassandraConstants;
import com.impetus.kundera.client.cassandra.persistence.CassandraCli;
import com.impetus.kundera.client.crud.mappedsuperclass.MappedSuperClassBase;

/**
 * @author Pragalbh Garg
 * 
 */
public class CassandraMappedSuperClassTestCQL3 extends MappedSuperClassBase
{

    @Before
    public void setUp() throws Exception
    {
        _PU = "cassandra_pu";
        CassandraCli.cassandraSetUp();
        HashMap propertyMap = new HashMap();
        propertyMap.put(CassandraConstants.CQL_VERSION, CassandraConstants.CQL_VERSION_3_0);
        emf = Persistence.createEntityManagerFactory(_PU, propertyMap);
        em = emf.createEntityManager();
    }

    @Test
    public void test()
    {
        assertInternal();
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
        CassandraCli.dropKeySpace("KunderaTests");
        tearDownInternal();
    }

}
