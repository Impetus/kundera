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
package com.impetus.client.hbase.crud.mappedsuperclass.inheritence;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.hbase.testingutil.HBaseTestingUtils;
import com.impetus.kundera.metadata.mappedsuperclass.EntityWithoutFieldsBase;

/**
 * The Class HBaseEntityWithoutFieldsTest.
 * 
 * @author Pragalbh Garg
 * 
 *         Class to verify that at least one field in entity class is not
 *         mandatory if the superclass contains all the mandatory fields
 */
public class HBaseEntityWithoutFieldsTest extends EntityWithoutFieldsBase
{

    /**
     * Setup.
     */
    @Before
    public void setup()
    {
        persistenceUnit = "entityWithoutFieldTest";
        setupInternal();
    }

    /**
     * Test entity with no fields.
     */
    @Test
    public void testEntityWithNoFields()
    {
        testEntityWithNoFieldsBase();
    }

    /**
     * Test entity with no fields2 level inheritance.
     */
    @Test
    public void testEntityWithNoFields2LevelInheritance()
    {
        testEntityWithNoFields2LevelInheritanceBase();
    }

    /**
     * Tear down.
     */
    @After
    public void tearDown()
    {
        tearDownInternal();
    }

    /**
     * Tear down.
     * 
     * @throws Exception
     *             the exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception
    {
        HBaseTestingUtils.dropSchema("HBaseNew");
    }
}
