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
package com.impetus.kundera.client.crud.associations;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Pragalbh Garg
 *
 */
public class AssociationsBiDirectionalTest extends AssociationsBiDirectionalBase
{


    @Before
    public void setUp() throws Exception
    {
        super.setUpInternal();
    }

    @Test
    public void test()
    {
        assertCRUD();

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
        tearDownInternal();
    }

}
