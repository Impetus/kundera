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
package com.impetus.kundera.client.mappedsuperclass.inheritance;


import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.metadata.mappedsuperclass.EntityWithoutFieldsTest;

/**
 * @author amitkumar
 *
 *	Class to verify that at least one field in entity class is not mandatory if the 
 *	superclass contains all the mandatory fields
 */
public class RedisEntityWithoutFieldsTest extends EntityWithoutFieldsTest{

	@Before
	public void setup()
	{
		persistenceUnit = "redisMappedSuperClass_pu";
		setupInternal();
	}
	
	@Test
	public void testEntityWithNoFields()
	{
		testEntityWithNoFieldsBase();
	}
	
	@Test
	public void testEntityWithNoFields2LevelInheritance()
	{
		testEntityWithNoFields2LevelInheritanceBase();
	}

	@After
	public void tearDown()
	{
                em.createQuery("Delete from Person p").executeUpdate();
		tearDownInternal();
	}
}
