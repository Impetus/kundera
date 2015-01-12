package com.impetus.client.crud.mappedsuperclass.inheritence;


import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.metadata.mappedsuperclass.EntityWithoutFieldsBase;

/**
 * @author amitkumar
 *
 *	Class to verify that at least one field in entity class is not mandatory if the 
 *	superclass contains all the mandatory fields
 */
public class MongoEntityWithoutFieldsTest extends EntityWithoutFieldsBase
{

	@Before
	public void setup()
	{
		persistenceUnit = "mongoShowQueryDisabledPU";
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
		tearDownInternal();
	}
}
