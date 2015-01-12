package com.impetus.kundera.metadata.mappedsuperclass;


import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author amitkumar
 *
 *	Class to verify that at least one field in entity class is not mandatory if the 
 *	superclass contains all the mandatory fields
 */
public class EntityWithoutFieldsTest extends EntityWithoutFieldsBase 
{
	
	@Before
	public void setup()
	{
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

