/**
 * 
 */
package com.impetus.kundera.metadata.mappedsuperclass;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author amitkumar
 *
 *	Class to verify that at least one field in entity class is not mandatory if the 
 *	superclass contains all the mandatory fields
 */
public class EntityWithoutFieldsBase {

	protected static EntityManagerFactory emf;

	protected static EntityManager em;

	protected static String persistenceUnit = "inheritanceTest";
	
	private static Logger logger = LoggerFactory.getLogger(EntityWithoutFieldsBase.class);

	protected void setupInternal()
	{
		emf = Persistence.createEntityManagerFactory(persistenceUnit);
		em = emf.createEntityManager();
	}

	protected void testEntityWithNoFieldsBase()
	{
		logger.info("Test entity with no fields, all the fields declared in mappedsuperclass.");

		// Persist Person
		Person person = new Person();
		person.setId("1");
		person.setFirstName("Amit");
		person.setLastName("Kumar");

		em.persist(person);

		Person fetchPerson = em.find(Person.class, "1");

		// Assertions to validate persisted object
		Assert.assertNotNull(fetchPerson);
		Assert.assertEquals("1", fetchPerson.getId());
		Assert.assertEquals("Amit", fetchPerson.getFirstName());
		Assert.assertEquals("Kumar", fetchPerson.getLastName());

		// Assert to verify that person has been removed
		em.remove(fetchPerson);
		Assert.assertNull(em.find(Person.class, "1"));
	}

	protected void testEntityWithNoFields2LevelInheritanceBase()
	{
		logger.info("Test entity with no fields with two level inheritance, all the fields declared in mappedsuperclass.");

		// Persist Person
		PersonChild person = new PersonChild();
		person.setId("1");
		person.setFirstName("Amit");
		person.setLastName("Kumar");

		em.persist(person);

		PersonChild fetchPerson = em.find(PersonChild.class, "1");

		// Assertions to validate persisted object
		Assert.assertNotNull(fetchPerson);
		Assert.assertEquals("1", fetchPerson.getId());
		Assert.assertEquals("Amit", fetchPerson.getFirstName());
		Assert.assertEquals("Kumar", fetchPerson.getLastName());

		// Assert to verify that person has been removed
		em.remove(fetchPerson);
		Assert.assertNull(em.find(PersonChild.class, "1"));
	}

	protected void tearDownInternal()
	{
		if (em != null)
		{
			em.close();
			em = null;
		}

		if (emf != null)
		{
			emf.close();
			emf = null;
		}
	}
}