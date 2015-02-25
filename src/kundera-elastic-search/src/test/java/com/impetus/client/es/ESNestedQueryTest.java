/*******************************************************************************
 *  * Copyright 2015 Impetus Infotech.
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
package com.impetus.client.es;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import junit.framework.Assert;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.client.es.PersonES.Day;

/**
 * @author amitkumar
 *
 */
public class ESNestedQueryTest {

	
	/** The emf. */
	private EntityManagerFactory emf;

	/** The em. */
	private EntityManager em;

	private static Node node = null;
	
	private PersonES person;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception
	{
		if (!checkIfServerRunning())
		{
			ImmutableSettings.Builder builder = ImmutableSettings.settingsBuilder();
			builder.put("path.data", "target/data");
			node = new NodeBuilder().settings(builder).node();
		}
	}

	/**
	 * Check if server running.
	 * 
	 * @return true, if successful
	 */
	private static boolean checkIfServerRunning()
	{
		try
		{
			Socket socket = new Socket("127.0.0.1", 9300);
			return socket.getInetAddress() != null;
		}
		catch (UnknownHostException e)
		{
			return false;
		}
		catch (IOException e)
		{
			return false;
		}
	}
	@Before
	public void setup() throws InterruptedException
	{

		emf = Persistence.createEntityManagerFactory("es-pu");
		em = emf.createEntityManager();
		init();
	}

	private void init() throws InterruptedException {
		
		createPerson("1", 20, "Amit", 100.0);
		createPerson("2", 10, "Dev", 200.0);
		createPerson("3", 30, "Karthik", 300.0);
		createPerson("4", 40, "Pragalbh", 400.0);
		waitThread();
	}

	/**
	 * 
	 */
	private void createPerson(String id, int age, String name, Double salary) {
		person = new PersonES();
		person.setAge(age);
		person.setDay(Day.FRIDAY);
		person.setPersonId(id);
		person.setPersonName(name);
		person.setSalary(salary);
		em.persist(person);
	}
	
	@Test
	public void testAndAggregation()
	{
		String nestedQquery = "Select p from PersonES p where p.personName = 'karthik' AND p.personName = 'pragalbh'";
		Query query = em.createQuery(nestedQquery);
		List resultList = query.getResultList();
		
		Assert.assertEquals(0, resultList.size());
	}
	
	@Test
	public void testMultiAndAggregation()
	{
		String nestedQquery = "Select p from PersonES p where p.age > 0 AND p.age < 35 AND p.salary > 150";
		Query query = em.createQuery(nestedQquery);
		List resultList = query.getResultList();
		
		Assert.assertEquals(2, resultList.size());
	}
	
	@Test
	public void testOrAggregation()
	{
		String nestedQquery = "Select p from PersonES p where p.personName = 'karthik' OR p.personName = 'pragalbh'";
		Query query = em.createQuery(nestedQquery);
		List resultList = query.getResultList();
		
		Assert.assertEquals(2, resultList.size());
	}
	
	@Test
	public void testMultiOrAggregation()
	{
		String nestedQquery = "Select p from PersonES p where p.personName = 'amit' OR p.age < 15 OR p.age > 35";
		Query query = em.createQuery(nestedQquery);
		List resultList = query.getResultList();
		
		Assert.assertEquals(3, resultList.size());
	}
	
	@Test
	public void testNestedAndOrAggregation()
	{
		String nestedQuery = "Select p from PersonES p where p.age > 0 AND (p.salary > 350 and (p.personName = 'karthik' OR p.personName = 'pragalbh'))";
		
		Query query = em.createQuery(nestedQuery);
		List resultList = query.getResultList();	
		Assert.assertEquals(1, resultList.size());
		Assert.assertEquals("Pragalbh", ((PersonES)resultList.get(0)).getPersonName());
	}
	
	@Test
	public void testNestedAndOrMaxAggregation()
	{
		String nestedQuery = "Select max(p.age) from PersonES p where p.age > 0 AND (p.salary > 250 and (p.personName = 'karthik' OR p.personName = 'pragalbh'))";
		
		Query query = em.createQuery(nestedQuery);
		List resultList = query.getResultList();	
		
		Assert.assertEquals(1, resultList.size());
		Assert.assertEquals(40.0, resultList.get(0));
	}
	
	@Test
	public void testNestedAndOrMinQuery()
	{
		String invalidQueryWithAndClause = "Select min(p.age) from PersonES p where p.age > 0 AND (p.personName = 'amit' OR p.personName = 'dev')";
		Query nameQuery = em.createNamedQuery(invalidQueryWithAndClause);
		List resultList = nameQuery.getResultList();
		
		Assert.assertEquals(1, resultList.size());
		Assert.assertEquals(10.0, resultList.get(0));
	}
	
	@Test
	public void testNestedQuery()
	{
		String invalidQueryWithAndClause = "Select min(p.age) from PersonES p where p.age > 0 AND (p.personName = 'amit' OR p.personName = 'dev')";
		Query nameQuery = em.createNamedQuery(invalidQueryWithAndClause);
		List persons = nameQuery.getResultList();
		
		Assert.assertEquals(1, persons.size());
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception
	{
		if(node != null)
			node.close();
	}


	@After
	public void tearDown()
	{
		em.close();
		emf.close();
	}

	private void waitThread() throws InterruptedException
	{
		Thread.sleep(2000);
	}
}
