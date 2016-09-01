/*******************************************************************************
 *  * Copyright 2016 Impetus Infotech.
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

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.client.es.PersonES.Day;

import junit.framework.Assert;

/**
 * The Class ESQueryTest.
 * 
 * @author devender.yadav
 * 
 */
public class ESQueryWithPaginationTest {

	/** The emf. */
	private static EntityManagerFactory emf;

	/** The em. */
	private static EntityManager em;

	/** The node. */
	private static Node node = null;

	/**
	 * Sets the up before class.
	 * 
	 * @throws Exception
	 *             the exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		if (!checkIfServerRunning()) {
			Builder builder = Settings.settingsBuilder();
			builder.put("path.home", "target/data");
			node = new NodeBuilder().settings(builder).node();
		}
	}

	/**
	 * Check if server running.
	 * 
	 * @return true, if successful
	 */
	private static boolean checkIfServerRunning() {
		try {
			Socket socket = new Socket("127.0.0.1", 9300);
			return socket.getInetAddress() != null;
		} catch (UnknownHostException e) {
			return false;
		} catch (IOException e) {
			return false;
		}
	}

	/**
	 * Setup.
	 */
	@Before
	public void setup() {

		emf = Persistence.createEntityManagerFactory("es-pu");
		em = emf.createEntityManager();
		init();
	}

	/**
	 * Test query.
	 *
	 * @throws NoSuchFieldException
	 *             the no such field exception
	 * @throws SecurityException
	 *             the security exception
	 * @throws IllegalArgumentException
	 *             the illegal argument exception
	 * @throws IllegalAccessException
	 *             the illegal access exception
	 * @throws InterruptedException
	 *             the interrupted exception
	 */
	@Test
	public void testQuery() throws NoSuchFieldException, SecurityException, IllegalArgumentException,
			IllegalAccessException, InterruptedException {

		Query qry = em.createQuery("select p from PersonES p where p.age > 10 order by p.age", PersonES.class);

		qry.setFirstResult(5);
		qry.setMaxResults(3);

		List<PersonES> persons = qry.getResultList();
		assertResultList(persons);

	}

	/**
	 * Tear down.
	 */
	@After
	public void tearDown() {
		purge();
		em.close();
		emf.close();
	}

	/**
	 * Tear down after class.
	 * 
	 * @throws Exception
	 *             the exception
	 */
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		if (node != null)
			node.close();
	}

	/**
	 * Wait thread.
	 */
	private void waitThread() {
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Inits all the records.
	 */
	private void init() {
		PersonES person = new PersonES();

		for (int i = 1; i <= 30; i++) {
			person.setAge(i);
			person.setDay(Day.FRIDAY);
			person.setPersonId(i + "");
			person.setPersonName("dev_" + i);
			person.setSalary(1000.0 * i);
			em.persist(person);

		}
		waitThread();
	}

	/**
	 * Delete all the records.
	 */
	private void purge() {
		String deleteQuery = "delete from PersonES p";
		Query query = em.createQuery(deleteQuery);
		query.executeUpdate();
		waitThread();
	}

	/**
	 * Verify each person object of result list.
	 * 
	 * @param resultPersonList
	 *            the result person list
	 * @param persons
	 *            the persons
	 */
	private void assertResultList(List<PersonES> persons) {

		Assert.assertNotNull(persons);
		Assert.assertEquals(3, persons.size());

		for (PersonES person : persons) {

			if (person.getAge().equals(16)) {
				Assert.assertEquals("dev_16", person.getPersonName());
				Assert.assertEquals(16000.0, person.getSalary());
			} else if (person.getAge().equals(17)) {
				Assert.assertEquals("dev_17", person.getPersonName());
				Assert.assertEquals(17000.0, person.getSalary());
			}

			else if (person.getAge().equals(18)) {
				Assert.assertEquals("dev_18", person.getPersonName());
				Assert.assertEquals(18000.0, person.getSalary());
			} else {

				Assert.fail();
			}
		}
	}

}