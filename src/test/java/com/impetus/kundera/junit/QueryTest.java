/*
 * Copyright 2010 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.impetus.kundera.junit;

import java.net.URL;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Query;

import org.apache.cassandra.service.EmbeddedCassandraService;

import junit.framework.TestCase;

import com.impetus.kundera.ejb.EntityManagerFactoryImpl;
import com.impetus.kundera.entity.Author;

public class QueryTest extends TestCase {

	/** The factory. */
	EntityManagerFactory factory;

	private void startCassandraServer() throws Exception {
		URL configURL = TestKundera.class.getClassLoader().getResource(
				"storage-conf.xml");
		try {
			String storageConfigPath = configURL.getFile().substring(1)
					.substring(0, configURL.getFile().lastIndexOf("/"));
			System.setProperty("storage-config", storageConfigPath);
		} catch (Exception e) {
			fail("Could not find storage-config.xml sfile");
		}
		EmbeddedCassandraService cassandra = new EmbeddedCassandraService();
		cassandra.init();
		Thread t = new Thread(cassandra);
		t.setDaemon(true);
		t.start();
	}

	protected void setUp() throws Exception {
		super.setUp();
		startCassandraServer();
		factory = new EntityManagerFactoryImpl("test-unit-2");// from META-INF/persistence.xml
	}

	protected void tearDown() throws Exception {
		super.tearDown();
		factory.close();
	}

	public void testQueryAccrossManagerSessions() throws Exception {
		EntityManager firstManager = factory.createEntityManager();

		int count = 10;
		String msTestRan = Integer.toString(Calendar.getInstance().get(
				Calendar.MILLISECOND));
		
		String country = msTestRan + "-India";

		for (int i = 0; i < count; i++) {
			String key = msTestRan + "-author" + i;
			Author author = createAuthor(key, "someEmail", country, new Date());
			firstManager.persist(author);
		}
		firstManager.close();

		EntityManager secondManager = factory.createEntityManager();

		Query q = secondManager
				.createQuery("select a from Author a where a.country like :country");
		q.setParameter("country", country);

		List<Author> authors = q.getResultList();
		System.out.println(authors.size());

		for (Author a : authors) {
			secondManager.remove(a);
			System.out.println("removing " + a.getUsername());
		}
		secondManager.close();

		assertEquals(count, authors.size());
	}

	private static Author createAuthor(String username, String email,
			String country, Date registeredSince) {
		Author author = new Author();
		author.setUsername(username);
		author.setCountry(country);
		author.setEmailAddress(email);
		author.setRegistered(registeredSince);
		return author;
	}

}
