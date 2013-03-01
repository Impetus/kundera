/**
 * Copyright 2012 Impetus Infotech.
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
package com.impetus.client.neo4j.imdb.datatype;

import java.io.File;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.kernel.impl.util.FileUtils;

import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.KunderaMetadata;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;

/**
 * Test case
 * 
 * @author amresh.singh
 */
public class IMDBAllDataTypeTest {
	EntityManagerFactory emf;
	EntityManager em;
	Calendar cal = Calendar.getInstance();

	ActorAllDataType actor1;
	ActorAllDataType actor2;

	private static final String IMDB_PU = "imdb";

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		KunderaMetadata.INSTANCE.setApplicationMetadata(null);
		emf = Persistence.createEntityManagerFactory(IMDB_PU);
		em = emf.createEntityManager();

	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
		em.close();
		emf.close();
	}

	@Test
	public void testCRUD() {
		insert();
		findById();
		delete();

	}

	private void insert() {
		prepareData();

		em.getTransaction().begin();
		em.persist(actor1);
		em.persist(actor2);
		em.getTransaction().commit();
	}

	private void findById() {
		// Find actor by ID
		em.clear();
		ActorAllDataType actor1 = em.find(ActorAllDataType.class, 1);
		ActorAllDataType actor2 = em.find(ActorAllDataType.class, 2);

		assertActors(actor1, actor2);
	}

	private void delete() {
		ActorAllDataType actor1 = em.find(ActorAllDataType.class, 1);
		ActorAllDataType actor2 = em.find(ActorAllDataType.class, 2);

		em.getTransaction().begin();
		em.remove(actor1);
		em.remove(actor2);
		em.getTransaction().commit();

		em.clear();
		ActorAllDataType actor11 = em.find(ActorAllDataType.class, 1);
		ActorAllDataType actor22 = em.find(ActorAllDataType.class, 2);
		Assert.assertNull(actor11);
		Assert.assertNull(actor22);
	}

	private void assertActors(ActorAllDataType actor1, ActorAllDataType actor2) {
		Assert.assertNotNull(actor1);
		Assert.assertEquals(1, actor1.getId());
		Assert.assertEquals("Tom Cruise", actor1.getName());
		Assert.assertEquals(23456789l, actor1.getDepartmentId());
		Assert.assertEquals(true, actor1.isExceptional());
		Assert.assertEquals('C', actor1.getGrade());
		Assert.assertEquals((byte) 8, actor1.getDigitalSignature());
		Assert.assertEquals((short) 5, actor1.getRating());
		Assert.assertEquals((float) 10.0, actor1.getCompliance(), 0.0);
		Assert.assertEquals(163.12, actor1.getHeight(), 0.0);
		Assert.assertEquals(new Date(Long.parseLong("1351667541111")),
				actor1.getEnrolmentDate());
		Assert.assertEquals(new Date(Long.parseLong("1351667542222")),
				actor1.getEnrolmentTime());
		Assert.assertEquals(new Date(Long.parseLong("1351667543333")),
				actor1.getJoiningDateAndTime());
		Assert.assertEquals(new Integer(2), actor1.getYearsSpent());
		Assert.assertEquals(new Long(3634521523423L), actor1.getUniqueId());
		Assert.assertEquals(new Double(7.23452342343),
				actor1.getMonthlySalary());
		Assert.assertEquals(new BigInteger("123456789"),
				actor1.getJobAttempts());
		Assert.assertEquals(new BigDecimal(123456789),
				actor1.getAccumulatedWealth());
		Assert.assertEquals(cal, actor1.getGraduationDay());

		Map<RoleAllDataType, MovieAllDataType> movies1 = actor1.getMovies();
		Assert.assertFalse(movies1 == null || movies1.isEmpty());
		Assert.assertEquals(2, movies1.size());
		for (RoleAllDataType role : movies1.keySet()) {
			Assert.assertNotNull(role);
			Assert.assertNotNull(role.getActor());
			Assert.assertNotNull(role.getMovie());
			Assert.assertNotNull(movies1.get(role));
		}

		Assert.assertNotNull(actor2);
		Assert.assertEquals(2, actor2.getId());
		Assert.assertEquals("Emmanuelle Béart", actor2.getName());
		Assert.assertEquals(23456790l, actor2.getDepartmentId());
		Assert.assertEquals(false, actor2.isExceptional());
		Assert.assertEquals('D', actor2.getGrade());
		Assert.assertEquals((byte) 9, actor2.getDigitalSignature());
		Assert.assertEquals((short) 6, actor2.getRating());
		Assert.assertEquals((float) 11.3, actor2.getCompliance(), 0.0);
		Assert.assertEquals(161.99, actor2.getHeight(), 0.0);
		Assert.assertEquals(new Date(Long.parseLong("1351667544444")),
				actor2.getEnrolmentDate());
		Assert.assertEquals(new Date(Long.parseLong("1351667545555")),
				actor2.getEnrolmentTime());
		Assert.assertEquals(new Date(Long.parseLong("1351667546666")),
				actor2.getJoiningDateAndTime());
		Assert.assertEquals(new Integer(3), actor2.getYearsSpent());
		Assert.assertEquals(new Long(3634521523453L), actor2.getUniqueId());
		Assert.assertEquals(new Double(8.23452342343),
				actor2.getMonthlySalary());
		Assert.assertEquals(new BigInteger("123456790"),
				actor2.getJobAttempts());
		Assert.assertEquals(new BigDecimal(123456790),
				actor2.getAccumulatedWealth());
		Assert.assertEquals(cal, actor2.getGraduationDay());

		Map<RoleAllDataType, MovieAllDataType> movies2 = actor2.getMovies();
		Assert.assertFalse(movies2 == null || movies2.isEmpty());
		Assert.assertEquals(2, movies2.size());

		for (RoleAllDataType role : movies2.keySet()) {
			Assert.assertNotNull(role);
			Assert.assertNotNull(role.getActor());
			Assert.assertNotNull(role.getMovie());
			Assert.assertNotNull(movies2.get(role));
		}
	}

	private void prepareData() {
		actor1 = new ActorAllDataType(1, "Tom Cruise", 23456789l, true, 'C',
				(byte) 8, (short) 5, (float) 10.0, 163.12, new Date(
						Long.parseLong("1351667541111")), new Date(
						Long.parseLong("1351667542222")), new Date(
						Long.parseLong("1351667543333")), 2, new Long(
						3634521523423L), new Double(7.23452342343),
				new BigInteger("123456789"), new BigDecimal(123456789), cal);

		actor2 = new ActorAllDataType(2, "Emmanuelle Béart", 23456790l, false,
				'D', (byte) 9, (short) 6, (float) 11.3, 161.99, new Date(
						Long.parseLong("1351667544444")), new Date(
						Long.parseLong("1351667545555")), new Date(
						Long.parseLong("1351667546666")), 3, new Long(
						3634521523453L), new Double(8.23452342343),
				new BigInteger("123456790"), new BigDecimal(123456790), cal);

		// Movies
		MovieAllDataType movie1 = new MovieAllDataType("m1",
				"War of the Worlds", 2005);
		MovieAllDataType movie2 = new MovieAllDataType("m2",
				"Mission Impossible", 1996);
		MovieAllDataType movie3 = new MovieAllDataType("m3", "Hell", 2005);

		// Roles
		RoleAllDataType role1 = new RoleAllDataType("Ray Ferrier",
				"Lead Actor", 1, 354354354l, true, 'A', (byte) 8, (short) 5,
				3.7f, 6.5, new Date(Long.parseLong("1351667541111")), new Date(
						Long.parseLong("1351667542222")), new Date(
						Long.parseLong("1351667543333")), 1, 98682342343l, 6.7,
				new BigInteger("1111111111111"), new BigDecimal(1234567890),
				cal);
		role1.setActor(actor1);
		role1.setMovie(movie1);

		RoleAllDataType role2 = new RoleAllDataType("Ethan Hunt", "Lead Actor",
				2, 354354355l, false, 'B', (byte) 9, (short) 6, 3.8f, 6.8,
				new Date(Long.parseLong("1351667544444")), new Date(
						Long.parseLong("1351667545555")), new Date(
						Long.parseLong("1351667546666")), 2, 98682342344l, 6.8,
				new BigInteger("22222222222222"), new BigDecimal(1234567891),
				cal);
		role2.setActor(actor1);
		role2.setMovie(movie2);

		RoleAllDataType role3 = new RoleAllDataType("Claire Phelps",
				"Lead Actress", 3, 354354356l, true, 'C', (byte) 10, (short) 7,
				3.9f, 6.9, new Date(Long.parseLong("1351667547777")), new Date(
						Long.parseLong("1351667548888")), new Date(
						Long.parseLong("1351667549999")), 3, 98682342345l, 6.9,
				new BigInteger("3333333333333"), new BigDecimal(1234567892),
				cal);
		role3.setActor(actor2);
		role1.setMovie(movie2);

		RoleAllDataType role4 = new RoleAllDataType("Sophie",
				"Supporting Actress", 4, 354354357l, false, 'D', (byte) 11,
				(short) 5, 3.7f, 7.0,
				new Date(Long.parseLong("1351667551111")), new Date(
						Long.parseLong("1351667552222")), new Date(
						Long.parseLong("1351667553333")), 4, 98682342346l, 7.0,
				new BigInteger("4444444444444"), new BigDecimal(1234567893),
				cal);
		role4.setActor(actor2);
		role1.setMovie(movie3);

		// Relationships
		actor1.addMovie(role1, movie1);
		actor1.addMovie(role2, movie2);
		actor2.addMovie(role3, movie2);
		actor2.addMovie(role4, movie3);

		movie1.addActor(role1, actor1);
		movie2.addActor(role2, actor1);
		movie2.addActor(role3, actor2);
		movie3.addActor(role4, actor2);
	}

}
