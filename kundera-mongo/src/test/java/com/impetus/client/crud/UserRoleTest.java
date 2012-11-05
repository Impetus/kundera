/*******************************************************************************
 * * Copyright 2012 Impetus Infotech.
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

package com.impetus.client.crud;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.client.twitter.entities.RoleMongo;
import com.impetus.client.twitter.entities.User;
import com.impetus.client.utils.MongoUtils;

/**
 * The Class UserRoleTest.
 */
public class UserRoleTest {

	EntityManagerFactory emf;
	/** The em. */
	private EntityManager em;

	/**
	 * Sets the up.
	 */
	@Before
	public void setUp() {
		emf = Persistence.createEntityManagerFactory("mongoTest");
		em = emf.createEntityManager();
	}

	/**
	 * Test association.
	 */
	@Test
	public void testPersist() {
		RoleMongo rol = new RoleMongo();
		rol.setRolId(1);
		rol.setName("Administrador");
		User u = new User();
		u.setAge(15);
		u.setEmail("usuario1@infos.com");
		u.setName("usuario1");
		u.setUserId(1);
		u.setLastName("apellido1");
		User u2 = new User();
		u2.setAge(17);
		u2.setEmail("usuario2@infos.com");
		u2.setName("usuario2");
		u2.setUserId(2);
		u2.setLastName("apellido2");
		u.setUserRol(rol);
		u2.setUserRol(rol);
		List<User> users = new ArrayList<User>();
		users.add(u);
		users.add(u2);
		rol.setSegUsuarioList(users);
		em.persist(rol);

	}

	/**
	 * Test findby role.
	 */
	@Test
	public void testFindbyRole() {
		testPersist();
		String query = "Select r from Role r";
		Query q = em.createQuery(query);
		List<RoleMongo> roles = q.getResultList();
		Assert.assertNotNull(roles);
		Assert.assertEquals(1, roles.size());
	}

	/**
	 * Test findby user.
	 */
	@Test
	public void testFindbyUser() {
		testPersist();
		List<User> users = getAllUsers();
		Assert.assertNotNull(users);
		Assert.assertEquals(2, users.size());
	}

	/**
	 * Tear down.
	 */
	@After
	public void tearDown() {
		RoleMongo rol = em.find(RoleMongo.class, 1);
		if (rol != null) {
			em.remove(rol);
		}

		MongoUtils.dropDatabase(emf, "mongoTest");
		em.close();

		em = null;
	}

	/**
	 * @return
	 */
	private List<User> getAllUsers() {
		String query = "Select u from User u";
		Query q = em.createQuery(query);
		List<User> users = q.getResultList();
		return users;
	}
}
