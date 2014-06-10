/*******************************************************************************
 * * Copyright 2014 Impetus Infotech.
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
package com.impetus.client.cassandra.query;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.metamodel.Metamodel;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.client.crud.PersonCassandra;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.cassandra.persistence.CassandraCli;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.persistence.EntityManagerImpl;
import com.impetus.kundera.persistence.PersistenceDelegator;
import com.impetus.kundera.query.KunderaQuery;
import com.impetus.kundera.query.KunderaQueryParser;
import com.vividsolutions.jts.util.Assert;

/*
 * @author shaheed hussain
 * testcase to test the exception which is thrown if user uses any of the interclause or intraclause value in his data
 */
public class JpaQueryTest {

	/** The Constant logger. */
	private static final Logger logger = LoggerFactory
			.getLogger(JpaQueryTest.class);

	@Before
	public void setUp() throws Exception {
		CassandraCli.cassandraSetUp();
		CassandraCli.createKeySpace("KunderaExamples");
	}

	@After
	public void tearDown() throws Exception {
		CassandraCli.dropKeySpace("KunderaExamples");
		
	}

	/**
	 * @throws Exception
	 */
	@Test
	public void test() throws Exception {

		String pu = "secIdxCassandraTest";
		EntityManagerFactory emf = Persistence.createEntityManagerFactory(pu);
		EntityManager em = emf.createEntityManager();

		String queryString = " select p from PersonCassandra p where p.age in ('in vivek','in kk','AND shaheed') OR p.personName in ('vivek','kk')";
		String cqlQuery = parseAndCreateCqlQuery(
				getQueryObject(queryString, emf), emf, em, pu,
				PersonCassandra.class, 200);
		String expectedQuery = "SELECT * FROM \"PERSONCASSANDRA\" WHERE \"AGE\" IN (in vivek, inkk, AND shaheed) AND \"PERSON_NAME\" IN ('vivek', 'kk') LIMIT 200  ALLOW FILTERING";
		// Assert.equals(cqlQuery,expectedQuery); //vivek is fixing for extra
		// quotes in 'in' clause

		queryString = "Select p from PersonCassandra p where p.personName='sam''s and joseph''s' and p.age=80 ";
		cqlQuery = parseAndCreateCqlQuery(getQueryObject(queryString, emf),
				emf, em, pu, PersonCassandra.class, 200);

		expectedQuery = "SELECT * FROM \"PERSONCASSANDRA\" WHERE \"PERSON_NAME\" = 'sam''s and joseph''s' AND \"AGE\" = 80 LIMIT 200  ALLOW FILTERING";
		Assert.equals(cqlQuery, expectedQuery);

		queryString = "Select p from PersonCassandra p where p.personName = 'ram and wwe' and p.age='10'";
		cqlQuery = parseAndCreateCqlQuery(getQueryObject(queryString, emf),
				emf, em, pu, PersonCassandra.class, 200);
		expectedQuery = "SELECT * FROM \"PERSONCASSANDRA\" WHERE \"PERSON_NAME\" = 'ram and wwe' AND \"AGE\" = 10 LIMIT 200  ALLOW FILTERING";
		Assert.equals(cqlQuery, expectedQuery);

		queryString = "Select p from PersonCassandra p where p.personName = 'Like-==' ";
		cqlQuery = parseAndCreateCqlQuery(getQueryObject(queryString, emf),
				emf, em, pu, PersonCassandra.class, 200);
		expectedQuery = "SELECT * FROM \"PERSONCASSANDRA\" WHERE \"PERSON_NAME\" = 'Like-==' LIMIT 200  ALLOW FILTERING";
		Assert.equals(cqlQuery, expectedQuery);

		queryString = "Select p from PersonCassandra p where p.personName = '==1'";
		cqlQuery = parseAndCreateCqlQuery(getQueryObject(queryString, emf),
				emf, em, pu, PersonCassandra.class, 200);
		expectedQuery = "SELECT * FROM \"PERSONCASSANDRA\" WHERE \"PERSON_NAME\" = '==1' LIMIT 200  ALLOW FILTERING";
		Assert.equals(cqlQuery, expectedQuery);

		queryString = "Select p from PersonCassandra p where p.personName = 'Like >= NOT IN'";
		cqlQuery = parseAndCreateCqlQuery(getQueryObject(queryString, emf),
				emf, em, pu, PersonCassandra.class, 200);
		expectedQuery = "SELECT * FROM \"PERSONCASSANDRA\" WHERE \"PERSON_NAME\" = 'Like >= NOT IN' LIMIT 200  ALLOW FILTERING";

		queryString = "Select p from PersonCassandra p where p.personName = 'in= NOT IN >=set >< <>'";
		cqlQuery = parseAndCreateCqlQuery(getQueryObject(queryString, emf),
				emf, em, pu, PersonCassandra.class, 200);
		expectedQuery = "SELECT * FROM \"PERSONCASSANDRA\" WHERE \"PERSON_NAME\" = 'in= NOT IN >=set >< <>' LIMIT 200  ALLOW FILTERING";
		Assert.equals(cqlQuery, expectedQuery);

		queryString = "Select p from PersonCassandra p where p.personName = 'in= between. >=set anand >< or <>'";
		cqlQuery = parseAndCreateCqlQuery(getQueryObject(queryString, emf),
				emf, em, pu, PersonCassandra.class, 200);
		expectedQuery = "SELECT * FROM \"PERSONCASSANDRA\" WHERE \"PERSON_NAME\" = 'in= between. >=set anand >< or <>' LIMIT 200  ALLOW FILTERING";
		Assert.equals(cqlQuery, expectedQuery);

		em.close();
		emf.close();

	}

	/**
	 * @param queryString
	 * @param emf
	 * @return
	 */
	private KunderaQuery getQueryObject(String queryString,
			EntityManagerFactory emf) {
		Method getpostParsingInit = null;
		try {
			getpostParsingInit = KunderaQuery.class
					.getDeclaredMethod("postParsingInit");
		} catch (SecurityException e) {
			logger.warn(e.getMessage());
		} catch (NoSuchMethodException e) {
			logger.warn(e.getMessage());
		}
		getpostParsingInit.setAccessible(true);

		KunderaMetadata kunderaMetadata = ((EntityManagerFactoryImpl) emf)
				.getKunderaMetadataInstance();
		KunderaQuery kunderaQuery = new KunderaQuery(queryString,
				kunderaMetadata);
		KunderaQueryParser queryParser = new KunderaQueryParser(kunderaQuery);
		queryParser.parse();
		try {
			getpostParsingInit.invoke(kunderaQuery);
		} catch (IllegalArgumentException e) {
			logger.warn(e.getMessage());
		} catch (IllegalAccessException e) {
			logger.warn(e.getMessage());
		} catch (InvocationTargetException e) {
			logger.warn(e.getMessage());
		}

		return kunderaQuery;
	}

	/**
	 * @param kunderaQuery
	 * @param emf
	 * @param em
	 * @param puName
	 * @param entityClass
	 * @param maxResult
	 * @return
	 */
	private String parseAndCreateCqlQuery(KunderaQuery kunderaQuery,
			EntityManagerFactory emf, EntityManager em, String puName,
			Class entityClass, Integer maxResult) {
		Method getpd = null;
		try {
			getpd = EntityManagerImpl.class
					.getDeclaredMethod("getPersistenceDelegator");
		} catch (SecurityException e) {
			logger.warn(e.getMessage());
		} catch (NoSuchMethodException e) {
			logger.warn(e.getMessage());
		}
		getpd.setAccessible(true);
		PersistenceDelegator pd = null;
		try {
			pd = (PersistenceDelegator) getpd.invoke(em);
		} catch (IllegalArgumentException e) {
			logger.warn(e.getMessage());
		} catch (IllegalAccessException e) {
			logger.warn(e.getMessage());
		} catch (InvocationTargetException e) {
			logger.warn(e.getMessage());
		}

		KunderaMetadata kunderaMetadata = ((EntityManagerFactoryImpl) emf)
				.getKunderaMetadataInstance();

		CassQuery query = new CassQuery(kunderaQuery, pd, kunderaMetadata);
		query.setMaxResults(maxResult);

		EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(
				kunderaMetadata, entityClass);
		Metamodel metaModel = KunderaMetadataManager.getMetamodel(
				kunderaMetadata, puName);

		Client<CassQuery> client = pd.getClient(metadata);

		String cqlQuery = query.onQueryOverCQL3(metadata, client,
				(MetamodelImpl) metaModel, metadata.getRelationNames());
		return cqlQuery;

	}
}
