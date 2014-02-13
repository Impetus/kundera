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
package com.impetus.kundera.metadata.processor;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.Constants;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.CoreTestClient;
import com.impetus.kundera.metadata.MetadataBuilder;
import com.impetus.kundera.metadata.model.ApplicationMetadata;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;

/**
 * Junit Test case for @See TableProcessor.
 * 
 * @author vivek.mishra
 * 
 */
public class TableProcessorTest
{

    private EntityManagerFactory emf;

    private KunderaMetadata kunderaMetadata;

    /**
     * Sets the up.
     * 
     * @throws Exception
     *             the exception
     */
    @Before
    public void setUp() throws Exception
    {
        emf = Persistence.createEntityManagerFactory("kunderatest");
        kunderaMetadata = ((EntityManagerFactoryImpl) emf).getKunderaMetadataInstance();
    }

    /**
     * Test process query metadata.
     * 
     * @throws InstantiationException
     *             the instantiation exception
     * @throws IllegalAccessException
     *             the illegal access exception
     */
    @Test
    public void testProcessQueryMetadata() throws InstantiationException, IllegalAccessException
    {
        final String persistenceUnit = "rdbms";
        final String named_query = "Select t from TestEntity t where t.field = :field";
        final String named_query1 = "Select t1 from TestEntity t1 where t1.field = :field";
        final String named_query2 = "Select t2 from TestEntity t2 where t2.field = :field";
        final String native_query = "Select native from TestEntity native where native.field = :field";
        final String native_query1 = "Select native1 from TestEntity native1 where native1.field = :field";
        final String native_query2 = "Select native2 from TestEntity native2 where native2.field = :field";

        EntityMetadata metadata = new EntityMetadata(EntitySample.class);
        metadata.setPersistenceUnit("rdbms");

        PersistenceUnitMetadata puMetadata = new PersistenceUnitMetadata();
        puMetadata.setPersistenceUnitName(persistenceUnit);

        Map<String, Object> props = new HashMap<String, Object>();
        props.put(Constants.PERSISTENCE_UNIT_NAME, persistenceUnit);
        props.put(PersistenceProperties.KUNDERA_NODES, "localhost");
        props.put(PersistenceProperties.KUNDERA_PORT, "9160");
        props.put(PersistenceProperties.KUNDERA_KEYSPACE, "KunderaHbaseExamples");
        props.put(PersistenceProperties.KUNDERA_CLIENT_FACTORY, "com.impetus.client.CoreTestClientFactory");

        Properties p = new Properties();
        p.putAll(props);
        puMetadata.setProperties(p);

        ApplicationMetadata appMetadata = kunderaMetadata.getApplicationMetadata();

        Map<String, PersistenceUnitMetadata> metadataCol = new HashMap<String, PersistenceUnitMetadata>();

        metadataCol.put(persistenceUnit, puMetadata);
        appMetadata.addPersistenceUnitMetadata(metadataCol);

        MetadataBuilder metadataBuilder = new MetadataBuilder(persistenceUnit, CoreTestClient.class.getSimpleName(),
                null, kunderaMetadata);
        metadataBuilder.buildEntityMetadata(metadata.getEntityClazz());

        // Named query asserts.
        Assert.assertNotNull(appMetadata.getQuery("test.named.query"));
        Assert.assertEquals(appMetadata.getQuery("test.named.query"), named_query);
        Assert.assertNotNull(appMetadata.getQuery("test.named.queries1"));
        Assert.assertEquals(appMetadata.getQuery("test.named.queries1"), named_query1);
        Assert.assertNotNull(appMetadata.getQuery("test.named.queries2"));
        Assert.assertEquals(appMetadata.getQuery("test.named.queries2"), named_query2);
        Assert.assertNotNull(appMetadata.getQuery("test.named.queries2"));

        // Native query asserts
        Assert.assertNotNull(appMetadata.getQuery("test.native.query"));
        Assert.assertEquals(appMetadata.getQuery("test.native.query"), native_query);
        Assert.assertNotNull(appMetadata.getQuery("test.native.query1"));
        Assert.assertEquals(appMetadata.getQuery("test.native.query1"), native_query1);
        Assert.assertNotNull(appMetadata.getQuery("test.native.query2"));
        Assert.assertEquals(appMetadata.getQuery("test.native.query2"), native_query2);
    }

    /**
     * Test process query metadata.
     * 
     * @throws InstantiationException
     *             the instantiation exception
     * @throws IllegalAccessException
     *             the illegal access exception
     */
    @Test
    public void testProcessInheritedClass() throws InstantiationException, IllegalAccessException
    {
        final String persistenceUnit = "rdbms";

        EntityMetadata metadata;

        PersistenceUnitMetadata puMetadata = new PersistenceUnitMetadata();
        puMetadata.setPersistenceUnitName(persistenceUnit);

        Map<String, Object> props = new HashMap<String, Object>();
        props.put(Constants.PERSISTENCE_UNIT_NAME, persistenceUnit);
        props.put(PersistenceProperties.KUNDERA_NODES, "localhost");
        props.put(PersistenceProperties.KUNDERA_PORT, "9160");
        props.put(PersistenceProperties.KUNDERA_KEYSPACE, "KunderaHbaseExamples");
        props.put(PersistenceProperties.KUNDERA_CLIENT_FACTORY, "com.impetus.client.CoreTestClientFactory");

        Properties p = new Properties();
        p.putAll(props);
        puMetadata.setProperties(p);

        TableProcessor t1 = new TableProcessor(p, kunderaMetadata);

        metadata = new EntityMetadata(Rectangle.class);
        metadata.setPersistenceUnit(persistenceUnit);
        t1.process(Rectangle.class, metadata);
        Assert.assertNotNull(metadata.getIdAttribute());

        metadata = new EntityMetadata(Circle.class);
        metadata.setPersistenceUnit(persistenceUnit);
        t1.process(Circle.class, metadata);
        Assert.assertNotNull(metadata.getIdAttribute());

        metadata = new EntityMetadata(Shape.class);
        metadata.setPersistenceUnit(persistenceUnit);
        t1.process(Shape.class, metadata);
        Assert.assertNotNull(metadata.getIdAttribute());

    }

    /**
     * Test process query metadata.
     * 
     */
    @Test
    public void testInheritedRelations()
    {
        final String persistenceUnit = "inheritanceTest";

        EntityManagerFactory emf = Persistence.createEntityManagerFactory(persistenceUnit);
        EntityManager em = emf.createEntityManager();

        Rectangle rect1 = new Rectangle();
        rect1.setId("r1");
        rect1.setName("Rect1");

        Geometry geo1 = new Geometry();
        geo1.setGeoId("g1");
        geo1.setName("Two D");

        rect1.setGeometry(geo1);

        Circle circle = new Circle();
        circle.setId("c1");
        circle.setName("Circle1");
        circle.setGeometry(geo1);

        em.persist(rect1);
        em.persist(circle);
        em.clear();

        Geometry geo2 = new Geometry();
        geo2.setGeoId("g2");
        geo2.setName("Closed");

        Rectangle rectangle = em.find(Rectangle.class, "r1");

        Assert.assertNotNull(rectangle.getGeometry());
        Assert.assertEquals("Two D", rectangle.getGeometry().getName());

        rectangle.setGeometry(geo2);

        em.merge(rectangle);
        em.clear();

        circle = em.find(Circle.class, "c1");
        Assert.assertNotNull(circle.getGeometry());
        Assert.assertEquals("Two D", circle.getGeometry().getName());
        em.clear();

        em.close();
        emf.close();
    }

    /**
     * Test constraints on inherited objects
     * 
     */
    @Test
    public void testInheritedConstraints()
    {
        try
        {

            final String persistenceUnit = "inheritanceTest";

            EntityManagerFactory emf = Persistence.createEntityManagerFactory(persistenceUnit);
            EntityManager em = emf.createEntityManager();

            Rectangle rect1 = new Rectangle();
            rect1.setId("r1");
            // rect1.setName("Rect1");

            Geometry geo1 = new Geometry();
            geo1.setGeoId("g1");
            geo1.setName("Two D");

            rect1.setGeometry(geo1);

            em.persist(rect1);

            em.clear();

            em.close();
            emf.close();
        }
        catch (Exception e)
        {

            Assert.assertEquals("javax.validation.ValidationException: Name of the object should be defined",
                    e.getMessage());

        }
    }

    /**
     * Tear down.
     * 
     * @throws Exception
     *             the exception
     */
    @After
    public void tearDown() throws Exception
    {
        // Do nothing.
    }

}
