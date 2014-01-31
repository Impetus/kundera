package com.impetus.kundera.metadata.processor.relation;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.kundera.loader.MetamodelLoaderException;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;
import com.impetus.kundera.polyglot.entities.PersonU11FK;
import com.impetus.kundera.polyglot.entities.PersonU1M;
import com.impetus.kundera.polyglot.entities.PersonUM1;
import com.impetus.kundera.polyglot.entities.PersonUMM;
import com.impetus.kundera.polyglot.entities.PersonUMMByMap;

public class RelationProcessorTest
{

    private static EntityManagerFactory emf;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {

        emf = Persistence.createEntityManagerFactory("patest");
    }

    @Test
    public void testManyToMany() throws NoSuchFieldException, SecurityException
    {
        ManyToManyRelationMetadataProcessor processor = new ManyToManyRelationMetadataProcessor(
                ((EntityManagerFactoryImpl) emf).getKunderaMetadataInstance());
        EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(
                ((EntityManagerFactoryImpl) emf).getKunderaMetadataInstance(), PersonUMMByMap.class);
        Assert.assertNotNull(metadata.getRelation("addresses"));
        // processor.addRelationIntoMetadata(PersonB11FK.class.getDeclaredField("address"),metadata);

        try
        {
            processor.process(PersonUMM.class, metadata);
            Assert.fail("Should have gone to catch block!");
        }
        catch (MetamodelLoaderException mlex)
        {
            Assert.assertNotNull(mlex.getMessage());
        }
    }

    @Test
    public void testOneToMany() throws NoSuchFieldException, SecurityException
    {
        OneToManyRelationMetadataProcessor processor = new OneToManyRelationMetadataProcessor(
                ((EntityManagerFactoryImpl) emf).getKunderaMetadataInstance());
        EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(
                ((EntityManagerFactoryImpl) emf).getKunderaMetadataInstance(), PersonU1M.class);
        Assert.assertNotNull(metadata.getRelation("addresses"));

        try
        {
            processor.process(PersonU1M.class, metadata);
            Assert.fail("Should have gone to catch block!");
        }
        catch (MetamodelLoaderException mlex)
        {
            Assert.assertNotNull(mlex.getMessage());
        }

        /*
         * try {
         * processor.addRelationIntoMetadata(PersonUMMByMap.class.getDeclaredField
         * ("addresses"), metadata);
         * Assert.fail("Should have gone to catch block!"); } catch
         * (UnsupportedOperationException uoex) {
         * Assert.assertNotNull(uoex.getMessage()); }
         */
    }

    @Test
    public void testManyToOne() throws NoSuchFieldException, SecurityException
    {
        ManyToOneRelationMetadataProcessor processor = new ManyToOneRelationMetadataProcessor(
                ((EntityManagerFactoryImpl) emf).getKunderaMetadataInstance());
        EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(
                ((EntityManagerFactoryImpl) emf).getKunderaMetadataInstance(), PersonUM1.class);
        Assert.assertNotNull(metadata.getRelation("address"));

        try
        {
            processor.process(PersonUM1.class, metadata);
            Assert.fail("Should have gone to catch block!");
        }
        catch (MetamodelLoaderException mlex)
        {
            Assert.assertNotNull(mlex.getMessage());
        }

        /*
         * try {
         * processor.addRelationIntoMetadata(PersonUMMByMap.class.getDeclaredField
         * ("addresses"), metadata);
         * Assert.fail("Should have gone to catch block!"); } catch
         * (UnsupportedOperationException uoex) {
         * Assert.assertNotNull(uoex.getMessage()); }
         */}

    @Test
    public void testOneToOne() throws NoSuchFieldException, SecurityException
    {
        OneToOneRelationMetadataProcessor processor = new OneToOneRelationMetadataProcessor(
                ((EntityManagerFactoryImpl) emf).getKunderaMetadataInstance());
        EntityMetadata metadata = KunderaMetadataManager.getEntityMetadata(
                ((EntityManagerFactoryImpl) emf).getKunderaMetadataInstance(), PersonU11FK.class);
        Assert.assertNotNull(metadata.getRelation("address"));

        try
        {
            processor.process(PersonU11FK.class, metadata);
            Assert.fail("Should have gone to catch block!");
        }
        catch (MetamodelLoaderException mlex)
        {
            Assert.assertNotNull(mlex.getMessage());
        }

        /*
         * try {
         * processor.addRelationIntoMetadata(PersonUMMByMap.class.getDeclaredField
         * ("addresses"), metadata);
         * Assert.fail("Should have gone to catch block!"); } catch
         * (UnsupportedOperationException uoex) {
         * Assert.assertNotNull(uoex.getMessage()); }
         */
    }

    @AfterClass
    public static void tearDown()
    {
        emf.close();
        emf = null;
    }
}
