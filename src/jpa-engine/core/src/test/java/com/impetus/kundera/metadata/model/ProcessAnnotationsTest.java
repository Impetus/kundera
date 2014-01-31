package com.impetus.kundera.metadata.model;

import javax.persistence.EntityManagerFactory;
import javax.persistence.GenerationType;
import javax.persistence.Persistence;
import javax.persistence.metamodel.Metamodel;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.validator.GeneratedIdDefault;
import com.impetus.kundera.metadata.validator.GeneratedIdStrategyAuto;
import com.impetus.kundera.metadata.validator.GeneratedIdStrategyIdentity;
import com.impetus.kundera.metadata.validator.GeneratedIdStrategySequence;
import com.impetus.kundera.metadata.validator.GeneratedIdStrategyTable;
import com.impetus.kundera.metadata.validator.GeneratedIdWithOutSequenceGenerator;
import com.impetus.kundera.metadata.validator.GeneratedIdWithOutTableGenerator;
import com.impetus.kundera.metadata.validator.GeneratedIdWithSequenceGenerator;
import com.impetus.kundera.metadata.validator.GeneratedIdWithTableGenerator;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;

public class ProcessAnnotationsTest
{

    EntityManagerFactory emf;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception
    {
    }

    @Before
    public void setUp() throws Exception
    {
        emf = Persistence.createEntityManagerFactory("GeneratedValue,kunderatest");
    }

    @After
    public void tearDown() throws Exception
    {
        emf.close();
    }

    @Test
    public void testProcess()
    {
        Metamodel metamodel = KunderaMetadataManager.getMetamodel(
                ((EntityManagerFactoryImpl) emf).getKunderaMetadataInstance(), "GeneratedValue");

        // for entity GeneratedIdDefault.
        IdDiscriptor keyValue = ((MetamodelImpl) metamodel).getKeyValue(GeneratedIdDefault.class.getName());
        Assert.assertNotNull(keyValue);
        Assert.assertEquals(GenerationType.AUTO, keyValue.getStrategy());
        Assert.assertNull(keyValue.getTableDiscriptor());
        Assert.assertNull(keyValue.getSequenceDiscriptor());
        keyValue = null;

        // for entity GeneratedIdStrategyAuto.
        keyValue = ((MetamodelImpl) metamodel).getKeyValue(GeneratedIdStrategyAuto.class.getName());
        Assert.assertNotNull(keyValue);
        Assert.assertEquals(GenerationType.AUTO, keyValue.getStrategy());
        Assert.assertNull(keyValue.getTableDiscriptor());
        Assert.assertNull(keyValue.getSequenceDiscriptor());
        keyValue = null;

        keyValue = ((MetamodelImpl) metamodel).getKeyValue(GeneratedIdStrategyIdentity.class.getName());
        Assert.assertNotNull(keyValue);
        Assert.assertEquals(GenerationType.IDENTITY, keyValue.getStrategy());
        Assert.assertNull(keyValue.getTableDiscriptor());
        Assert.assertNull(keyValue.getSequenceDiscriptor());
        keyValue = null;

        // for entity GeneratedIdStrategySequence.
        keyValue = ((MetamodelImpl) metamodel).getKeyValue(GeneratedIdStrategySequence.class.getName());
        Assert.assertNotNull(keyValue);
        Assert.assertEquals(GenerationType.SEQUENCE, keyValue.getStrategy());
        Assert.assertNull(keyValue.getTableDiscriptor());
        Assert.assertNotNull(keyValue.getSequenceDiscriptor());
        Assert.assertEquals(50, keyValue.getSequenceDiscriptor().getAllocationSize());
        Assert.assertEquals(1, keyValue.getSequenceDiscriptor().getInitialValue());
        Assert.assertEquals("KunderaTest", keyValue.getSequenceDiscriptor().getSchemaName());
        Assert.assertEquals("sequence_name", keyValue.getSequenceDiscriptor().getSequenceName());
        Assert.assertNull(keyValue.getSequenceDiscriptor().getCatalog());
        keyValue = null;

        // for entity GeneratedIdStrategyTable.
        keyValue = ((MetamodelImpl) metamodel).getKeyValue(GeneratedIdStrategyTable.class.getName());
        Assert.assertNotNull(keyValue);
        Assert.assertEquals(GenerationType.TABLE, keyValue.getStrategy());
        Assert.assertNull(keyValue.getSequenceDiscriptor());
        Assert.assertNotNull(keyValue.getTableDiscriptor());
        Assert.assertEquals(50, keyValue.getTableDiscriptor().getAllocationSize());
        Assert.assertEquals(1, keyValue.getTableDiscriptor().getInitialValue());
        Assert.assertEquals("KunderaTest", keyValue.getTableDiscriptor().getSchema());
        Assert.assertEquals("kundera_sequences", keyValue.getTableDiscriptor().getTable());
        Assert.assertEquals("sequence_name", keyValue.getTableDiscriptor().getPkColumnName());
        Assert.assertEquals("GeneratedIdStrategyTable", keyValue.getTableDiscriptor().getPkColumnValue());
        Assert.assertEquals("sequence_value", keyValue.getTableDiscriptor().getValueColumnName());
        Assert.assertNull(keyValue.getTableDiscriptor().getCatalog());
        Assert.assertNull(keyValue.getTableDiscriptor().getUniqueConstraints());
        keyValue = null;

        // for entity GeneratedIdWithSequenceGenerator.
        keyValue = ((MetamodelImpl) metamodel).getKeyValue(GeneratedIdWithSequenceGenerator.class.getName());
        Assert.assertNotNull(keyValue);
        Assert.assertEquals(GenerationType.SEQUENCE, keyValue.getStrategy());
        Assert.assertNull(keyValue.getTableDiscriptor());
        Assert.assertNotNull(keyValue.getSequenceDiscriptor());
        Assert.assertEquals(20, keyValue.getSequenceDiscriptor().getAllocationSize());
        Assert.assertEquals(80, keyValue.getSequenceDiscriptor().getInitialValue());
        Assert.assertEquals("KunderaTest", keyValue.getSequenceDiscriptor().getSchemaName());
        Assert.assertEquals("newSequence", keyValue.getSequenceDiscriptor().getSequenceName());
        Assert.assertNull(keyValue.getSequenceDiscriptor().getCatalog());
        keyValue = null;

        // for entity GeneratedIdWithTableGenerator.
        keyValue = ((MetamodelImpl) metamodel).getKeyValue(GeneratedIdWithTableGenerator.class.getName());
        Assert.assertNotNull(keyValue);
        Assert.assertEquals(GenerationType.TABLE, keyValue.getStrategy());
        Assert.assertNull(keyValue.getSequenceDiscriptor());
        Assert.assertNotNull(keyValue.getTableDiscriptor());
        Assert.assertEquals(30, keyValue.getTableDiscriptor().getAllocationSize());
        Assert.assertEquals(100, keyValue.getTableDiscriptor().getInitialValue());
        Assert.assertEquals("KunderaTest", keyValue.getTableDiscriptor().getSchema());
        Assert.assertEquals("kundera", keyValue.getTableDiscriptor().getTable());
        Assert.assertEquals("sequence", keyValue.getTableDiscriptor().getPkColumnName());
        Assert.assertEquals("kk", keyValue.getTableDiscriptor().getPkColumnValue());
        Assert.assertEquals("sequenceValue", keyValue.getTableDiscriptor().getValueColumnName());
        Assert.assertNull(keyValue.getTableDiscriptor().getCatalog());
        Assert.assertNull(keyValue.getTableDiscriptor().getUniqueConstraints());
        keyValue = null;

        // for entity GeneratedIdStrategySequence.
        keyValue = ((MetamodelImpl) metamodel).getKeyValue(GeneratedIdWithOutSequenceGenerator.class.getName());
        Assert.assertNotNull(keyValue);
        Assert.assertEquals(GenerationType.SEQUENCE, keyValue.getStrategy());
        Assert.assertNull(keyValue.getTableDiscriptor());
        Assert.assertNotNull(keyValue.getSequenceDiscriptor());
        Assert.assertEquals(50, keyValue.getSequenceDiscriptor().getAllocationSize());
        Assert.assertEquals(1, keyValue.getSequenceDiscriptor().getInitialValue());
        Assert.assertEquals("KunderaTest", keyValue.getSequenceDiscriptor().getSchemaName());
        Assert.assertEquals("sequence_name", keyValue.getSequenceDiscriptor().getSequenceName());
        Assert.assertNull(keyValue.getSequenceDiscriptor().getCatalog());
        keyValue = null;

        // for entity GeneratedIdStrategyTable.
        keyValue = ((MetamodelImpl) metamodel).getKeyValue(GeneratedIdWithOutTableGenerator.class.getName());
        Assert.assertNotNull(keyValue);
        Assert.assertEquals(GenerationType.TABLE, keyValue.getStrategy());
        Assert.assertNull(keyValue.getSequenceDiscriptor());
        Assert.assertNotNull(keyValue.getTableDiscriptor());
        Assert.assertEquals(50, keyValue.getTableDiscriptor().getAllocationSize());
        Assert.assertEquals(1, keyValue.getTableDiscriptor().getInitialValue());
        Assert.assertEquals("KunderaTest", keyValue.getTableDiscriptor().getSchema());
        Assert.assertEquals("kundera_sequences", keyValue.getTableDiscriptor().getTable());
        Assert.assertEquals("sequence_name", keyValue.getTableDiscriptor().getPkColumnName());
        Assert.assertEquals("GeneratedIdWithOutTableGenerator", keyValue.getTableDiscriptor().getPkColumnValue());
        Assert.assertEquals("sequence_value", keyValue.getTableDiscriptor().getValueColumnName());
        Assert.assertNull(keyValue.getTableDiscriptor().getCatalog());
        Assert.assertNull(keyValue.getTableDiscriptor().getUniqueConstraints());
        keyValue = null;
    }
}
