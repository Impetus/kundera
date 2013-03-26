package com.impetus.kundera.metadata.validator;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class EntityValidatorImplTest
{
    private EntityValidatorImpl validator;

    @Before
    public void setUp() throws Exception
    {
        validator = new EntityValidatorImpl();
    }

    @After
    public void tearDown() throws Exception
    {
        validator = null;
    }

    @Test
    public void testValidate()
    {
        try
        {
            validator.validate(GeneratedIdDefault.class);
        }
        catch (IllegalArgumentException e)
        {
            Assert.fail();
        }
        try
        {
            validator.validate(GeneratedIdStrategyAuto.class);
        }
        catch (IllegalArgumentException e)
        {
            Assert.fail();
        }
        try
        {
            validator.validate(GeneratedIdStrategyIdentity.class);
        }
        catch (IllegalArgumentException e)
        {
            Assert.fail();
        }
        try
        {
            validator.validate(GeneratedIdStrategySequence.class);
        }
        catch (IllegalArgumentException e)
        {
            Assert.fail();
        }
        try
        {
            validator.validate(GeneratedIdWithSequenceGenerator.class);
        }
        catch (IllegalArgumentException e)
        {
            Assert.fail();
        }
        try
        {
            validator.validate(GeneratedIdWithTableGenerator.class);
        }
        catch (IllegalArgumentException e)
        {
            Assert.fail();
        }
        try
        {
            validator.validate(GeneratedIdWithOutSequenceGenerator.class);
        }
        catch (IllegalArgumentException e)
        {
            Assert.fail();
        }
        try
        {
            validator.validate(GeneratedIdWithOutTableGenerator.class);
        }
        catch (IllegalArgumentException e)
        {
            Assert.fail();
        }
        try
        {
            validator.validate(GeneratedIdStrategyTable.class);
        }
        catch (IllegalArgumentException e)
        {
            Assert.fail();
        }

        try
        {
            validator.validate(GeneratedIdWithNoGenerator.class);
            Assert.fail();
        }
        catch (IllegalArgumentException e)
        {
            Assert.assertEquals("Unknown Id.generator: id", e.getMessage());
        }

        try
        {
            validator.validate(GeneratedIdWithInvalidGenerator.class);
            Assert.fail();
        }
        catch (IllegalArgumentException e)
        {
            Assert.assertEquals("Unknown Id.generator: id", e.getMessage());
        }
    }

}
