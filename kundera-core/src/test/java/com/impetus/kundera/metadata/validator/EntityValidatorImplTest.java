/*******************************************************************************
 * * Copyright 2013 Impetus Infotech.
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

package com.impetus.kundera.metadata.validator;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Kuldeep
 *  Entity validator impl test.
 */
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
        
        try
        {
            validator.validate(String.class);
        } catch(InvalidEntityDefinitionException iedfx)
        {
            Assert.assertEquals(String.class.getName() + " is not annotated with @Entity.", iedfx.getMessage());
        }
        
        try
        {
            validator.validate(EntityWithOutTableAnnotation.class);
        } catch(InvalidEntityDefinitionException iedfx)
        {
            Assert.assertEquals(EntityWithOutTableAnnotation.class.getName() + " must be annotated with @Table.", iedfx.getMessage());
        }
        
        try
        {
            validator.validate(EntityWithOutConstructor.class);
        } catch(InvalidEntityDefinitionException iedfx)
        {
            Assert.assertEquals(EntityWithOutConstructor.class.getName() + " must have a default no-argument constructor.", iedfx.getMessage());
        }
        
        try
        {
            validator.validate(EntityWithOutId.class);
        } catch(InvalidEntityDefinitionException iedfx)
        {
            Assert.assertEquals(EntityWithOutId.class.getName() + " must have an @Id field.", iedfx.getMessage());
        }

        try
        {
            validator.validate(EntityWithMultipleId.class);
        } catch(InvalidEntityDefinitionException iedfx)
        {
            Assert.assertEquals(EntityWithMultipleId.class.getName() + " can only have 1 @Id field.", iedfx.getMessage());
        }

    }

}
