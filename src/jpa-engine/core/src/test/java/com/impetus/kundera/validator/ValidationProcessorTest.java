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
package com.impetus.kundera.validator;

import java.lang.reflect.Field;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.metamodel.EntityType;
import javax.validation.ValidationException;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.KunderaException;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.processor.MetaModelBuilder;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.validation.ValidationFactory;
import com.impetus.kundera.validation.ValidationFactoryGenerator;
import com.impetus.kundera.validation.ValidationFactoryGenerator.ValidationFactoryType;
import com.impetus.kundera.validation.rules.AttributeConstraintRule;

/**
 * @author Chhavi Gangwal
 * 
 */
public class ValidationProcessorTest
{

    /** the log used by this class. */
    private static Logger log = LoggerFactory.getLogger(ValidationProcessorTest.class);

    private static final String PU = "kunderatest";

    private EntityManagerFactory emf;

    private EntityManager em;

    protected Map propertyMap = null;

    private KunderaMetadata kunderaMetadata;

    private ValidationFactoryGenerator generator = new ValidationFactoryGenerator();

    private ValidationFactory factory = generator.getFactory(ValidationFactoryType.BOOT_STRAP_VALIDATION);

    /**
     * @throws Exception
     */
    @Before
    public void setUp() throws Exception
    {
        emf = Persistence.createEntityManagerFactory(PU, propertyMap);
        kunderaMetadata = ((EntityManagerFactoryImpl) emf).getKunderaMetadataInstance();
        // kunderaMetadata.setApplicationMetadata(null);

        em = emf.createEntityManager();

    }

    /**
     * @throws ParseException
     */
    @Test
    public void testValid() throws ParseException
    {
        try
        {
            String pastStr = "11-11-2012";
            String futStr = "11-11-2015";
            DateFormat df = new SimpleDateFormat("dd-mm-yyyy");
            Date pastDate = df.parse(pastStr);
            Date futDate = df.parse(futStr);

            ValidationEntity entity = new ValidationEntity();
            entity.setAge(13);
            entity.seteId("e1");
            entity.setHuman(false);
            entity.setDecimalMax(5);
            entity.setDecimalMin(59);
            entity.setDigits(123);
            entity.setIHuman(true);
            entity.setMax("90");
            entity.setMin(20);
            entity.setNullField(null);
            entity.setPast(pastDate);
            entity.setFuture(futDate);
            entity.setSize("hello232141423423535353453");
            entity.setEmail("abc@gcd.com");
            
            validateEntityAttribute("age", entity, "Age should not be null");
            validateEntityAttribute("nullField", entity, "The value should be null.");
            validateEntityAttribute("isHuman", entity, "The  person type must be human");
            validateEntityAttribute("isIHuman", entity, "The person type must be I-human");
            validateEntityAttribute("decimalMax", entity, "Invalid Decimal max value.");
            validateEntityAttribute("decimalMin", entity, "Invalid Decimal min value.");
            validateEntityAttribute("digits", entity, "Invalid number.");
            validateEntityAttribute("future", entity, "Invalid future date.");
            validateEntityAttribute("past", entity, "Invalid past date");
            validateEntityAttribute("pattern", entity, "Invalid pattern.");
            validateEntityAttribute("size", entity, "Invalid size.");
            validateEntityAttribute("max", entity, "Invalid max value.");
            validateEntityAttribute("min", entity, "Invalid min value.");

            //em.persist(entity);
        }
        catch (KunderaException e)
        {
            Assert.assertNotNull(e.getMessage());
        }

    }

    /**
     * @throws ParseException
     */
    @Test
    public void testNull() throws ParseException
    {
        try
        {

            ValidationEntity entity = new ValidationEntity();
            entity.setAge(13);
            entity.seteId("e1");
            entity.setHuman(false);
            entity.setDecimalMax(5);
            entity.setDecimalMin(59);
            entity.setDigits(123);
            entity.setIHuman(true);
            entity.setMax("90");
            entity.setMin(21);
            entity.setNullField(null);
            entity.setPast(null);
            entity.setFuture(null);
            entity.setSize(null);
            entity.setEmail(null);

            validateEntityAttribute("age", entity, "");
            validateEntityAttribute("nullField", entity, "");
            validateEntityAttribute("isHuman", entity, "");
            validateEntityAttribute("isIHuman", entity, "");
            validateEntityAttribute("decimalMax", entity, "");
            validateEntityAttribute("decimalMin", entity, "");
            validateEntityAttribute("digits", entity, "");
            validateEntityAttribute("future", entity, "");
            validateEntityAttribute("past", entity, "");
            validateEntityAttribute("pattern", entity, "");
            validateEntityAttribute("size", entity, "");
            validateEntityAttribute("max", entity, "");
            validateEntityAttribute("min", entity, "");
        }
        catch (KunderaException e)
        {

            Assert.assertNotNull(e.getMessage());
        }

    }

    /**
     * @throws ParseException
     */
    @Test
    public void testInValid() throws ParseException
    {
        try
        {
            String pastStr = "11-11-2015";
            String futStr = "11-11-2012";
            DateFormat df = new SimpleDateFormat("dd-mm-yyyy");
            Date pastDate = df.parse(pastStr);
            Date futDate = df.parse(futStr);

            ValidationEntity entity = new ValidationEntity();
            entity.setAge(13);
            entity.seteId("e1");
            entity.setHuman(true);
            entity.setDecimalMax(50);
            entity.setDecimalMin(5);
            entity.setDigits(123);
            entity.setIHuman(false);
            entity.setMax("90");
            entity.setMin(20);
            entity.setNullField("hello");
            entity.setPast(pastDate);
            entity.setFuture(futDate);
            entity.setSize("123");
            entity.setEmail("axc");

            validateEntityAttribute("age", entity, "Age should not be null");
            validateEntityAttribute("nullField", entity, "The value should be null.");
            validateEntityAttribute("isHuman", entity, "The  person type must be human");
            validateEntityAttribute("isIHuman", entity, "The person type must be I-human");
            validateEntityAttribute("decimalMax", entity, "Invalid Decimal max value.");
            validateEntityAttribute("decimalMin", entity, "Invalid Decimal min value.");
            validateEntityAttribute("digits", entity, "Invalid number.");
            validateEntityAttribute("future", entity, "Invalid future date.");
            validateEntityAttribute("past", entity, "Invalid past date");
            validateEntityAttribute("pattern", entity, "Invalid pattern.");
            validateEntityAttribute("size", entity, "Invalid size.");
            validateEntityAttribute("max", entity, "Invalid max value.");
            validateEntityAttribute("min", entity, "Invalid min value.");

            em.persist(entity);
        }
        catch (KunderaException e)
        {

            Assert.assertNotNull(e.getMessage());
        }

    }

    /**
     * @param <X>
     * @param <T>
     * @return
     */
    private <X extends Class, T extends Object> void validateEntityAttribute(String fieldname, Object validationObject,
            String message)
    {

        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata,
                validationObject.getClass());
        MetaModelBuilder<X, T> metaModelBuilder = kunderaMetadata.getApplicationMetadata().getMetaModelBuilder(
                entityMetadata.getPersistenceUnit());
        EntityType entityType = (EntityType) metaModelBuilder.getManagedTypes().get(entityMetadata.getEntityClazz());

        Field field = (Field) entityType.getAttribute(fieldname).getJavaMember();

        try
        {
            factory.validate(field, validationObject, new AttributeConstraintRule());

        }
        catch (ValidationException e)
        {
           
            Assert.assertEquals(message, e.getMessage());
            Assert.assertNotNull(e.getMessage());
        }

    }
}
