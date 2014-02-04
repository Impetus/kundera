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
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.metamodel.Attribute;
import javax.persistence.metamodel.EntityType;
import javax.validation.ValidationException;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.metadata.processor.MetaModelBuilder;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.validation.ValidationFactory;
import com.impetus.kundera.validation.ValidationFactoryGenerator;
import com.impetus.kundera.validation.ValidationFactoryGenerator.ValidationFactoryType;

import com.impetus.kundera.validation.rules.AttributeConstraintRule;





public class ValidationProcessorTest
{
    
    /** the log used by this class. */
    private static Logger log = LoggerFactory.getLogger(ValidationProcessorTest.class);

      
    private static final String PU = "kunderatest";

    private EntityManagerFactory emf;

    private EntityManager em;

    protected Map propertyMap = null;
    
    private KunderaMetadata kunderaMetadata;

    @Before
    public void setUp() throws Exception
    {
            emf = Persistence.createEntityManagerFactory(PU, propertyMap);
            kunderaMetadata = ((EntityManagerFactoryImpl) emf).getKunderaMetadataInstance();
            //kunderaMetadata.setApplicationMetadata(null);
            
            em = emf.createEntityManager();

    }

    @Test
    public void testValid() throws ParseException{
        
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
        entity.setNullField(0);
        entity.setPast(pastDate);
        entity.setFuture(futDate);
        entity.setSize(30);
        entity.setEmail("abc@gcd.com");
        validateEntityAttributes(entity);
        
        em.persist(entity);
        
    }
    
    @Test
    public void testNull() throws ParseException{
        
              
        
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
        entity.setNullField(0);
        entity.setPast(null);
        entity.setFuture(null);
        entity.setSize(40);
        entity.setEmail(null);
        validateEntityAttributes(entity);
        
        em.persist(entity);
        
    }
    
    @Test
    public void testInValid() throws ParseException{
        
        String pastStr = "11-11-2015";
        String futStr = "11-11-2012";
        DateFormat df = new SimpleDateFormat("dd-mm-yyyy"); 
        Date pastDate = df.parse(pastStr);
        Date futDate = df.parse(futStr);
        
        
        ValidationEntity entity = new ValidationEntity();
        entity.setAge(13);
        entity.seteId("e1");
        entity.setHuman(true);
        entity.setDecimalMax(5);
        entity.setDecimalMin(59);
        entity.setDigits(123);
        entity.setIHuman(true);
        entity.setMax("90");
        entity.setMin(20);
        entity.setNullField(0);
        entity.setPast(pastDate);
        entity.setFuture(futDate);
        entity.setSize(0);
        entity.setEmail("axc");
        validateEntityAttributes(entity);
        
        em.persist(entity);
        
    }
    
    
    /**
     * @param <X>
     * @param <T>
     * @return
     */
    private <X extends Class, T extends Object> void validateEntityAttributes(Object validationObject)
    {
       
        EntityMetadata entityMetadata = KunderaMetadataManager.getEntityMetadata(kunderaMetadata, validationObject.getClass());
        MetaModelBuilder<X, T> metaModelBuilder = kunderaMetadata.getApplicationMetadata()
                .getMetaModelBuilder(entityMetadata.getPersistenceUnit());
        EntityType entityType = (EntityType) metaModelBuilder.getManagedTypes().get(entityMetadata.getEntityClazz());

        Set<Attribute> attributes = entityType.getAttributes();

        Iterator<Attribute> iter = attributes.iterator();

        while (iter.hasNext())
        {
            Attribute attribute = iter.next();

            Field f = (Field) ((Field) attribute.getJavaMember());
            
            ValidationFactoryGenerator generator = new ValidationFactoryGenerator();
            ValidationFactory factory = generator.getFactory(ValidationFactoryType.BOOT_STRAP_VALIDATION);
          
            boolean isValid = true;
            
            try
            {
              isValid = factory.validate(f, validationObject, new AttributeConstraintRule());
              
            }
            catch (ValidationException e)
            {
           
                 Assert.assertNotNull(e.getMessage());
            }

        }
        
        
        

    }
}
