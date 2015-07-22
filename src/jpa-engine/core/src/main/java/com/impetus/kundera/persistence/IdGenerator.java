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
package com.impetus.kundera.persistence;

import javax.persistence.GenerationType;
import javax.persistence.metamodel.Metamodel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.KunderaException;
import com.impetus.kundera.PersistenceProperties;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.client.ClientBase;
import com.impetus.kundera.generator.AutoGenerator;
import com.impetus.kundera.generator.Generator;
import com.impetus.kundera.generator.SequenceGenerator;
import com.impetus.kundera.generator.TableGenerator;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.IdDiscriptor;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.PersistenceUnitMetadata;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.property.PropertyAccessorHelper;
import com.impetus.kundera.utils.KunderaCoreUtils;

/**
 * Generate id for entity when {@GeneratedValue} annotation
 * given.
 * 
 * @author Kuldeep.Mishra
 * 
 */
public class IdGenerator
{
    /** The Constant log. */
    private static final Logger log = LoggerFactory.getLogger(IdGenerator.class);

    public Object generateAndSetId(Object e, EntityMetadata m, PersistenceDelegator pd,
            final KunderaMetadata kunderaMetadata)
    {
        Metamodel metamodel = KunderaMetadataManager.getMetamodel(kunderaMetadata, m.getPersistenceUnit());
        Client<?> client = pd.getClient(m);
        return generateId(e, m, client, kunderaMetadata);
    }

    private Object generateId(Object e, EntityMetadata m, Client<?> client, final KunderaMetadata kunderaMetadata)
    {
        Metamodel metamodel = KunderaMetadataManager.getMetamodel(kunderaMetadata, m.getPersistenceUnit());
        IdDiscriptor keyValue = ((MetamodelImpl) metamodel).getKeyValue(e.getClass().getName());

        if (keyValue != null)
        {

            if (!client.getQueryImplementor().getSimpleName().equalsIgnoreCase("RDBMSQuery"))
            {
                if (client != null)
                {
                    GenerationType type = keyValue.getStrategy();
                    switch (type)
                    {
                    case TABLE:
                        return onTableGenerator(m, client, keyValue, e);
                    case SEQUENCE:
                        return onSequenceGenerator(m, client, keyValue, e);
                    case AUTO:
                        return onAutoGenerator(m, client, e);
                    case IDENTITY:
                        throw new UnsupportedOperationException(GenerationType.class.getSimpleName() + "." + type
                                + " Strategy not supported by this client :" + client.getClass().getName());
                    }
                }
            }
            else
            {
                int hashCode = e.hashCode();
                Object generatedId = PropertyAccessorHelper.fromSourceToTargetClass(m.getIdAttribute().getJavaType(),
                        Integer.class, new Integer(hashCode));
                PropertyAccessorHelper.setId(e, m, generatedId);
                return generatedId;
            }
        }

        return null;
    }

    /**
     * Generate Id when given auto generation strategy.
     * 
     * @param m
     * @param client
     * @param e
     * @param kunderaMetadata
     */
    private Object onAutoGenerator(EntityMetadata m, Client<?> client, Object e)
    {
        Object autogenerator = getAutoGenClazz(client);
        
        if (autogenerator instanceof AutoGenerator)
        {
            
            Object generatedId = ((AutoGenerator)autogenerator).generate(client, m.getIdAttribute().getJavaType().getSimpleName());
            try
            {
                generatedId = PropertyAccessorHelper.fromSourceToTargetClass(m.getIdAttribute().getJavaType(),
                        generatedId.getClass(), generatedId);
                PropertyAccessorHelper.setId(e, m, generatedId);
                return generatedId;
            }
            catch (IllegalArgumentException iae)
            {
                log.error("Unknown data type for ids : " + m.getIdAttribute().getJavaType());
                throw new KunderaException("Unknown data type for ids : " + m.getIdAttribute().getJavaType(), iae);
            }
        }
        throw new IllegalArgumentException(GenerationType.class.getSimpleName() + "." + GenerationType.AUTO
                + " Strategy not supported by this client :" + client.getClass().getName());
    }

    private Generator getAutoGenClazz(Client<?> client)
    {
        Generator autoGenerator = null;
        String autoGen = ((ClientBase)client).getAutoGenerator();
        if (null != autoGen)
        {
            Class autogenClazz;
            try
            {
                autogenClazz = Class.forName(autoGen);
                autoGenerator = (Generator) (KunderaCoreUtils.createNewInstance(autogenClazz));
            }
            catch (ClassNotFoundException cnfe)
            {
                log.error("The autogen custom class is invalid");
                throw new KunderaException("The autogen custom class should implement AutoGenerator class", cnfe);
            }

        }
        else
        {

            autoGenerator = ((Generator) client.getIdGenerator());

        }
        return autoGenerator;
    }

    /**
     * Generate Id when given sequence generation strategy.
     * 
     * @param m
     * @param client
     * @param keyValue
     * @param e
     */
    private Object onSequenceGenerator(EntityMetadata m, Client<?> client, IdDiscriptor keyValue, Object e)
    {
        Object seqgenerator = getAutoGenClazz(client);
        if (seqgenerator instanceof SequenceGenerator)
        {
            Object generatedId = ((SequenceGenerator) seqgenerator).generate(
                    keyValue.getSequenceDiscriptor(), client, m.getIdAttribute().getJavaType().getSimpleName());
            try
            {
                generatedId = PropertyAccessorHelper.fromSourceToTargetClass(m.getIdAttribute().getJavaType(),
                        generatedId.getClass(), generatedId);
                PropertyAccessorHelper.setId(e, m, generatedId);
                return generatedId;
            }
            catch (IllegalArgumentException iae)
            {
                log.error("Unknown integral data type for ids : " + m.getIdAttribute().getJavaType());
                throw new KunderaException("Unknown integral data type for ids : " + m.getIdAttribute().getJavaType(),
                        iae);
            }
        }
        throw new IllegalArgumentException(GenerationType.class.getSimpleName() + "." + GenerationType.SEQUENCE
                + " Strategy not supported by this client :" + client.getClass().getName());
    }

    /**
     * Generate Id when given table generation strategy.
     * 
     * @param m
     * @param client
     * @param keyValue
     * @param e
     */
    private Object onTableGenerator(EntityMetadata m, Client<?> client, IdDiscriptor keyValue, Object e)
    {
        Object tablegenerator = getAutoGenClazz(client);   
        if (tablegenerator instanceof TableGenerator)
        {
            Object generatedId = ((TableGenerator) tablegenerator).generate(keyValue.getTableDiscriptor(),
                    (ClientBase) client, m.getIdAttribute().getJavaType().getSimpleName());
            try
            {
                generatedId = PropertyAccessorHelper.fromSourceToTargetClass(m.getIdAttribute().getJavaType(),
                        generatedId.getClass(), generatedId);
                PropertyAccessorHelper.setId(e, m, generatedId);
                return generatedId;
            }
            catch (IllegalArgumentException iae)
            {
                log.error("Unknown integral data type for ids : " + m.getIdAttribute().getJavaType());
                throw new KunderaException("Unknown integral data type for ids : " + m.getIdAttribute().getJavaType(),
                        iae);
            }
        }
        throw new IllegalArgumentException(GenerationType.class.getSimpleName() + "." + GenerationType.TABLE
                + " Strategy not supported by this client :" + client.getClass().getName());
    }
}
