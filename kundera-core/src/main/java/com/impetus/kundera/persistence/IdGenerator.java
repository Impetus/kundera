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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.impetus.kundera.KunderaException;
import com.impetus.kundera.client.Client;
import com.impetus.kundera.generator.AutoGenerator;
import com.impetus.kundera.generator.SequenceGenerator;
import com.impetus.kundera.generator.TableGenerator;
import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.IdDiscriptor;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.property.PropertyAccessorHelper;

public class IdGenerator
{
    /** The Constant log. */
    private static final Log log = LogFactory.getLog(PersistenceDelegator.class);

    void setGeneratedIdIfApplicable(Object e, EntityMetadata m, Client<?> client)
    {
        Metamodel metamodel = KunderaMetadataManager.getMetamodel(m.getPersistenceUnit());
        IdDiscriptor keyValue = ((MetamodelImpl) metamodel).getKeyValue(e.getClass().getName());
        if (keyValue != null)
        {
            GenerationType type = keyValue.getStrategy();
            switch (type)
            {
            case TABLE:
                onTableGenerator(m, client, keyValue, e);
                break;
            case SEQUENCE:
                onSequenceGenerator(m, client, keyValue, e);
                break;
            case AUTO:
                onAutoGenerator(m, client, e);
                break;
            case IDENTITY:
                throw new UnsupportedOperationException(GenerationType.class.getSimpleName() + "." + type
                        + " Strategy not supported by this client :" + client.getClass().getName());
            default:

                throw new IllegalArgumentException(GenerationType.class.getSimpleName() + "." + type
                        + " Strategy not supported by this client :" + client.getClass().getName());
            }
        }
    }

    private void onAutoGenerator(EntityMetadata m, Client<?> client, Object e)
    {
        if (client instanceof AutoGenerator)
        {
            Object generatedId = ((AutoGenerator) client).generate();
            try
            {
                PropertyAccessorHelper.setId(e, m, generatedId);
            }
            catch (IllegalArgumentException iae)
            {
                log.error("Unknown data type for ids : " + m.getIdAttribute().getJavaType());
                throw new KunderaException("Unknown data type for ids : " + m.getIdAttribute().getJavaType(), iae);
            }
        }
    }

    private void onSequenceGenerator(EntityMetadata m, Client<?> client, IdDiscriptor keyValue, Object e)
    {
        if (client instanceof SequenceGenerator)
        {
            Object generatedId = ((SequenceGenerator) client).generate(keyValue.getSequenceDiscriptor());
            try
            {
                PropertyAccessorHelper.setId(e, m, generatedId);
            }
            catch (IllegalArgumentException iae)
            {
                log.error("Unknown integral data type for ids : " + m.getIdAttribute().getJavaType());
                throw new KunderaException("Unknown integral data type for ids : " + m.getIdAttribute().getJavaType(),
                        iae);
            }
        }
    }

    private void onTableGenerator(EntityMetadata m, Client<?> client, IdDiscriptor keyValue, Object e)
    {
        if (client instanceof TableGenerator)
        {
            Object generatedId = ((TableGenerator) client).generate(keyValue.getTableDiscriptor());
            try
            {
                PropertyAccessorHelper.setId(e, m, generatedId);
            }
            catch (IllegalArgumentException iae)
            {
                log.error("Unknown integral data type for ids : " + m.getIdAttribute().getJavaType());
                throw new KunderaException("Unknown integral data type for ids : " + m.getIdAttribute().getJavaType(),
                        iae);
            }
        }
    }
}
