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
package com.impetus.kundera.metadata.processor.relation;

import java.lang.reflect.Field;

import javax.persistence.ManyToMany;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;

import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;

/**
 * A factory for creating RelationMetadataProcessor objects.
 * 
 * @author Amresh Singh
 */
public class RelationMetadataProcessorFactory
{

    /**
     * Gets the relation metadata processor.
     * 
     * @param relationField
     *            the relation field
     * @return the relation metadata processor
     */
    public static RelationMetadataProcessor getRelationMetadataProcessor(Field relationField, KunderaMetadata kunderaMetadata)
    {
        RelationMetadataProcessor relProcessor = null;

        // OneToOne
        if (relationField.isAnnotationPresent(OneToOne.class))
        {
            relProcessor = new OneToOneRelationMetadataProcessor(kunderaMetadata);
        }

        // OneToMany
        else if (relationField.isAnnotationPresent(OneToMany.class))
        {
            relProcessor = new OneToManyRelationMetadataProcessor(kunderaMetadata);

        }

        // ManyToOne
        else if (relationField.isAnnotationPresent(ManyToOne.class))
        {
            relProcessor = new ManyToOneRelationMetadataProcessor(kunderaMetadata);

        }

        // ManyToMany
        else if (relationField.isAnnotationPresent(ManyToMany.class))
        {
            relProcessor = new ManyToManyRelationMetadataProcessor(kunderaMetadata);

        }

        return relProcessor;

    }

}
