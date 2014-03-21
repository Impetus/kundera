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
import java.util.Arrays;

import javax.persistence.AssociationOverride;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;

import org.apache.commons.lang.StringUtils;

import com.impetus.kundera.loader.MetamodelLoaderException;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.metadata.processor.AbstractEntityFieldProcessor;
import com.impetus.kundera.metadata.validator.EntityValidatorImpl;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;

/**
 * The Class ManyToOneRelationMetadataProcessor.
 * 
 * @author Amresh Singh
 */
public class ManyToOneRelationMetadataProcessor extends AbstractEntityFieldProcessor implements
        RelationMetadataProcessor
{

    /**
     * Instantiates a new many to one relation metadata processor.
     */
    public ManyToOneRelationMetadataProcessor(KunderaMetadata kunderaMetadata)
    {
        super(kunderaMetadata);
        validator = new EntityValidatorImpl();
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.metadata.processor.relation.RelationMetadataProcessor
     * #addRelationIntoMetadata(java.lang.reflect.Field,
     * com.impetus.kundera.metadata.model.EntityMetadata)
     */
    @Override
    public void addRelationIntoMetadata(Field relationField, EntityMetadata metadata)
    {
        // taking field's type as foreign entity, ignoring "targetEntity"
        Class<?> targetEntity = relationField.getType();

        ManyToOne ann = relationField.getAnnotation(ManyToOne.class);
        Relation relation = new Relation(relationField, targetEntity, null, ann.fetch(), Arrays.asList(ann.cascade()),
                ann.optional(), null, // mappedBy is null
                Relation.ForeignKey.MANY_TO_ONE);

        boolean isJoinedByFK = relationField.isAnnotationPresent(JoinColumn.class);

        if (relationField.isAnnotationPresent(AssociationOverride.class))
        {
            AssociationOverride annotation = relationField.getAnnotation(AssociationOverride.class);
            JoinColumn[] joinColumns = annotation.joinColumns();

            relation.setJoinColumnName(joinColumns[0].name());

        }
        else if (isJoinedByFK)
        {
            JoinColumn joinColumnAnn = relationField.getAnnotation(JoinColumn.class);
            relation.setJoinColumnName(StringUtils.isBlank(joinColumnAnn.name()) ? relationField.getName()
                    : joinColumnAnn.name());
        }
        else
        {
            relation.setJoinColumnName(relationField.getName());
        }

        relation.setBiDirectionalField(metadata.getEntityClazz());
        metadata.addRelation(relationField.getName(), relation);

    }

    @Override
    public void process(Class<?> clazz, EntityMetadata metadata)
    {
        throw new MetamodelLoaderException("Method call not applicable for Relation processors");
    }

}
