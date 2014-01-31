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
import java.util.List;
import java.util.Map;

import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.ManyToMany;
import javax.persistence.MapKeyClass;
import javax.persistence.MapKeyJoinColumn;

import org.apache.commons.lang.StringUtils;

import com.impetus.kundera.loader.MetamodelLoaderException;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.JoinTableMetadata;
import com.impetus.kundera.metadata.model.Relation;
import com.impetus.kundera.metadata.processor.AbstractEntityFieldProcessor;
import com.impetus.kundera.metadata.validator.EntityValidatorImpl;
import com.impetus.kundera.metadata.validator.InvalidEntityDefinitionException;
import com.impetus.kundera.persistence.EntityManagerFactoryImpl.KunderaMetadata;
import com.impetus.kundera.property.PropertyAccessorHelper;

/**
 * The Class ManyToManyRelationMetadataProcessor.
 * 
 * @author Amresh Singh
 */
public class ManyToManyRelationMetadataProcessor extends AbstractEntityFieldProcessor implements
        RelationMetadataProcessor
{

    /**
     * Instantiates a new many to many relation metadata processor.
     */
    public ManyToManyRelationMetadataProcessor(KunderaMetadata kunderaMetadata)
    {
        super(kunderaMetadata);
        validator = new EntityValidatorImpl();
    }

    @Override
    public void addRelationIntoMetadata(Field relationField, EntityMetadata metadata)
    {
        ManyToMany m2mAnnotation = relationField.getAnnotation(ManyToMany.class);

        boolean isJoinedByFK = relationField.isAnnotationPresent(JoinColumn.class);
        boolean isJoinedByTable = relationField.isAnnotationPresent(JoinTable.class);
        boolean isJoinedByMap = false;
        if (m2mAnnotation != null && relationField.getType().isAssignableFrom(Map.class))
        {
            isJoinedByMap = true;
        }

        Class<?> targetEntity = null;
        Class<?> mapKeyClass = null;

        if (!isJoinedByMap)
        {

            targetEntity = PropertyAccessorHelper.getGenericClass(relationField);
        }
        else
        {
            List<Class<?>> genericClasses = PropertyAccessorHelper.getGenericClasses(relationField);

            if (!genericClasses.isEmpty() && genericClasses.size() == 2)
            {
                mapKeyClass = genericClasses.get(0);
                targetEntity = genericClasses.get(1);
            }

            MapKeyClass mapKeyClassAnn = relationField.getAnnotation(MapKeyClass.class);

            // Check for Map key class specified at annotation
            if (mapKeyClass == null && mapKeyClassAnn != null && mapKeyClassAnn.value() != null
                    && !mapKeyClassAnn.value().getSimpleName().equals("void"))
            {
                mapKeyClass = mapKeyClassAnn.value();
            }

            if (mapKeyClass == null)
            {
                throw new InvalidEntityDefinitionException(
                        "For a Map relationship field,"
                                + " it is mandatory to specify Map key class either using @MapKeyClass annotation or through generics");
            }

        }

        // Check for target class specified at annotation
        if (targetEntity == null && null != m2mAnnotation.targetEntity()
                && !m2mAnnotation.targetEntity().getSimpleName().equals("void"))
        {
            targetEntity = m2mAnnotation.targetEntity();
        }

        if (targetEntity == null)
        {
            throw new InvalidEntityDefinitionException("Could not determine target entity class for relationship."
                    + " It should either be specified using targetEntity attribute of @ManyToMany or through generics");
        }

        validate(targetEntity);
       
        Relation relation = new Relation(relationField, targetEntity, relationField.getType(), m2mAnnotation.fetch(),
                Arrays.asList(m2mAnnotation.cascade()), Boolean.TRUE, m2mAnnotation.mappedBy(),
                Relation.ForeignKey.MANY_TO_MANY);

        if (isJoinedByFK)
        {
            throw new InvalidEntityDefinitionException(
                    "@JoinColumn not allowed for ManyToMany relationship. Use @JoinTable instead");

        }

        if (isJoinedByTable)
        {
            JoinTableMetadata jtMetadata = new JoinTableMetadata(relationField);

            relation.setRelatedViaJoinTable(true);
            relation.setJoinTableMetadata(jtMetadata);
        }

        if (isJoinedByMap)
        {
            relation.setMapKeyJoinClass(mapKeyClass);

            MapKeyJoinColumn mapKeyJoinColumnAnn = relationField.getAnnotation(MapKeyJoinColumn.class);
            if (mapKeyJoinColumnAnn != null)
            {
                String mapKeyJoinColumnName = mapKeyJoinColumnAnn.name();
                if (!StringUtils.isEmpty(mapKeyJoinColumnName))
                {
                    relation.setJoinColumnName(mapKeyJoinColumnName);
                }
                else
                {
                    throw new InvalidEntityDefinitionException(
                            "It's mandatory to specify name attribute with @MapKeyJoinColumn annotation");
                }
            }

        }

        else if (!isJoinedByTable && !isJoinedByMap
                && (relation.getMappedBy() == null || relation.getMappedBy().isEmpty()))
        {
            throw new InvalidEntityDefinitionException(
                    "It's manadatory to use @JoinTable with parent side of ManyToMany relationship.");
        }

        relation.setBiDirectionalField(metadata.getEntityClazz());
        metadata.addRelation(relationField.getName(), relation);

        // Set whether this entity has at least one Join table relation, if not
        // already set
        if (!metadata.isRelationViaJoinTable())
        {
            metadata.setRelationViaJoinTable(relation.isRelatedViaJoinTable());
        }

    }

    @Override
    public void process(Class<?> clazz, EntityMetadata metadata)
    {
        throw new MetamodelLoaderException("Method call not applicable for Relation processors");
    }

}
