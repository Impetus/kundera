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
package com.impetus.kundera.validation.rules;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.AssociationOverride;
import javax.persistence.ElementCollection;
import javax.persistence.Embedded;
import javax.persistence.EmbeddedId;
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.ManyToMany;
import javax.persistence.ManyToOne;
import javax.persistence.MapKeyClass;
import javax.persistence.MapKeyJoinColumn;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.kundera.metadata.validator.InvalidEntityDefinitionException;
import com.impetus.kundera.property.PropertyAccessorHelper;

/**
 * @author Chhavi Gangwal
 *
 */
public class RelationAttributeRule extends AbstractFieldRule implements FieldRule
{

    /** The Constant log. */
    private static final Logger log = LoggerFactory.getLogger(RelationAttributeRule.class);

    /** The relation type map. */
    static enum RelationType
    {

        MANY_TO_MANY(ManyToMany.class.getSimpleName()), MANY_TO_ONE(ManyToOne.class.getSimpleName()), ONE_TO_MANY(
                OneToMany.class.getSimpleName()), ONE_TO_ONE(OneToOne.class.getSimpleName()), ELEMENT_COLLECTION(
                ElementCollection.class.getSimpleName()), EMBEDDED_ID(EmbeddedId.class.getSimpleName()), EMBEDDED(
                Embedded.class.getSimpleName());

        private String clazz;

        private static final Map<String, RelationType> lookup = new HashMap<String, RelationType>();

        static
        {
            for (RelationType s : EnumSet.allOf(RelationType.class))
            {
                lookup.put(s.getClazz(), s);
            }
        }

        /**
         * @param clazz
         */
        private RelationType(String clazz)
        {
            this.clazz = clazz;
        }

        /**
         * @return
         */
        public String getClazz()
        {
            return clazz;
        }

        /**
         * @param clazz
         * @return
         */
        public static RelationType get(String clazz)
        {
            return lookup.get(clazz);
        }
    }

    /**
     * @param annotationType
     * @return
     */
    private RelationType getRuleType(String annotationType)
    {

        if (RelationType.get(annotationType) != null)
        {
            return RelationType.get(annotationType);
        }
        else
        {
            return null;
        }

    }

    /* (non-Javadoc)
     * @see com.impetus.kundera.validation.rules.AbstractFieldRule#validate(java.lang.reflect.Field)
     */
    @Override
    public boolean validate(Field f) throws RuleValidationException
    {

        boolean checkvalidation = true;
        for (Annotation annotate : f.getDeclaredAnnotations())
        {
            RelationType eruleType = getRuleType(annotate.annotationType().getSimpleName());

            if (eruleType != null)
            {

                switch (eruleType)
                {
                case MANY_TO_MANY:
                    checkvalidation = validateManyToMany(f, annotate);
                    break;
                case MANY_TO_ONE:
                    checkvalidation = validateManyToOne(f, annotate);
                    break;
                case ONE_TO_MANY:
                    checkvalidation = validateOneToMany(f, annotate);
                    break;
                case ONE_TO_ONE:
                    checkvalidation = validateOneToOne(f, annotate);
                    break;

                }

            }
        }

        return checkvalidation;
    }

    /**
     * @param relationField
     * @param annotate
     * @return
     */
    private Boolean validateOneToOne(Field relationField, Annotation annotate)
    {
       

        boolean isJoinedByTable = relationField.isAnnotationPresent(JoinTable.class);

        if (relationField.isAnnotationPresent(AssociationOverride.class))
        {
            AssociationOverride annotation = relationField.getAnnotation(AssociationOverride.class);
            JoinColumn[] joinColumns = annotation.joinColumns();

            validateJoinColumns(joinColumns);

            JoinTable joinTable = annotation.joinTable();
            onJoinTable(joinTable);
        }
        else if (isJoinedByTable)
        {
            throw new UnsupportedOperationException("@JoinTable not supported for one to one association");
        }
        return true;
    }

    /**
     * @param relationField
     * @param annotate
     * @return
     * @throws RuleValidationException
     */
    private Boolean validateOneToMany(Field relationField, Annotation annotate) throws RuleValidationException
    {

        OneToMany ann = (OneToMany) annotate;
        Class<?> targetEntity = PropertyAccessorHelper.getGenericClass(relationField);

        // now, check annotations
        if (null != ann.targetEntity() && !ann.targetEntity().getSimpleName().equals("void"))
        {
            targetEntity = ann.targetEntity();
        }

        boolean isJoinedByTable = relationField.isAnnotationPresent(JoinTable.class);

        if (isJoinedByTable)
        {
            throw new UnsupportedOperationException("@JoinTable not supported for one to many association");
        }

        boolean isJoinedByColumn = relationField.isAnnotationPresent(JoinTable.class);

        return true;
    }

    /**
     * @param relationField
     * @param annotate
     * @return
     */
    private Boolean validateManyToOne(Field relationField, Annotation annotate)
    {
        // taking field's type as foreign entity, ignoring "targetEntity"

        Class<?> targetEntity = relationField.getType();

        boolean isJoinedByTable = relationField.isAnnotationPresent(JoinTable.class);
          
        if (relationField.isAnnotationPresent(AssociationOverride.class))
        {
            AssociationOverride annotation = relationField.getAnnotation(AssociationOverride.class);
            JoinColumn[] joinColumns = annotation.joinColumns();
            
            //validate if more than one  join column is defined
            validateJoinColumns(joinColumns);

            JoinTable joinTable = annotation.joinTable();
            //validate if join table is null
            onJoinTable(joinTable);
        }
        // join table not valid for Many to one check
        else if (isJoinedByTable)
        { 
            throw new UnsupportedOperationException("@JoinTable not supported for many to one association");
        }

        return true;
    }

    /**
     * @param relationField
     * @param annotate
     * @return
     * @throws RuleValidationException
     */
    private Boolean validateManyToMany(Field relationField, Annotation annotate) throws RuleValidationException
    {
        ManyToMany m2mAnnotation = (ManyToMany) annotate;

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
        //check if target entity is null
        if (targetEntity == null)
        {
            throw new InvalidEntityDefinitionException("Could not determine target entity class for relationship."
                    + " It should either be specified using targetEntity attribute of @ManyToMany or through generics");
        }
        //check if joined by foreign key
        if (isJoinedByFK)
        {
            throw new InvalidEntityDefinitionException(
                    "@JoinColumn not allowed for ManyToMany relationship. Use @JoinTable instead");

        }
      //check if joined by foreign key and join column name is set
        if (isJoinedByMap)
        {

            MapKeyJoinColumn mapKeyJoinColumnAnn = relationField.getAnnotation(MapKeyJoinColumn.class);
            if (mapKeyJoinColumnAnn != null)
            {
                String mapKeyJoinColumnName = mapKeyJoinColumnAnn.name();
                if (StringUtils.isEmpty(mapKeyJoinColumnName))
                {
                    throw new InvalidEntityDefinitionException(
                            "It's mandatory to specify name attribute with @MapKeyJoinColumn annotation");
                }
            }

        }
        //check if not joined by table in many to many
        if (!isJoinedByTable && !isJoinedByMap
                && (m2mAnnotation.mappedBy() == null || m2mAnnotation.mappedBy().isEmpty()))
        {
            throw new InvalidEntityDefinitionException(
                    "It's manadatory to use @JoinTable with parent side of ManyToMany relationship.");
        }
        return true;
    }

    /**
     * @param joinTable
     */
    private void onJoinTable(JoinTable joinTable)
    {
        if (joinTable != null)
        {
            throw new UnsupportedOperationException("@JoinTable not supported for many to one association");
        }
    }

    /**
     * @param joinColumns
     */
    private void validateJoinColumns(JoinColumn[] joinColumns)
    {
        if (joinColumns.length > 1)
        {
            throw new UnsupportedOperationException("More than one join columns are not supported.");
        }
    }

}
