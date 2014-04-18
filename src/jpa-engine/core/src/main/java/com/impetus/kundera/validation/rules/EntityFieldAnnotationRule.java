package com.impetus.kundera.validation.rules;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import javassist.Modifier;

import javax.persistence.Column;
import javax.persistence.ElementCollection;
import javax.persistence.Embedded;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.ManyToMany;
import javax.persistence.ManyToOne;
import javax.persistence.MappedSuperclass;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;
import javax.persistence.TableGenerator;
import javax.persistence.Transient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EntityFieldAnnotationRule extends AbstractEntityRule implements EntityRule
{
    /** The Constant log. */
    private static final Logger log = LoggerFactory.getLogger(EntityFieldAnnotationRule.class);
    @Override
    public void validate(Class<?> clazz)
    {

      
        // Check for @Key and ensure that there is just 1 @Key field of
        // String
        // type.
        List<Field> keys = new ArrayList<Field>();
        for (Field field : clazz.getDeclaredFields())
        {
//            if (checkValidField(field))
//            {
                onIdField(field, clazz);
                if (field.isAnnotationPresent(Id.class))
                {
                    keys.add(field);
                    // validate @GeneratedValue annotation if given
                    if (field.isAnnotationPresent(GeneratedValue.class))
                    {
                        validateGeneratedValueAnnotation(clazz, field);
                    }
                }
                else if (field.isAnnotationPresent(EmbeddedId.class))
                {
                    keys.add(field);
                }
//            }
//            else
//            {
//
//                throw new RuleValidationException(" Invalid attribute annotation defined for : "
//                        + field.getName() + " of class " + clazz.getName());
//            }
        }

        // Check for a class key only in case it's of type entity or
        // MappedSuperClass
        if (clazz.isAnnotationPresent(Entity.class) || clazz.isAnnotationPresent(MappedSuperclass.class))
        {
            onSuperClass(clazz, keys);
        }

    }

    /**
     * Checks whether the defined class is a validone with an id field present
     * either in class itself or its superclass
     * 
     * @param clazz
     * @param keys
     * @throws RuleValidationException
     */
    private void onSuperClass(Class<?> clazz, List<Field> keys)
    {

        Class<?> superClass = clazz.getSuperclass();
        if (superClass != null
                && (superClass.isAnnotationPresent(MappedSuperclass.class) || superClass
                        .isAnnotationPresent(Entity.class)))
        {

            while (superClass != null
                    && (superClass.isAnnotationPresent(MappedSuperclass.class) || superClass
                            .isAnnotationPresent(Entity.class)))
            {
                for (Field field : superClass.getDeclaredFields())
                {

//                    if (checkValidField(field))
//                    {

                        onIdField(field, superClass);
                        if (field.isAnnotationPresent(Id.class))
                        {
                            keys.add(field);
                            // validate @GeneratedValue annotation if given
                            if (field.isAnnotationPresent(GeneratedValue.class))
                            {
                                validateGeneratedValueAnnotation(superClass, field);
                            }
                        }
                        else if (field.isAnnotationPresent(EmbeddedId.class))
                        {
                            keys.add(field);
                        }

                   // }
                }

                if (keys.size() > 0)
                {
                    onEntityKey(keys, superClass);
                    break;
                }
                superClass = superClass.getSuperclass();

            }
        }

        onEntityKey(keys, clazz);

    }

    /**
     * @param field
     * @param clazz
     */
    private void onIdField(Field field, Class<?> clazz)
    {

        if (field.isAnnotationPresent(Id.class) && field.isAnnotationPresent(EmbeddedId.class))
        {

            throw new RuleValidationException(clazz.getName()
                    + " must have either @Id field or @EmbeddedId field");
        }

    }

    /**
     * @param keys
     * @param clazz
     */
    private void onEntityKey(List<Field> keys, Class<?> clazz)
    {

        if (keys.size() <= 0)
        {

            throw new RuleValidationException(clazz.getName() + " must have an @Id field.");

        }
        else if (keys.size() > 1)
        {

            throw new RuleValidationException(clazz.getName() + " can only have 1 @Id field.");

        }

    }

    /**
     * @param field
     * @return
     */
    private boolean checkValidField(Field field)
    {
        if (field != null && !Modifier.isStatic(field.getModifiers()) && !Modifier.isTransient(field.getModifiers())
                && !field.isAnnotationPresent(Transient.class))
        {
            return field.isAnnotationPresent(Id.class) || field.isAnnotationPresent(EmbeddedId.class)
                    || field.isAnnotationPresent(Column.class) || field.isAnnotationPresent(ManyToMany.class)
                    || field.isAnnotationPresent(ManyToOne.class) || field.isAnnotationPresent(OneToOne.class)
                    || field.isAnnotationPresent(Embedded.class) || field.isAnnotationPresent(OneToMany.class)
                    || field.isAnnotationPresent(ElementCollection.class);
        }
        else
        {
            return true;
        }

    }

   
    /**
     * validate generated value annotation if given.
     * 
     * @param clazz
     * @param field
     * @throws RuleValidationException
     */
    private void validateGeneratedValueAnnotation(final Class<?> clazz, Field field)
    {

        Table table = clazz.getAnnotation(Table.class);
        // Still we need to validate for this after post metadata
        // population.
        if (table != null)
        {
            String schemaName = table.schema();
            if (schemaName != null && schemaName.indexOf('@') > 0)
            {
                schemaName = schemaName.substring(0, schemaName.indexOf('@'));
                GeneratedValue generatedValue = field.getAnnotation(GeneratedValue.class);
                if (generatedValue != null && generatedValue.generator() != null
                        && !generatedValue.generator().isEmpty())
                {
                    if (!(field.isAnnotationPresent(TableGenerator.class)
                            || field.isAnnotationPresent(SequenceGenerator.class)
                            || clazz.isAnnotationPresent(TableGenerator.class) || clazz
                            .isAnnotationPresent(SequenceGenerator.class)))
                    {

                        throw new IllegalArgumentException("Unknown Id.generator: " + generatedValue.generator());
                    }
                    else
                    {
                        checkForGenerator(clazz, field, generatedValue, schemaName);
                    }
                }
            }
        }

    }

    /**
     * Validate for generator.
     * 
     * @param clazz
     * @param field
     * @param generatedValue
     * @param schemaName
     * @throws RuleValidationException
     */
    private void checkForGenerator(final Class<?> clazz, Field field, GeneratedValue generatedValue, String schemaName)

    {

        TableGenerator tableGenerator = field.getAnnotation(TableGenerator.class);
        SequenceGenerator sequenceGenerator = field.getAnnotation(SequenceGenerator.class);
        if (tableGenerator == null || !tableGenerator.name().equals(generatedValue.generator()))
        {
            tableGenerator = clazz.getAnnotation(TableGenerator.class);
        }
        if (sequenceGenerator == null || !sequenceGenerator.name().equals(generatedValue.generator()))
        {
            sequenceGenerator = clazz.getAnnotation(SequenceGenerator.class);
        }

        if ((tableGenerator == null && sequenceGenerator == null)
                || (tableGenerator != null && !tableGenerator.name().equals(generatedValue.generator()))
                || (sequenceGenerator != null && !sequenceGenerator.name().equals(generatedValue.generator())))
        {

            throw new RuleValidationException("Unknown Id.generator: " + generatedValue.generator());

        }
        else if ((tableGenerator != null && !tableGenerator.schema().isEmpty() && !tableGenerator.schema().equals(
                schemaName))
                || (sequenceGenerator != null && !sequenceGenerator.schema().isEmpty() && !sequenceGenerator.schema()
                        .equals(schemaName)))
        {

            throw new RuleValidationException("Generator " + generatedValue.generator() + " in entity : "
                    + clazz.getName() + " has different schema name ,it should be same as entity have");

        }

    }


}
