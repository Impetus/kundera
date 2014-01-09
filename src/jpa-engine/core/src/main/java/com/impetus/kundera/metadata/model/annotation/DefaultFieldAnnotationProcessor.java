package com.impetus.kundera.metadata.model.annotation;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.Column;
import javax.persistence.Table;
import javax.persistence.metamodel.ManagedType;

import com.impetus.kundera.metadata.model.type.AbstractManagedType;
import com.impetus.kundera.metadata.validator.InvalidEntityDefinitionException;

public class DefaultFieldAnnotationProcessor implements FieldAnnotationProcessor
{
    private Map<String, Annotation> fieldAnnotations;

    public DefaultFieldAnnotationProcessor(Field field)
    {
        fieldAnnotations = new HashMap<String, Annotation>();
        processFieldAnnotations(field);
    }

    @Override
    public Map<String, Annotation> getAnnotations()
    {
        return fieldAnnotations;
    }

    @Override
    public Annotation getAnnotation(String annotationName)
    {
        return fieldAnnotations.get(annotationName);
    }

    @Override
    public void validateFieldAnnotation(Annotation annotation, Field field, ManagedType managedType)
    {

        List<String> tables = ((DefaultEntityAnnotationProcessor) ((AbstractManagedType) managedType)
                .getEntityAnnotation()).getSecondaryTablesName();

        Annotation primaryTableannotation = ((AbstractManagedType) managedType).getEntityAnnotation().getAnnotation(
                Table.class.getName());

        String primaryTableName = "";
        if (primaryTableannotation != null)
        {
            primaryTableName = ((Table) primaryTableannotation).name();
        }

        String tableNameOfColumn = getTableNameOfColumn();
        if (tableNameOfColumn != null && !tables.contains(tableNameOfColumn) && !primaryTableName.isEmpty()
                && !primaryTableName.equals(tableNameOfColumn))
        {
            throw new InvalidEntityDefinitionException("Inavalid table " + tableNameOfColumn + " for field " + field);
        }
    }

    private void processFieldAnnotations(Field field)
    {
        if (field != null)
        {
            Annotation[] annotations = field.getAnnotations();
            for (Annotation annotation : annotations)
            {
                fieldAnnotations.put(annotation.annotationType().getName(), annotation);
            }
        }
    }

    public String getTableNameOfColumn()
    {
        Column column = (Column) getAnnotation(Column.class.getName());

        String tableName = null;
        if (column != null)
        {
            tableName = column.table();
        }

        if (tableName == null || tableName.isEmpty())
        {
            return null;
        }
        return tableName;
    }
}
