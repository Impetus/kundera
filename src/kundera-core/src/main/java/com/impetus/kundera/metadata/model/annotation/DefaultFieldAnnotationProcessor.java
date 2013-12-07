package com.impetus.kundera.metadata.model.annotation;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import javax.persistence.Column;

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
    public void validateFieldAnnotation(Annotation annotation, Field field)
    {

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
