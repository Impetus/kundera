package com.impetus.kundera.metadata.model.annotation;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.SecondaryTable;
import javax.persistence.SecondaryTables;

public class DefaultEntityAnnotationProcessor implements EntityAnnotationProcessor
{
    private Map<String, Annotation> entityAnnotations;

    public DefaultEntityAnnotationProcessor(Class clazz)
    {
        entityAnnotations = new HashMap<String, Annotation>();
        processEntityAnnotations(clazz);
    }

    @Override
    public Map<String, Annotation> getAnnotations()
    {
        return entityAnnotations;
    }

    @Override
    public Annotation getAnnotation(String annotationName)
    {
        return entityAnnotations.get(annotationName);
    }

    @Override
    public void validateClassAnnotation(Annotation annotation, Class clazz)
    {

    }

    private void processEntityAnnotations(Class clazz)
    {
        if (clazz != null)
        {
            Annotation[] annotations = clazz.getAnnotations();
            for (Annotation annotation : annotations)
            {
                entityAnnotations.put(annotation.annotationType().getName(), annotation);
            }
        }
    }

    public List<String> getSecondaryTablesName()
    {
        List<String> tables = new ArrayList<String>();

        SecondaryTables secondaryTablesAnnotation = (SecondaryTables) getAnnotation(SecondaryTables.class.getName());

        SecondaryTable secondaryTableAnnotation = (SecondaryTable) getAnnotation(SecondaryTable.class.getName());

        if (secondaryTablesAnnotation != null)
        {
            SecondaryTable[] secondaryTables = secondaryTablesAnnotation.value();

            for (SecondaryTable secondaryTable : secondaryTables)
            {
                tables.add(secondaryTable.name());
            }
        }
        else if (secondaryTableAnnotation != null)
        {
            tables.add(secondaryTableAnnotation.name());
        }
        return tables;
    }
}
