package com.impetus.kundera.metadata.model.annotation;

import java.lang.annotation.Annotation;
import java.util.Map;

public interface JPAAnnotationProcessor
{
    public Map<String, Annotation> getAnnotations();

    public Annotation getAnnotation(String annotationName);
}
