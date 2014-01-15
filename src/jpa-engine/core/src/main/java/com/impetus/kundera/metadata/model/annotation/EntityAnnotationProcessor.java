package com.impetus.kundera.metadata.model.annotation;

import java.lang.annotation.Annotation;

public interface EntityAnnotationProcessor extends JPAAnnotationProcessor
{
    void validateClassAnnotation(Annotation annotation, Class clazz);
}
