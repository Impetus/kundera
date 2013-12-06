package com.impetus.kundera.metadata.model.annotation;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;

public interface FieldAnnotationProcessor extends JPAAnnotationProcessor
{
    void validateFieldAnnotation(Annotation annotation, Field field); 
}
