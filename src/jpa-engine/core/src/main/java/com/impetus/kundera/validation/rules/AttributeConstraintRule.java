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
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;

import javax.validation.ValidationException;
import javax.validation.constraints.AssertFalse;
import javax.validation.constraints.AssertTrue;
import javax.validation.constraints.DecimalMax;
import javax.validation.constraints.DecimalMin;
import javax.validation.constraints.Digits;
import javax.validation.constraints.Future;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Null;
import javax.validation.constraints.Past;
import javax.validation.constraints.Pattern;
import javax.validation.constraints.Size;

import org.apache.commons.lang.math.NumberUtils;

import com.impetus.kundera.property.PropertyAccessorHelper;

/**
 * @author Chhavi Gangwal
 * 
 */
public class AttributeConstraintRule extends AbstractFieldRule implements FieldRule
{

    /** The rules for entity type validation map. */
    static enum AttributeConstraintType
    {

        ASSERT_FALSE(AssertFalse.class.getSimpleName()), ASSERT_TRUE(AssertTrue.class.getSimpleName()), DECIMAL_MAX(
                DecimalMax.class.getSimpleName()), DECIMAL_MIN(DecimalMin.class.getSimpleName()), DIGITS(Digits.class
                .getSimpleName()), FUTURE(Future.class.getSimpleName()), MAX(Max.class.getSimpleName()), MIN(Min.class
                .getSimpleName()), NOT_NULL(NotNull.class.getSimpleName()), NULL(Null.class.getSimpleName()), PAST(
                Past.class.getSimpleName()), PATTERN(Pattern.class.getSimpleName()), SIZE(Size.class.getSimpleName());

        private String clazz;

        private static final Map<String, AttributeConstraintType> lookup = new HashMap<String, AttributeConstraintType>();

        static
        {
            for (AttributeConstraintType s : EnumSet.allOf(AttributeConstraintType.class))
            {
                lookup.put(s.getClazz(), s);
            }
        }

        /**
         * @param clazz
         */
        private AttributeConstraintType(String clazz)
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
        public static AttributeConstraintType get(String clazz)
        {
            return lookup.get(clazz);
        }
    }

    private AttributeConstraintType getERuleType(String annotationType)
    {

        if (AttributeConstraintType.get(annotationType) != null)
        {
            return AttributeConstraintType.get(annotationType);
        }
        else
        {
            return null;
        }

    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.validation.rules.AbstractFieldRule#validate(java.
     * lang.reflect.Field, java.lang.Object)
     */
    @Override
    public boolean validate(Field f, Object validationObject)
    {
        boolean checkvalidation = true;

        for (Annotation annotation : f.getDeclaredAnnotations())
        {

            AttributeConstraintType eruleType = getERuleType(annotation.annotationType().getSimpleName());

            if (eruleType != null)
            {
                Object fieldValue = PropertyAccessorHelper.getObject(validationObject, f);

                switch (eruleType)
                {

                case ASSERT_FALSE:
                    checkvalidation = validateFalse(fieldValue, annotation);
                    break;
                case ASSERT_TRUE:
                    checkvalidation = validateTrue(fieldValue, annotation);
                    break;
                case DECIMAL_MAX:
                    checkvalidation = validateMaxDecimal(fieldValue, annotation);
                    break;
                case DECIMAL_MIN:
                    checkvalidation = validateMinDecimal(fieldValue, annotation);
                    break;
                case DIGITS:
                    checkvalidation = validateDigits(fieldValue, annotation);
                    break;
                case FUTURE:
                    checkvalidation = validateFuture(fieldValue, annotation);
                    break;
                case MAX:
                    checkvalidation = validateMaxValue(fieldValue, annotation);
                    break;

                case MIN:
                    checkvalidation = validateMinValue(fieldValue, annotation);
                    break;
                case NOT_NULL:
                    checkvalidation = validateNotNull(fieldValue, annotation);
                    break;
                case NULL:
                    checkvalidation = validateNull(fieldValue, annotation);
                    break;
                case PAST:
                    checkvalidation = validatePast(fieldValue, annotation);
                    break;
                case PATTERN:
                    checkvalidation = validatePattern(fieldValue, annotation);
                    break;
                case SIZE:
                    checkvalidation = validateSize(fieldValue, annotation);
                    break;

                }
            }
        }

        return checkvalidation;
    }

    /**
     * Checks whether the given attribute's value is within specified limit 
     * 
     * @param validationObject
     * @param annotate
     * @return
     */
    private boolean validateSize(Object validationObject, Annotation annotate)
    {

        if (checkNullObject(validationObject))
        {
            return true;
        }
        
        int objectSize = 0;
        int minSize = ((Size) annotate).min();
        int maxSize = ((Size) annotate).max();
        if (validationObject != null)
        {
            if (String.class.isAssignableFrom(validationObject.getClass()))
            {
                objectSize = ((String) validationObject).length();

            }
            else if (Collection.class.isAssignableFrom(validationObject.getClass()))
            {
                
                objectSize = ((Collection) validationObject).size();
            }
            else if (Map.class.isAssignableFrom(validationObject.getClass()))
            {
                objectSize = ((Map) validationObject).size();
            }
            else if (ArrayList.class.isAssignableFrom(validationObject.getClass()))
            {

                objectSize = ((ArrayList) validationObject).size();

            }
            else
            {
                throwValidationException(((Size) annotate).message());

            }
        }
        return objectSize <= maxSize && objectSize >= minSize;

    }

    /**
     * Checks whether the given string is a valid pattern or not
     * 
     * @param validationObject
     * @param annotate
     * @return
     */
    private boolean validatePattern(Object validationObject, Annotation annotate)
    {
        if (checkNullObject(validationObject))
        {
            return true;
        }

        java.util.regex.Pattern pattern = java.util.regex.Pattern.compile(((Pattern) annotate).regexp(),
                ((Pattern) annotate).flags().length);
        Matcher matcherPattern = pattern.matcher((String) validationObject);
        if (!matcherPattern.matches())
        {
            throwValidationException(((Pattern) annotate).message());
           
        }

        return true;
    }

    /**
     * Checks whether the object is null or not
     * 
     * @param validationObject
     * @param annotate
     * @return
     */
    private boolean validatePast(Object validationObject, Annotation annotate)
    {
        if (checkNullObject(validationObject))
        {
            return true;
        }

        int res = 0;

        if (validationObject.getClass().isAssignableFrom(java.util.Date.class))
        {
            Date today = new Date();
            Date pastDate = (Date) validationObject;

            res = pastDate.compareTo(today);
        }
        else if (validationObject.getClass().isAssignableFrom(java.util.Calendar.class))
        {

            Calendar cal = Calendar.getInstance();
            Calendar pastDate = (Calendar) validationObject;
            res = pastDate.compareTo(cal);

        }
        // else
        // {
        // ruleExceptionHandler(((Past) annotate).message());
        // }
        if (res >= 0)
        {
            throwValidationException(((Past) annotate).message());
        }

        return true;
    }

    /**
     * Checks whether a given date is that in future or not
     * 
     * @param validationObject
     * @param annotate
     * @return
     */
    private boolean validateFuture(Object validationObject, Annotation annotate)
    {
        if (checkNullObject(validationObject))
        {
            return true;
        }

        int res = 0;

        if (validationObject.getClass().isAssignableFrom(java.util.Date.class))
        {
            Date today = new Date();
            Date futureDate = (Date) validationObject;

            res = futureDate.compareTo(today);
        }
        else if (validationObject.getClass().isAssignableFrom(java.util.Calendar.class))
        {

            Calendar cal = Calendar.getInstance();
            Calendar futureDate = (Calendar) validationObject;
            res = futureDate.compareTo(cal);

        }
        // else
        // {
        // //ruleExceptionHandler(((Future) annotate).message());
        // throw new RuleValidationException(((Future)
        // annotate).message());
        // }
        if (res <= 0)
        {
            throwValidationException(((Future) annotate).message());
        }

        return true;
    }

    /**
     * Checks whether a given date is that in past or not
     * 
     * @param validationObject
     * @param annotate
     * @return
     */
    private boolean validateNull(Object validationObject, Annotation annotate)
    {
        if (checkNullObject(validationObject))
        {
            return true;
        }

        if (!validationObject.equals(null) || validationObject != null)
        {
            throwValidationException(((Null) annotate).message());
        }

        return true;
    }

    /**
     * 
     * Checks whether a given date is not null
     * 
     * @param validationObject
     * @param annotate
     * @return
     */
    private boolean validateNotNull(Object validationObject, Annotation annotate)
    {

        if (validationObject == null || validationObject.equals(null))
        {
            throwValidationException(((NotNull) annotate).message());
        }
        return true;
    }

    /**
     * Checks whether a given value is greater than given min value or not
     * 
     * @param validationObject
     * @param annotate
     * @return
     */
    private boolean validateMinValue(Object validationObject, Annotation annotate)
    {
        if (checkNullObject(validationObject))
        {
            return true;
        }

        Long minValue = ((Min) annotate).value();
        if (checkvalidDigitTypes(validationObject.getClass()))
        {
            if ((NumberUtils.toLong(toString(validationObject))) < minValue)
            {

                throwValidationException(((Min) annotate).message());
            }
        }

        return true;
    }

    /**
     * Checks whether a given value is lesser than given max value or not
     * 
     * @param validationObject
     * @param annotate
     * @return
     */
    private boolean validateMaxValue(Object validationObject, Annotation annotate)
    {
        if (checkNullObject(validationObject))
        {
            return true;
        }

        Long maxValue = ((Max) annotate).value();
        if (checkvalidDigitTypes(validationObject.getClass()))
        {
            if ((NumberUtils.toLong(toString(validationObject))) > maxValue)
            {

                throwValidationException(((Max) annotate).message());

            }
        }

        return true;
    }

    /**
     * Checks whether a given value is is a number or not
     * 
     * @param validationObject
     * @param annotate
     * @return
     */
    private boolean validateDigits(Object validationObject, Annotation annotate)
    {
        if (checkNullObject(validationObject))
        {
            return true;
        }

        if (checkvalidDigitTypes(validationObject.getClass()))
        {
            if (!NumberUtils.isDigits(toString(validationObject)))
            {

                throwValidationException(((Digits) annotate).message());
            }
        }

        return true;
    }

    /**
     * Checks whether a given value is a valid minimum decimal digit when compared to given value 
     * or not
     * 
     * @param validationObject
     * @param annotate
     * @return
     */
    private boolean validateMinDecimal(Object validationObject, Annotation annotate)
    {

        if (validationObject != null)
        {
            try
            {
                if (checkvalidDeciDigitTypes(validationObject.getClass()))
                {
                    BigDecimal minValue = NumberUtils.createBigDecimal(((DecimalMin) annotate).value());
                    BigDecimal actualValue = NumberUtils.createBigDecimal(toString(validationObject));
                    int res = actualValue.compareTo(minValue);
                    if (res < 0)
                    {
                        throwValidationException(((DecimalMin) annotate).message());
                    }

                }
            }
            catch (NumberFormatException nfe)
            {
                throw new RuleValidationException(nfe.getMessage());
            }

        }

        return true;
    }

    /**
     * Checks whether a given value is a valid maximum decimal digit when compared to given value 
     * or not
     * 
     * @param validationObject
     * @param annotate
     * @return
     */
    private boolean validateMaxDecimal(Object validationObject, Annotation annotate)
    {
        if (validationObject != null)
        {
            try
            {
                if (checkvalidDeciDigitTypes(validationObject.getClass()))
                {
                    BigDecimal maxValue = NumberUtils.createBigDecimal(((DecimalMax) annotate).value());
                    BigDecimal actualValue = NumberUtils.createBigDecimal(toString(validationObject));
                    int res = actualValue.compareTo(maxValue);
                    if (res >  0)
                    {
                        throwValidationException(((DecimalMax) annotate).message());
                    }

                }
            }
            catch (NumberFormatException nfe)
            {
                throw new RuleValidationException(nfe.getMessage());
            }

        }
        return true;
    }

    /**
     * @param validationObject
     * @param annotate
     * @return
     */
    private boolean validateTrue(Object validationObject, Annotation annotate)
    {
        if (checkNullObject(validationObject))
        {
            return true;
        }

        if (checkvalidBooleanTypes(validationObject.getClass()) && !(Boolean) validationObject)
        {
            throwValidationException(((AssertTrue) annotate).message());
        }

        return true;
    }

    /**
     * @param validationObject
     * @param annotate
     * @return
     */
    private boolean validateFalse(Object validationObject, Annotation annotate)
    {
        if (checkNullObject(validationObject))
        {
            return true;
        }

        if (checkvalidBooleanTypes(validationObject.getClass()) && (Boolean) validationObject)
        {
            throwValidationException(((AssertFalse) annotate).message());

        }

        return true;
    }

    /**
     * @param javaType
     * @return
     */
    private boolean checkvalidDigitTypes(Class<?> javaType)
    {
        return javaType.isAssignableFrom(BigDecimal.class) || javaType.isAssignableFrom(byte.class)
                || javaType.isAssignableFrom(Byte.class) || javaType.isAssignableFrom(short.class)
                || javaType.isAssignableFrom(Short.class) || javaType.isAssignableFrom(int.class)
                || javaType.isAssignableFrom(Integer.class) || javaType.isAssignableFrom(long.class)
                || javaType.isAssignableFrom(Long.class);

    }

    /**
     * @param javaType
     * @return
     */
    private boolean checkvalidBooleanTypes(Class<?> javaType)
    {
        return javaType.isAssignableFrom(Boolean.class) || javaType.isAssignableFrom(boolean.class);

    }

    /**
     * @param javaType
     * @return
     */
    private boolean checkvalidDeciDigitTypes(Class<?> javaType)
    {
        return javaType.isAssignableFrom(BigDecimal.class) || javaType.isAssignableFrom(String.class)
                || javaType.isAssignableFrom(byte.class) || javaType.isAssignableFrom(Byte.class)
                || javaType.isAssignableFrom(short.class) || javaType.isAssignableFrom(Short.class)
                || javaType.isAssignableFrom(int.class) || javaType.isAssignableFrom(Integer.class)
                || javaType.isAssignableFrom(long.class) || javaType.isAssignableFrom(Long.class);

    }

    /**
     * @param validationObject
     * @return
     */
    private String toString(Object validationObject)
    {
        String stringObject = null;
        if (validationObject.getClass().isAssignableFrom(int.class)
                || validationObject.getClass().isAssignableFrom(Integer.class))
        {
            stringObject = Integer.toString((Integer) validationObject);

        }
        else if (validationObject.getClass().isAssignableFrom(byte.class)
                || validationObject.getClass().isAssignableFrom(Byte.class))
        {
            stringObject = Byte.toString((Byte) validationObject);
        }
        else if (validationObject.getClass().isAssignableFrom(short.class)
                || validationObject.getClass().isAssignableFrom(Short.class))
        {
            stringObject = Short.toString((Short) validationObject);
        }

        else if (validationObject.getClass().isAssignableFrom(BigDecimal.class))
        {
            stringObject = validationObject.toString();
        }
        else if (validationObject.getClass().isAssignableFrom(Long.class)
                || validationObject.getClass().isAssignableFrom(long.class))
        {
            stringObject = Long.toString((Long) validationObject);
        }

        else if (validationObject.getClass().isAssignableFrom(String.class)
                || validationObject.getClass().isAssignableFrom(String.class))
        {
            stringObject = (String) validationObject;
        }

        return stringObject;
    }

    /**
     * @param validationObject
     * @return
     */
    private boolean checkNullObject(Object validationObject)
    {

        return validationObject == null;
    }

    /**
     * @param message
     */
    private void throwValidationException(String message)
    {
        if (!message.isEmpty() || message != null)
        {
            throw new ValidationException(message);
        }
        else
        {
            throw new ValidationException("Constraint validation exception");
        }

    }

}
