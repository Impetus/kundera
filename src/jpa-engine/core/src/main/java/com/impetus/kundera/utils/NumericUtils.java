/**
 * Copyright 2013 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.impetus.kundera.utils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.math.NumberUtils;


/**
 * Utility class to define various numeric functions 
 * 
 * @author vivek.mishra
 *
 */


public final class NumericUtils
{

    enum NumberType
    {
        INTEGER,FLOAT,LONG,DOUBLE,BIGDECIMAL,SHORT,BIGINTEGER
    }
    
    private static Map<Class, NumberType> numberTypes = new HashMap<Class, NumericUtils.NumberType>();
    static
    {
        numberTypes.put(int.class, NumberType.INTEGER);
        numberTypes.put(Integer.class, NumberType.INTEGER);
        numberTypes.put(float.class, NumberType.FLOAT);
        numberTypes.put(Float.class, NumberType.FLOAT);
        numberTypes.put(long.class, NumberType.LONG);
        numberTypes.put(Long.class, NumberType.LONG);
        numberTypes.put(double.class, NumberType.DOUBLE);
        numberTypes.put(Double.class, NumberType.DOUBLE);
        numberTypes.put(BigDecimal.class, NumberType.BIGDECIMAL);
        numberTypes.put(short.class, NumberType.SHORT);
        numberTypes.put(Short.class, NumberType.SHORT);
        numberTypes.put(BigInteger.class, NumberType.BIGINTEGER);
    }
    
    /**
     *  Check if zero
     * @param value        value string
     * @param valueClazz   value class 
     * @return             
     */
    public static final boolean checkIfZero(String value, Class valueClazz)
    {
        boolean returnValue=false;
        if(value != null && NumberUtils.isNumber(value) && numberTypes.get(valueClazz) != null)
        {
            switch (numberTypes.get(valueClazz))
            {
            
            case INTEGER:
                returnValue = Integer.parseInt(value) == (NumberUtils.INTEGER_ZERO);
                break;

            case FLOAT:
                returnValue = Float.parseFloat(value) == (NumberUtils.FLOAT_ZERO);
                break;

            case LONG:
                returnValue = Long.parseLong(value) == (NumberUtils.LONG_ZERO);
                break;

            case BIGDECIMAL:
                // Note: cannot use 'equals' here - it would require both BigDecimals to have
                // the same scale.
                returnValue = (new BigDecimal(value)).compareTo(BigDecimal.ZERO) == 0;
                break;

            case BIGINTEGER:
                returnValue = (new BigInteger(value)).equals(BigInteger.ZERO);
                break;
            
            case SHORT:
                returnValue = (new Short(value)).shortValue() == NumberUtils.SHORT_ZERO.shortValue();
                break;
            }
        }
        
        return returnValue;
    }
}
