/*******************************************************************************
 * * Copyright 2016 Impetus Infotech.
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
package com.impetus.client.kudu.crud.datatypes;

import com.impetus.kundera.datatypes.datagenerator.DataGenerator;
import com.impetus.kundera.datatypes.datagenerator.DataGeneratorFactory;

/**
 * The Class Base.
 * 
 * @author Devender Yadav
 */
public abstract class Base
{

    /** The data generator. */
    DataGenerator<?> dataGenerator;

    /** The factory. */
    DataGeneratorFactory factory = new DataGeneratorFactory();

    /**
     * Gets the max value.
     * 
     * @param clazz
     *            the clazz
     * @return the max value
     */
    protected Object getMaxValue(Class<?> clazz)
    {
        dataGenerator = factory.getDataGenerator(clazz);
        //#KUDU-1766
        return handleKuduBug(dataGenerator.maxValue());
        // return dataGenerator.maxValue();
    }

    /**
     * Handle kudu bug.
     * 
     * @param maxValue
     *            the max value
     * @return the object
     */
    private Object handleKuduBug(Object maxValue)
    {
        if (maxValue instanceof Byte)
        {
            return (byte) (((Byte) maxValue) - (byte) 1);
        }
        else if (maxValue instanceof Short)
        {
            return (short) (((Short) maxValue) - (short) 1);
        }
        else if (maxValue instanceof Integer)
        {
            return ((Integer) maxValue) - 1;
        }
        else if (maxValue instanceof Long)
        {
            return ((Long) maxValue) - 1l;
        }
        else
        {
            return maxValue;
        }
    }

    /**
     * Gets the min value.
     * 
     * @param clazz
     *            the clazz
     * @return the min value
     */
    protected Object getMinValue(Class<?> clazz)
    {
        dataGenerator = factory.getDataGenerator(clazz);
        return dataGenerator.minValue();
    }

    /**
     * Gets the random value.
     * 
     * @param clazz
     *            the clazz
     * @return the random value
     */
    protected Object getRandomValue(Class<?> clazz)
    {
        dataGenerator = factory.getDataGenerator(clazz);
        return dataGenerator.randomValue();
    }

    /**
     * Gets the partial value.
     * 
     * @param clazz
     *            the clazz
     * @return the partial value
     */
    protected Object getPartialValue(Class<?> clazz)
    {
        dataGenerator = factory.getDataGenerator(clazz);
        return dataGenerator.partialValue();
    }
}
