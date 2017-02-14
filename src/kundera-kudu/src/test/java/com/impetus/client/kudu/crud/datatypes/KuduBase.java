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
 * KuduBase class for different data type test.
 * 
 * @author Devender Yadav
 */
public abstract class KuduBase
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
        return dataGenerator.maxValue();
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
