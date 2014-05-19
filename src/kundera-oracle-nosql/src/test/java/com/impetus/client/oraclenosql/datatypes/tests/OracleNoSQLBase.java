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
package com.impetus.client.oraclenosql.datatypes.tests;

import com.impetus.kundera.datatypes.datagenerator.DataGenerator;
import com.impetus.kundera.datatypes.datagenerator.DataGeneratorFactory;

/**
 * @author vivek.mishra
 *
 */
public abstract class OracleNoSQLBase
{
    public static final boolean RUN_IN_EMBEDDED_MODE = true;

    public static final boolean AUTO_MANAGE_SCHEMA = true;

    DataGenerator<?> dataGenerator;

    DataGeneratorFactory factory = new DataGeneratorFactory();

    protected Object getMaxValue(Class<?> clazz)
    {
        dataGenerator = factory.getDataGenerator(clazz);
        return dataGenerator.maxValue();
    }

    protected Object getMinValue(Class<?> clazz)
    {
        dataGenerator = factory.getDataGenerator(clazz);
        return dataGenerator.minValue();
    }

    protected Object getRandomValue(Class<?> clazz)
    {
        dataGenerator = factory.getDataGenerator(clazz);
        return dataGenerator.randomValue();
    }

    protected Object getPartialValue(Class<?> clazz)
    {
        dataGenerator = factory.getDataGenerator(clazz);
        return dataGenerator.partialValue();
    }
}
