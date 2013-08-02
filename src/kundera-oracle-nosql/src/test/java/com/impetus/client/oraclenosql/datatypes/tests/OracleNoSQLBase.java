package com.impetus.client.oraclenosql.datatypes.tests;

import com.impetus.kundera.datatypes.datagenerator.DataGenerator;
import com.impetus.kundera.datatypes.datagenerator.DataGeneratorFactory;

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
