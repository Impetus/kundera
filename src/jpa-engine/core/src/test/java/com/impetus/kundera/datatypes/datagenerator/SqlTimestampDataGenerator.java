package com.impetus.kundera.datatypes.datagenerator;

import java.sql.Timestamp;

public class SqlTimestampDataGenerator implements DataGenerator<Timestamp>
{

    private static final LongDataGenerator LONG_DATA_GENERATOR = new LongDataGenerator();

    @Override
    public Timestamp randomValue()
    {

        return new Timestamp(LONG_DATA_GENERATOR.randomValue());
    }

    @Override
    public Timestamp maxValue()
    {

        return new Timestamp(Integer.MAX_VALUE);
    }

    @Override
    public Timestamp minValue()
    {

        return new Timestamp(0L);
    }

    @Override
    public Timestamp partialValue()
    {

        return null;
    }

}
