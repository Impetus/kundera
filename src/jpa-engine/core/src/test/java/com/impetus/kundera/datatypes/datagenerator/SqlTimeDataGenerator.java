package com.impetus.kundera.datatypes.datagenerator;

import java.sql.Time;

public class SqlTimeDataGenerator implements DataGenerator<Time>
{

    @Override
    public Time partialValue()
    {

        return null;
    }
    
    private static final Long LONG_DATA_GENERATOR = new LongDataGenerator().randomValue();

    @Override
    public Time randomValue()
    {

        return new Time(LONG_DATA_GENERATOR);
    }

    @Override
    public Time maxValue()
    {

        return new Time(Integer.MAX_VALUE);
    }

    @Override
    public Time minValue()
    {

        return new Time(0L);
    }

}
