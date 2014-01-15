package com.impetus.kundera.datatypes.datagenerator;

import java.sql.Date;
import java.sql.Time;

public class SqlTimeDataGenerator implements DataGenerator<Time>
{

    private static final Time RANDOM_TIME = new Time(System.currentTimeMillis());

    @Override
    public Time randomValue()
    {
        return RANDOM_TIME;
    }

    @Override
    public Time maxValue()
    {

        return new Time(new Date(2100, 1, 1).getTime() + 3600000);
    }

    @Override
    public Time minValue()
    {

        return new Time(new Date(1970, 1, 1).getTime());
    }

    @Override
    public Time partialValue()
    {

        return null;
    }

}
