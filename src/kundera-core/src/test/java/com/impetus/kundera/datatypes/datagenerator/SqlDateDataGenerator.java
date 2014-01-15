package com.impetus.kundera.datatypes.datagenerator;

import java.sql.Date;

public class SqlDateDataGenerator implements DataGenerator<Date>
{

    private static final Date RANDOM_DATE = new Date(System.currentTimeMillis());

    @Override
    public Date randomValue()
    {

        return RANDOM_DATE;
    }

    @Override
    public Date maxValue()
    {

        return new Date(2100, 1, 1);
    }

    @Override
    public Date minValue()
    {

        return new Date(1970, 1, 1);
    }

    @Override
    public Date partialValue()
    {

        return null;
    }

}
