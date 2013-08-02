package com.impetus.kundera.datatypes.datagenerator;

import java.sql.Date;

public class SqlDateDataGenerator implements DataGenerator<Date>
{

    private static final Long LONG = new Long(123456789);

    @Override
    public Date randomValue()
    {

        return new Date(LONG);
    }

    @Override
    public Date maxValue()
    {

        return new Date(Long.MAX_VALUE);
    }

    @Override
    public Date minValue()
    {

        return new Date(0L);
    }

    @Override
    public Date partialValue()
    {

        return null;
    }

}
