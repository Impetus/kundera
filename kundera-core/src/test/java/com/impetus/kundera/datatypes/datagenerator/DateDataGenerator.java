package com.impetus.kundera.datatypes.datagenerator;

import java.util.Date;

public class DateDataGenerator implements DataGenerator<Date>
{
    private static final LongDataGenerator LONG_DATA_GENERATOR = new LongDataGenerator();

    private static final Date MIN_DATE = new Date(0L);

    private static final Date MAX_DATE = new Date(Integer.MAX_VALUE);

    private static final Date DATE = new Date(LONG_DATA_GENERATOR.randomValue());

    @Override
    public Date randomValue()
    {
        // System.out.println("In date generator random" + DATE.getTime());
        return DATE;
    }

    @Override
    public Date maxValue()
    {
        // System.out.println("In date generator max" + MAX_DATE.getTime());
        return MAX_DATE;
    }

    @Override
    public Date minValue()
    {
        // System.out.println("In date generator min" + MIN_DATE.getTime());
        return MIN_DATE;
    }

    @Override
    public Date partialValue()
    {

        return null;
    }

}
