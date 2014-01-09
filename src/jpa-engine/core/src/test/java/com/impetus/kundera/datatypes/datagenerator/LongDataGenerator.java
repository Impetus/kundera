package com.impetus.kundera.datatypes.datagenerator;

public class LongDataGenerator implements DataGenerator<Long>
{

    private static final Long LONG = new Long(12345678);

    @Override
    public Long randomValue()
    {
        return LONG;
    }

    @Override
    public Long maxValue()
    {
        return Long.MAX_VALUE;
    }

    @Override
    public Long minValue()
    {
        return Long.MIN_VALUE;
    }

    @Override
    public Long partialValue()
    {
        return null;
    }

}
