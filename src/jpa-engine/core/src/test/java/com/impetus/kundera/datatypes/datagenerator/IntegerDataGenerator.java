package com.impetus.kundera.datatypes.datagenerator;

public class IntegerDataGenerator implements DataGenerator<Integer>
{

    private static final Integer INTEGER = new Integer("1234");

    @Override
    public Integer randomValue()
    {
        return INTEGER;
    }

    @Override
    public Integer maxValue()
    {
        return Integer.MAX_VALUE;
    }

    @Override
    public Integer minValue()
    {
        return Integer.MIN_VALUE;
    }

    @Override
    public Integer partialValue()
    {
        return null;
    }
}
