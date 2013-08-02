package com.impetus.kundera.datatypes.datagenerator;

public class ShortDataGenerator implements DataGenerator<Short>
{

    private static final Short SHORT = new Short("3");

    @Override
    public Short randomValue()
    {
        return SHORT;
    }

    @Override
    public Short maxValue()
    {
        return Short.MAX_VALUE;
    }

    @Override
    public Short minValue()
    {
        return Short.MIN_VALUE;
    }

    @Override
    public Short partialValue()
    {
        return (short) 0;
    }
}
