package com.impetus.kundera.datatypes.datagenerator;

public class DoubleDataGenerator implements DataGenerator<Double>
{

    private static final Double DOUBLE = new Double("1235.151");

    @Override
    public Double randomValue()
    {

        return DOUBLE;
    }

    @Override
    public Double maxValue()
    {

        return Double.MAX_VALUE;
    }

    @Override
    public Double minValue()
    {

        return Double.MIN_VALUE;
    }

    @Override
    public Double partialValue()
    {

        return Double.NaN;
    }

}
