package com.impetus.kundera.datatypes.datagenerator;

public class FloatDataGenerator implements DataGenerator<Float>
{

    private static final Float FLOAT = new Float("121.11");

    @Override
    public Float randomValue()
    {

        return FLOAT;
    }

    @Override
    public Float maxValue()
    {

        return Float.MAX_VALUE;
    }

    @Override
    public Float minValue()
    {

        return Float.MIN_VALUE;
    }

    @Override
    public Float partialValue()
    {

        return Float.NaN;
    }

}
