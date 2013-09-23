package com.impetus.kundera.datatypes.datagenerator;

public class ByteDataGenerator implements DataGenerator<Byte>
{

    private static final Byte BYTE = new Byte("12");

    @Override
    public Byte randomValue()
    {

        return BYTE;
    }

    @Override
    public Byte maxValue()
    {

        return Byte.MAX_VALUE;
    }

    @Override
    public Byte minValue()
    {

        return Byte.MIN_VALUE;
    }

    @Override
    public Byte partialValue()
    {

        return null;
    }
}
