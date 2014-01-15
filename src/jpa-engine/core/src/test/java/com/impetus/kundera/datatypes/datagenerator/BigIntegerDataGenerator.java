package com.impetus.kundera.datatypes.datagenerator;

import java.math.BigInteger;

public class BigIntegerDataGenerator implements DataGenerator<BigInteger>
{

    private static final BigInteger BIG_INTEGER = new BigInteger("1234567");

    @Override
    public BigInteger randomValue()
    {
        return BIG_INTEGER;
    }

    @Override
    public BigInteger maxValue()
    {
        return BigInteger.TEN;
    }

    @Override
    public BigInteger minValue()
    {
        return BigInteger.ZERO;
    }

    @Override
    public BigInteger partialValue()
    {
        return null;
    }

}
