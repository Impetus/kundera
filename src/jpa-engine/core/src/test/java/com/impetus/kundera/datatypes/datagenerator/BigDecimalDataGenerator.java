package com.impetus.kundera.datatypes.datagenerator;

import java.math.BigDecimal;
import java.math.MathContext;

public class BigDecimalDataGenerator implements DataGenerator<BigDecimal>
{
    @Override
    public BigDecimal randomValue()
    {
        return new BigDecimal(1234);
    }

    @Override
    public BigDecimal maxValue()
    {
        return new BigDecimal(23.45, new MathContext(4));
    }

    @Override
    public BigDecimal minValue()
    {
        return new BigDecimal(0.01, new MathContext(1));
    }

    @Override
    public BigDecimal partialValue()
    {
        return BigDecimal.ZERO;
    }

}
