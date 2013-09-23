package com.impetus.kundera.datatypes.datagenerator;

import java.math.BigDecimal;

public class BigDecimalDataGenerator implements DataGenerator<BigDecimal>
{
    private static final LongDataGenerator LONG_DATA_GENERATOR = new LongDataGenerator();

    @Override
    public BigDecimal randomValue()
    {
        return BigDecimal.valueOf(LONG_DATA_GENERATOR.randomValue());
    }

    @Override
    public BigDecimal maxValue()
    {
        return new BigDecimal(23.45);
    }

    @Override
    public BigDecimal minValue()
    {
        return new BigDecimal(0.001);
    }

    @Override
    public BigDecimal partialValue()
    {
        return BigDecimal.ZERO;
    }

}
