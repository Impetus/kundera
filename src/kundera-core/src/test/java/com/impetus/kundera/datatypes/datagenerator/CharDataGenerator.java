package com.impetus.kundera.datatypes.datagenerator;

public class CharDataGenerator implements DataGenerator<Character>
{

    private static final char CHAR = 'a';

    @Override
    public Character randomValue()
    {

        return CHAR;
    }

    @Override
    public Character maxValue()
    {

        return Character.MAX_VALUE;
    }

    @Override
    public Character minValue()
    {

        return Character.MIN_VALUE;
    }

    @Override
    public Character partialValue()
    {

        return null;
    }

}
