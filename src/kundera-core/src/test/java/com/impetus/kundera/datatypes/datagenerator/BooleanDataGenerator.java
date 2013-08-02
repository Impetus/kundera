package com.impetus.kundera.datatypes.datagenerator;

public class BooleanDataGenerator implements DataGenerator<Boolean>
{

    @Override
    public Boolean randomValue()
    {

        return true;
    }

    @Override
    public Boolean maxValue()
    {

        return true;
    }

    @Override
    public Boolean minValue()
    {

        return false;
    }

    @Override
    public Boolean partialValue()
    {

        return null;
    }

}
