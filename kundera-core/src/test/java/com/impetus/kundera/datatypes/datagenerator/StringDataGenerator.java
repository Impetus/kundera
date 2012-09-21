/*
 * 
 */
package com.impetus.kundera.datatypes.datagenerator;

public class StringDataGenerator implements DataGenerator<String>
{

    // private static final String AMRESH = new String("Amresh");
    // private static final String random = new String("Kuldeep");

    @Override
    public String randomValue()
    {
        return new String("Amresh");
    }

    @Override
    public String maxValue()
    {
        return new String("Vivek");
    }

    @Override
    public String minValue()
    {
        return new String("Kuldeep");
    }

    @Override
    public String partialValue()
    {
        return null;
    }
}
