package com.impetus.kundera.datatypes.datagenerator;

import java.util.UUID;

public class UUIDDataGenerator implements DataGenerator<UUID>
{

    private static final UUID RANDOM_UUID = UUID.randomUUID();

    private static final UUID MAX_UUID = UUID.randomUUID();

    private static final UUID MIN_UUID = UUID.randomUUID();

    @Override
    public UUID randomValue()
    {

        return RANDOM_UUID;
    }

    @Override
    public UUID maxValue()
    {

        return MAX_UUID;
    }

    @Override
    public UUID minValue()
    {

        return MIN_UUID;
    }

    @Override
    public UUID partialValue()
    {

        return null;
    }

}
