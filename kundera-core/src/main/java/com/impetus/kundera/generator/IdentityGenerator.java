package com.impetus.kundera.generator;

import com.impetus.kundera.metadata.model.SequenceGeneratorDiscriptor;

public interface IdentityGenerator extends IdGenerator
{
    public Object generate(SequenceGeneratorDiscriptor discriptor);
}
