package com.impetus.kundera.generator;

import javax.persistence.GenerationType;

import com.impetus.kundera.metadata.model.SequenceGeneratorDiscriptor;

public interface SequenceGenerator extends IdGenerator
{
    public Object generate(SequenceGeneratorDiscriptor discriptor);
}
