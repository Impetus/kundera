package com.impetus.kundera.generator;

import com.impetus.kundera.metadata.model.TableGeneratorDiscriptor;

public interface TableGenerator extends IdGenerator
{
    public Object generate(TableGeneratorDiscriptor discriptor);
}
