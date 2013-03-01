package com.impetus.kundera.metadata.processor;

import java.lang.reflect.Field;

import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.SequenceGenerator;
import javax.persistence.TableGenerator;
import javax.transaction.NotSupportedException;

import com.impetus.kundera.metadata.KunderaMetadataManager;
import com.impetus.kundera.metadata.model.EntityMetadata;
import com.impetus.kundera.metadata.model.KeyValue;
import com.impetus.kundera.metadata.model.MetamodelImpl;
import com.impetus.kundera.metadata.model.SequenceGeneratorDiscriptor;
import com.impetus.kundera.metadata.model.TableGeneratorDiscriptor;

public class GeneratedValueProcesser
{
    public void process(Class<? extends Object> clazz, Field idField, EntityMetadata m) throws NotSupportedException
    {
        MetamodelImpl metamodel = KunderaMetadataManager.getMetamodel(m.getPersistenceUnit());
        if (metamodel == null)
        {
            return;
        }
        KeyValue keyValue = new KeyValue();

        GeneratedValue value = idField.getAnnotation(GeneratedValue.class);
        String generatorName = value.generator();
        GenerationType generationType = value.strategy();

        switch (generationType)
        {
        case TABLE:
            TableGeneratorDiscriptor tgd = null;
            if (!generatorName.isEmpty())
            {
                TableGenerator tableGenerator = idField.getAnnotation(TableGenerator.class);
                if (tableGenerator == null || !tableGenerator.name().equals(generatorName))
                {
                    tableGenerator = clazz.getAnnotation(TableGenerator.class);
                }
                tgd = new TableGeneratorDiscriptor(tableGenerator, m.getSchema(), m.getTableName());
            }
            else
            {
                tgd = new TableGeneratorDiscriptor(m.getSchema(), m.getTableName());
            }
            keyValue.setTableDiscriptor(tgd);
            keyValue.setStrategy(GenerationType.TABLE);
            break;
        case SEQUENCE:
            SequenceGeneratorDiscriptor sgd = null;
            if (!generatorName.isEmpty())
            {
                SequenceGenerator sequenceGenerator = idField.getAnnotation(SequenceGenerator.class);
                if (sequenceGenerator == null || !sequenceGenerator.name().equals(generatorName))
                {
                    sequenceGenerator = clazz.getAnnotation(SequenceGenerator.class);
                }
                sgd = new SequenceGeneratorDiscriptor(sequenceGenerator, m.getSchema());
            }
            else
            {
                sgd = new SequenceGeneratorDiscriptor(m.getSchema());
            }
            keyValue.setSequenceDiscriptor(sgd);
            keyValue.setStrategy(GenerationType.SEQUENCE);
            break;
        case IDENTITY:
            keyValue.setStrategy(GenerationType.IDENTITY);
            break;
        case AUTO:
            // No need of Any Generator
            keyValue.setStrategy(GenerationType.AUTO);
            break;
        }
        metamodel.addKeyValue(clazz.getName(), keyValue);
    }
}
