package com.impetus.client.hbase.query;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;

public class BinaryComparatorFactory implements Function<byte[], ByteArrayComparable> {
    public static final BinaryComparatorFactory INSTANCE = new BinaryComparatorFactory();

    public ByteArrayComparable apply(byte[] bytes) {
        return new BinaryComparator(bytes);
    }
}
