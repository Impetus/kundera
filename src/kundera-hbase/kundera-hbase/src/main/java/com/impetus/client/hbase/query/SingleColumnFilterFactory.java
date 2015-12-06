package com.impetus.client.hbase.query;

import static org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;

import com.google.common.base.Function;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;

public class SingleColumnFilterFactory {
    public static final SingleColumnFilterFactory EQUAL = new SingleColumnFilterFactory(
            CompareOp.EQUAL, BinaryComparatorFactory.INSTANCE);
    public static final SingleColumnFilterFactory GREATER = new SingleColumnFilterFactory(
            CompareOp.GREATER, BinaryComparatorFactory.INSTANCE);
    public static final SingleColumnFilterFactory LESS = new SingleColumnFilterFactory(
            CompareOp.LESS, BinaryComparatorFactory.INSTANCE);
    public static final SingleColumnFilterFactory GREATER_OR_EQUAL = new SingleColumnFilterFactory(
            CompareOp.GREATER_OR_EQUAL, BinaryComparatorFactory.INSTANCE);
    public static final SingleColumnFilterFactory LESS_OR_EQUAL = new SingleColumnFilterFactory(
            CompareOp.LESS_OR_EQUAL, BinaryComparatorFactory.INSTANCE);
    public static final SingleColumnFilterFactory LIKE = new SingleColumnFilterFactory(
        CompareOp.EQUAL, LikeComparatorFactory.INSTANCE);

    public final CompareOp operator;
    private final Function<byte[], ByteArrayComparable> comparatorFactory;

    public SingleColumnFilterFactory(CompareOp operator,
            Function<byte[], ByteArrayComparable> comparatorFactory) {
        this.operator = operator;
        this.comparatorFactory = comparatorFactory;
    }

    public Filter create(String family, String column, byte[] value) {
        return new SingleColumnValueFilter(toBytes(family), toBytes(column),
                operator, comparatorFactory.apply(value));
    }
}
