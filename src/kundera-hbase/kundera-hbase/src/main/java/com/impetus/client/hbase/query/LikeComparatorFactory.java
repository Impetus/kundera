package com.impetus.client.hbase.query;

import static java.lang.Character.isLetterOrDigit;

import com.google.common.base.Function;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.RegexStringComparator;

public class LikeComparatorFactory implements Function<byte[], ByteArrayComparable> {
    public static final LikeComparatorFactory INSTANCE = new LikeComparatorFactory();

    static String likeToRegex(String like) {
        StringBuilder builder = new StringBuilder();
        boolean wasPercent = false;
        for (int i = 0; i < like.length(); ++i) {
            char c = like.charAt(i);
            if (isPlain(c)) {
                if (wasPercent) {
                    wasPercent = false;
                    builder.append(".*");
                }
                builder.append(c);
            } else if (wasPercent) {
                wasPercent = false;
                if (c == '%') {
                    builder.append('%');
                } else {
                    builder.append(".*").append(c);
                }
            } else {
                if (c == '%') {
                    wasPercent = true;
                } else {
                    builder.append('\\').append(c);
                }
            }
        }
        if (wasPercent) {
            builder.append(".*");
        }
        return builder.toString();
    }

    private static boolean isPlain(char c) {
        return isLetterOrDigit(c) || "_@ ~`\"'#<>,;;=!&".indexOf(c) >= 0;
    }

    public ByteArrayComparable apply(byte[] bytes) {
        return new RegexStringComparator(likeToRegex(new String(bytes)));
    }
}
