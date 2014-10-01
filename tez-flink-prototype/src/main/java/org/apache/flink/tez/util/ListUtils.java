package org.apache.flink.tez.util;


import java.util.List;

public class ListUtils {

    public static void ensureSize (List<?> list, int size) {
        for (int i = 0; i < size; i++)
            list.add(null);
    }

}
