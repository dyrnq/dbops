package com.dyrnq.dbops.utils;

import com.github.f4b6a3.tsid.TsidCreator;

public class IDUtils {

    public static Long getLongID() {
        return TsidCreator.getTsid().toLong();
    }

    public static String getLongIDAsString() {
        return String.valueOf(TsidCreator.getTsid().toLong());
    }
}
