package cn.hk.orange.utils;

public class Types {
    public static long addressToUid(int pgno, short offset) {
        return (long)pgno << 32 | (long)offset;
    }
}
