package cn.hk.orange.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;
import java.util.Random;

public class IOUtil {
    public static Long readLong(FileChannel fc, int offset) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(offset);
        fc.position(offset);
        fc.read(buf);
        return Parser.parseLong(buf.array());
    }
}
