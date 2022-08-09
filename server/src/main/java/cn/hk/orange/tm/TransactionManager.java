package cn.hk.orange.tm;

import cn.hk.orange.exception.ServerError;
import cn.hk.orange.utils.Panic;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Objects;


public interface TransactionManager {
    long begin();

    void commit(long xid);

    void abort(long xid);

    boolean isActive(long xid);

    boolean isCommitted(long xid);

    boolean isAborted(long xid);

    void close();

    static TransactionManagerImpl create(String path) {
        try {
            Files.createDirectories(Paths.get(path).getParent());
        } catch (IOException e) {
            Panic.panic(ServerError.PathException);
        }
        File f = new File(path + TransactionManagerImpl.XID_SUFFIX);
        try {
            if (!f.createNewFile()) {
                Panic.panic(ServerError.FileExistsException);
            }
        } catch (Exception e) {
            Panic.panic(e);
        }
        if (!f.canRead() || !f.canWrite()) {
            Panic.panic(ServerError.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        // 写空XID文件头
        ByteBuffer buf = ByteBuffer.wrap(new byte[TransactionManagerImpl.LEN_XID_HEADER_LENGTH]);
        try {
            Objects.requireNonNull(fc).position(0);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        return new TransactionManagerImpl(raf, fc);
    }

    static TransactionManagerImpl open(String path) {
        File f = new File(path + TransactionManagerImpl.XID_SUFFIX);
        if (!f.exists()) {
            Panic.panic(ServerError.FileNotExistsException);
        }
        if (!f.canRead() || !f.canWrite()) {
            Panic.panic(ServerError.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        return new TransactionManagerImpl(raf, fc);
    }
}
