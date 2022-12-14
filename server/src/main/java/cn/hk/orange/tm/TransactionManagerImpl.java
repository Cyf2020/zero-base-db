package cn.hk.orange.tm;

import cn.hk.orange.exception.ServerError;
import cn.hk.orange.utils.IOUtil;
import cn.hk.orange.utils.Panic;
import cn.hk.orange.utils.Parser;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 事务管理器
 * @author Cyf yifan_cao@ctrip.com
 */
public class TransactionManagerImpl implements TransactionManager {

    // XID文件头长度
    static final int LEN_XID_HEADER_LENGTH = 8;
    // 每个事务的占用长度
    private static final int XID_FIELD_SIZE = 1;

    // 事务的三种状态
    private static final byte FIELD_TRAN_ACTIVE = 0;
    private static final byte FIELD_TRAN_COMMITTED = 1;
    private static final byte FIELD_TRAN_ABORTED = 2;

    // 超级事务，永远为commited状态
    public static final long SUPER_XID = 0;

    static final String XID_SUFFIX = ".xid";

    private final RandomAccessFile file;
    private final FileChannel fc;
    private long xidCounter;
    private final Lock counterLock;

    TransactionManagerImpl(RandomAccessFile raf, FileChannel fc) {
        this.file = raf;
        this.fc = fc;
        counterLock = new ReentrantLock();
        checkXIDCounter();
    }

    /**
     * 检查XID文件是否合法
     * 读取XID_FILE_HEADER中的xidcounter，根据它计算文件的理论长度，对比实际长度
     *
     * @author Cyf yifan_cao@ctrip.com
     */
    private void checkXIDCounter() {
        try {
            this.xidCounter = IOUtil.readLong(fc, 0, 8);
            assert file.length() >= LEN_XID_HEADER_LENGTH : "XID header length error!";
            assert getXidPosition(this.xidCounter + 1) == file.length() : "XID end length error!";
        } catch (IOException e) {
            Panic.panic(ServerError.BadXIDFileException);
        }
    }

    /**
     * 根据事务xid取得其在xid文件中对应的位置
     *
     * @param xid xid
     * @return offset
     * @author Cyf yifan_cao@ctrip.com
     */
    private long getXidPosition(long xid) {
        return LEN_XID_HEADER_LENGTH + (xid - 1) * XID_FIELD_SIZE;
    }

    /**
     * 更新xid事务的状态为status
     *
     * @param xid    xid
     * @param status 事务状态
     * @author Cyf yifan_cao@ctrip.com
     * @see TransactionManagerImpl
     */
    private void updateXID(long xid, byte status) {
        long offset = getXidPosition(xid);
        byte[] tmp = new byte[XID_FIELD_SIZE];
        tmp[0] = status;
        ByteBuffer buf = ByteBuffer.wrap(tmp);
        try {
            fc.position(offset);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        try {
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    /**
     * 将XID加一，并更新XID Header
     *
     * @author Cyf yifan_cao@ctrip.com
     */
    private void incrXIDCounter() {
        ByteBuffer buf = ByteBuffer.wrap(Parser.long2Byte(++xidCounter));
        try {
            fc.position(0);
            fc.write(buf);
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    /**
     * 开始一个事务，并返回XID
     *
     * @return xid
     * @author Cyf yifan_cao@ctrip.com
     */
    public long begin() {
        counterLock.lock();
        try {
            long xid = xidCounter + 1;
            updateXID(xid, FIELD_TRAN_ACTIVE);
            incrXIDCounter();
            return xid;
        } finally {
            counterLock.unlock();
        }
    }

    /**
     * 提交XID事务
     *
     * @param xid xid
     * @author Cyf yifan_cao@ctrip.com
     */
    public void commit(long xid) {
        updateXID(xid, FIELD_TRAN_COMMITTED);
    }

    /**
     * 回滚XID事务
     *
     * @param xid xid
     * @author Cyf yifan_cao@ctrip.com
     */
    public void abort(long xid) {
        updateXID(xid, FIELD_TRAN_ABORTED);
    }

    /**
     * 检测XID事务是否处于status状态
     *
     * @param xid    xid
     * @param status status
     * @return boolean
     * @author Cyf yifan_cao@ctrip.com
     * @see TransactionManagerImpl
     */
    private boolean checkXID(long xid, byte status) {
        long offset = getXidPosition(xid);
        ByteBuffer buf = ByteBuffer.wrap(new byte[XID_FIELD_SIZE]);
        try {
            fc.position(offset);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        return buf.array()[0] == status;
    }

    /**
     * 是否active
     *
     * @param xid xid
     * @return boolean
     * @author Cyf yifan_cao@ctrip.com
     */
    public boolean isActive(long xid) {
        if (xid == SUPER_XID) return false;
        return checkXID(xid, FIELD_TRAN_ACTIVE);
    }

    /**
     * 是否committed
     *
     * @param xid xid
     * @return boolean
     * @author Cyf yifan_cao@ctrip.com
     */
    public boolean isCommitted(long xid) {
        if (xid == SUPER_XID) return true;
        return checkXID(xid, FIELD_TRAN_COMMITTED);
    }

    /**
     * 是否aborted
     *
     * @param xid xid
     * @return boolean
     * @author Cyf yifan_cao@ctrip.com
     */
    public boolean isAborted(long xid) {
        if (xid == SUPER_XID) return false;
        return checkXID(xid, FIELD_TRAN_ABORTED);
    }

    /**
     * 关闭资源
     *
     * @author Cyf yifan_cao@ctrip.com
     */
    public void close() {
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

}
