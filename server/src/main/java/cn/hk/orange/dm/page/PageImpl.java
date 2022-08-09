package cn.hk.orange.dm.page;

import cn.hk.orange.dm.pagecache.PageCache;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PageImpl implements Page {
    private final int pageNumber;
    private final byte[] data;
    private boolean dirty;
    private final Lock lock;
    
    private final PageCache pc;

    public PageImpl(int pageNumber, byte[] data, PageCache pc) {
        this.pageNumber = pageNumber;
        this.data = data;
        this.pc = pc;
        lock = new ReentrantLock();
    }

    public void lock() {
        lock.lock();
    }

    public void unlock() {
        lock.unlock();
    }

    public void release() {
        pc.release(this);
    }

    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    public boolean isDirty() {
        return dirty;
    }

    public int getPageNumber() {
        return pageNumber;
    }

    public byte[] getData() {
        return data;
    }

}
