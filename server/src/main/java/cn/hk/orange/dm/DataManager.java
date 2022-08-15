package cn.hk.orange.dm;


import cn.hk.orange.dm.dataitem.DataItem;
import cn.hk.orange.dm.logger.Logger;
import cn.hk.orange.dm.page.PageOne;
import cn.hk.orange.dm.pagecache.PageCache;
import cn.hk.orange.tm.TransactionManager;


/**
 * DM 直接管理数据库DB文件和日志文件。DM的主要职责有：
 * 1) 分页管理DB文件，并进行缓存；
 * 2) 管理日志文件，保证在发生错误时可以根据日志进行恢复；
 * 3) 抽象DB文件为DataItem供上层模块使用，并提供缓存
 *
 * @author Cyf yifan_cao@ctrip.com
 */
public interface DataManager {
    DataItem read(long uid) throws Exception;

    long insert(long xid, byte[] data) throws Exception;

    void close();

    /**
     * @param path 日志和页缓存的文件路径 .db和.log
     * @param mem  页缓存大小
     * @param tm   tm
     * @return dm
     * @author Cyf yifan_cao@ctrip.com
     */
    static DataManager create(String path, long mem, TransactionManager tm) {
        PageCache pc = PageCache.create(path, mem);
        Logger lg = Logger.create(path);
        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);
        dm.initPageOne();
        return dm;
    }

    /**
     * @param path 日志和页缓存的文件路径 .db和.log
     * @param mem  页缓存大小
     * @param tm   tm
     * @return dm
     * @author Cyf yifan_cao@ctrip.com
     */
    static DataManager open(String path, long mem, TransactionManager tm) {
        PageCache pc = PageCache.open(path, mem);
        Logger lg = Logger.open(path);
        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);
        if (!dm.loadCheckPageOne()) {
            Recover.recover(tm, lg, pc);
        }
        dm.fillPageIndex();
        PageOne.setVcOpen(dm.pageOne);
        dm.pc.flushPage(dm.pageOne);

        return dm;
    }
}
