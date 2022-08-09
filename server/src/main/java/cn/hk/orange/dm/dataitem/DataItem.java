package cn.hk.orange.dm.dataitem;

import java.util.Arrays;

import cn.hk.orange.common.SubArray;
import cn.hk.orange.dm.DataManagerImpl;
import cn.hk.orange.dm.page.Page;
import cn.hk.orange.utils.Parser;
import cn.hk.orange.utils.Types;
import com.google.common.primitives.Bytes;


public interface DataItem {
    SubArray data();

    void before();

    void unBefore();

    void after(long xid);

    void release();

    void lock();

    void unlock();

    void rLock();

    void rUnLock();

    Page page();

    long getUid();

    byte[] getOldRaw();

    SubArray getRaw();

    static byte[] wrapDataItemRaw(byte[] raw) {
        byte[] valid = new byte[1];
        byte[] size = Parser.short2Byte((short) raw.length);
        return Bytes.concat(valid, size, raw);
    }

    // 从页面的offset处解析处dataitem
    static DataItem parseDataItem(Page pg, short offset, DataManagerImpl dm) {
        byte[] raw = pg.getData();
        short size = Parser.parseShort(Arrays.copyOfRange(raw, offset + DataItemImpl.OF_SIZE, offset + DataItemImpl.OF_DATA));
        short length = (short) (size + DataItemImpl.OF_DATA);
        long uid = Types.addressToUid(pg.getPageNumber(), offset);
        return new DataItemImpl(new SubArray(raw, offset, offset + length), new byte[length], pg, uid, dm);
    }

    static void setDataItemRawInvalid(byte[] raw) {
        raw[DataItemImpl.OF_VALID] = (byte) 1;
    }
}
