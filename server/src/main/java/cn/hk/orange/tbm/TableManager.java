package cn.hk.orange.tbm;

import cn.hk.orange.dm.DataManager;
import cn.hk.orange.parser.statement.*;
import cn.hk.orange.utils.Parser;
import cn.hk.orange.vm.VersionManager;


public interface TableManager {
    BeginRes begin(Begin begin);

    byte[] commit(long xid) throws Exception;

    byte[] abort(long xid);

    byte[] show(long xid);

    byte[] create(long xid, Create create) throws Exception;

    byte[] insert(long xid, Insert insert) throws Exception;

    byte[] read(long xid, Select select) throws Exception;

    byte[] update(long xid, Update update) throws Exception;

    byte[] delete(long xid, Delete delete) throws Exception;

    static TableManager create(String path, VersionManager vm, DataManager dm) {
        Booter booter = Booter.create(path);
        booter.update(Parser.long2Byte(0));
        return new TableManagerImpl(vm, dm, booter);
    }

    static TableManager open(String path, VersionManager vm, DataManager dm) {
        Booter booter = Booter.open(path);
        return new TableManagerImpl(vm, dm, booter);
    }
}
