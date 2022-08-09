package cn.hk.orange.server;

import cn.hk.orange.exception.ServerError;
import cn.hk.orange.parser.Parser;
import cn.hk.orange.parser.statement.*;
import cn.hk.orange.tbm.BeginRes;
import cn.hk.orange.tbm.TableManager;


public class Executor {
    private long xid;
    TableManager tbm;

    public Executor(TableManager tbm) {
        this.tbm = tbm;
        this.xid = 0;
    }

    public void close() {
        if (xid != 0) {
            System.out.println("Abnormal Abort: " + xid);
            tbm.abort(xid);
        }
    }

    public byte[] execute(byte[] sql) throws Exception {
        System.out.println("Execute: " + new String(sql));
        Object stat = Parser.Parse(sql);
        if (stat instanceof Begin) {
            if (xid != 0) {
                throw ServerError.NestedTransactionException;
            }
            BeginRes r = tbm.begin((Begin) stat);
            xid = r.xid;
            return r.result;
        } else if (stat instanceof Commit) {
            if (xid == 0) {
                throw ServerError.NoTransactionException;
            }
            byte[] res = tbm.commit(xid);
            xid = 0;
            return res;
        } else if (stat instanceof Abort) {
            if (xid == 0) {
                throw ServerError.NoTransactionException;
            }
            byte[] res = tbm.abort(xid);
            xid = 0;
            return res;
        } else {
            return execute2(stat);
        }
    }

    private byte[] execute2(Object stat) throws Exception {
        boolean tmpTransaction = false;
        Exception e = null;
        if (xid == 0) {
            tmpTransaction = true;
            BeginRes r = tbm.begin(new Begin());
            xid = r.xid;
        }
        try {
            byte[] res = null;
            if (stat instanceof Show) {
                res = tbm.show(xid);
            } else if (stat instanceof Create) {
                res = tbm.create(xid, (Create) stat);
            } else if (stat instanceof Select) {
                res = tbm.read(xid, (Select) stat);
            } else if (stat instanceof Insert) {
                res = tbm.insert(xid, (Insert) stat);
            } else if (stat instanceof Delete) {
                res = tbm.delete(xid, (Delete) stat);
            } else if (stat instanceof Update) {
                res = tbm.update(xid, (Update) stat);
            }
            return res;
        } catch (Exception e1) {
            e = e1;
            throw e;
        } finally {
            if (tmpTransaction) {
                if (e != null) {
                    tbm.abort(xid);
                } else {
                    tbm.commit(xid);
                }
                xid = 0;
            }
        }
    }
}
