package cn.hk.orange;

import cn.hk.orange.dm.DataManager;
import cn.hk.orange.exception.ServerError;
import cn.hk.orange.tbm.TableManager;
import cn.hk.orange.tm.TransactionManager;
import cn.hk.orange.utils.Panic;
import cn.hk.orange.vm.VersionManager;
import cn.hk.orange.vm.VersionManagerImpl;



public class Server {

    public static final int port = 9999;

    public static final long DEFALUT_MEM = (1 << 20) * 64;
    public static final long KB = 1 << 10;
    public static final long MB = 1 << 20;
    public static final long GB = 1 << 30;

//    public static void main(String[] args) throws ParseException {
//        Options options = new Options();
//        options.addOption("open", true, "-open DBPath");
//        options.addOption("create", true, "-create DBPath");
//        options.addOption("mem", true, "-mem 64MB");
//        CommandLineParser parser = new DefaultParser();
//        CommandLine cmd = parser.parse(options,args);
//
//        if(cmd.hasOption("open")) {
//            openDB(cmd.getOptionValue("open"), parseMem(cmd.getOptionValue("mem")));
//            return;
//        }
//        if(cmd.hasOption("create")) {
//            createDB(cmd.getOptionValue("create"));
//            return;
//        }
//        System.out.println("Usage: launcher (open|create) DBPath");
//    }

    public static void main(String[] args) {
//        createDB("D:\\Users\\yifan_cao\\Desktop\\tmp\\mydb");
        openDB("D:\\Users\\yifan_cao\\Desktop\\tmp\\mydb", 64 * MB);
    }

    private static void createDB(String path) {
        TransactionManager tm = TransactionManager.create(path);
        DataManager dm = DataManager.create(path, DEFALUT_MEM, tm);
        VersionManager vm = new VersionManagerImpl(tm, dm);
        TableManager.create(path, vm, dm);
        tm.close();
        dm.close();
    }

    private static void openDB(String path, long mem) {
        TransactionManager tm = TransactionManager.open(path);
        DataManager dm = DataManager.open(path, mem, tm);
        VersionManager vm = new VersionManagerImpl(tm, dm);
        TableManager tbm = TableManager.open(path, vm, dm);
        new cn.hk.orange.server.Server(port, tbm).start();
    }

    private static long parseMem(String memStr) {
        if (memStr == null || "".equals(memStr)) {
            return DEFALUT_MEM;
        }
        if (memStr.length() < 2) {
            Panic.panic(ServerError.InvalidMemException);
        }
        String unit = memStr.substring(memStr.length() - 2);
        long memNum = Long.parseLong(memStr.substring(0, memStr.length() - 2));
        switch (unit) {
            case "KB":
                return memNum * KB;
            case "MB":
                return memNum * MB;
            case "GB":
                return memNum * GB;
            default:
                Panic.panic(ServerError.InvalidMemException);
        }
        return DEFALUT_MEM;
    }
}
