package org.clever.canal.parse.inbound.mysql.rds.data;

import java.util.List;

@SuppressWarnings("unused")
public class RdsItem {

    private List<BinlogFile> BinLogFile;

    public List<BinlogFile> getBinLogFile() {
        return BinLogFile;
    }

    public void setBinLogFile(List<BinlogFile> binLogFile) {
        BinLogFile = binLogFile;
    }

    @Override
    public String toString() {
        return "RdsItem [BinLogFile=" + BinLogFile + "]";
    }
}
