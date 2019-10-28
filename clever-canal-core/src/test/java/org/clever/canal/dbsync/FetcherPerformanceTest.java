package org.clever.canal.dbsync;

import org.clever.canal.dbsync.binlog.DirectLogFetcher;
import org.clever.canal.dbsync.binlog.LogEvent;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicLong;

@SuppressWarnings({"TryFinallyCanBeTryWithResources", "CStyleArrayDeclaration", "UnusedAssignment", "SqlNoDataSourceInspection"})
public class FetcherPerformanceTest {

    public static void main(String args[]) {
        DirectLogFetcher fetcher = new DirectLogFetcher();
        try {
            Class.forName("com.mysql.jdbc.Driver");
            Connection connection = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306", "root", "hello");
            Statement statement = connection.createStatement();
            statement.execute("SET @master_binlog_checksum='@@global.binlog_checksum'");
            statement.execute("SET @mariadb_slave_capability='" + LogEvent.MARIA_SLAVE_CAPABILITY_MINE + "'");

            fetcher.open(connection, "mysql-bin.000006", 120L, 2);

            AtomicLong sum = new AtomicLong(0);
            long start = System.currentTimeMillis();
            long last = 0;
            long end = 0;

            while (fetcher.fetch()) {
                sum.incrementAndGet();
                long current = sum.get();
                if (current - last >= 100000) {
                    end = System.currentTimeMillis();
                    long tps = ((current - last) * 1000) / (end - start);
                    System.out.println(" total : " + sum + " , cost : " + (end - start) + " , tps : " + tps);
                    last = current;
                    start = end;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            fetcher.close();
        }
    }
}
