package org.clever.canal.parse.inbound.mysql.local;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.clever.canal.parse.exception.CanalParseException;

import java.io.File;
import java.util.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 维护binlog文件列表
 */
@SuppressWarnings({"unused", "FieldCanBeLocal", "UnusedReturnValue", "WeakerAccess"})
public class BinLogFileQueue {

    private String baseName = "mysql-bin.";
    private List<File> binlogs = new ArrayList<>();
    private File directory;
    private ReentrantLock lock = new ReentrantLock();
    private Condition nextCondition = lock.newCondition();
    private Timer timer = new Timer(true);
    private long reloadInterval = 10 * 1000L;   // 10秒
    private CanalParseException exception = null;

    public BinLogFileQueue(String directory) {
        this(new File(directory));
    }

    public BinLogFileQueue(File directory) {
        this.directory = directory;
        if (!directory.canRead()) {
            throw new CanalParseException("Binlog index missing or unreadable;  " + directory.getAbsolutePath());
        }
        List<File> files = listBinlogFiles();
        for (File file : files) {
            offer(file);
        }
        timer.scheduleAtFixedRate(
                new TimerTask() {
                    public void run() {
                        try {
                            // File errorFile = new File(BinLogFileQueue.this.directory,
                            // errorFileName);
                            // if (errorFile.isFile() && errorFile.exists()) {
                            // String text = StringUtils.join(IOUtils.readLines(new
                            // FileInputStream(errorFile)), "\n");
                            // exception = new CanalParseException(text);
                            // }
                            List<File> files = listBinlogFiles();
                            for (File file : files) {
                                offer(file);
                            }
                        } catch (Throwable e) {
                            exception = new CanalParseException(e);
                        }
                        if (exception != null) {
                            offer(null);
                        }
                    }
                },
                reloadInterval,
                reloadInterval
        );
    }

    /**
     * 根据前一个文件，获取符合条件的下一个binlog文件
     */
    public File getNextFile(File pre) {
        try {
            lock.lockInterruptibly();
            if (exception != null) {
                throw exception;
            }
            if (binlogs.size() == 0) {
                return null;
            } else {
                if (pre == null) {
                    // 第一次
                    return binlogs.get(0);
                } else {
                    int index = seek(pre);
                    if (index < binlogs.size() - 1) {
                        return binlogs.get(index + 1);
                    } else {
                        return null;
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } finally {
            lock.unlock();
        }
    }

    public File getBefore(File file) {
        try {
            lock.lockInterruptibly();
            if (exception != null) {
                throw exception;
            }
            if (binlogs.size() == 0) {
                return null;
            } else {
                if (file == null) {
                    // 第一次
                    return binlogs.get(binlogs.size() - 1);
                } else {
                    int index = seek(file);
                    if (index > 0) {
                        return binlogs.get(index - 1);
                    } else {
                        return null;
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 根据前一个文件，获取符合条件的下一个binlog文件
     */
    public File waitForNextFile(File pre) throws InterruptedException {
        try {
            lock.lockInterruptibly();
            if (binlogs.size() == 0) {
                // 等待新文件
                nextCondition.await();
            }
            if (exception != null) {
                throw exception;
            }
            if (pre == null) {
                // 第一次
                return binlogs.get(0);
            } else {
                int index = seek(pre);
                if (index < binlogs.size() - 1) {
                    return binlogs.get(index + 1);
                } else {
                    // 等待新文件
                    nextCondition.await();
                    // 唤醒之后递归调用一下
                    return waitForNextFile(pre);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 获取当前所有binlog文件
     */
    public List<File> currentBinlogs() {
        return new ArrayList<>(binlogs);
    }

    public void destory() {
        try {
            lock.lockInterruptibly();
            timer.cancel();
            binlogs.clear();
            // 唤醒线程，通知退出
            nextCondition.signalAll();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            lock.unlock();
        }
    }

    private boolean offer(File file) {
        try {
            lock.lockInterruptibly();
            if (file != null) {
                if (!binlogs.contains(file)) {
                    binlogs.add(file);
                    // 唤醒
                    nextCondition.signalAll();
                    return true;
                }
            }
            // 唤醒
            nextCondition.signalAll();
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } finally {
            lock.unlock();
        }
    }

    private List<File> listBinlogFiles() {
        List<File> files = new ArrayList<>(
                FileUtils.listFiles(
                        directory,
                        new IOFileFilter() {
                            public boolean accept(File file) {
                                Pattern pattern = Pattern.compile("\\d+$");
                                Matcher matcher = pattern.matcher(file.getName());
                                return file.getName().startsWith(baseName) && matcher.find();
                            }

                            public boolean accept(File dir, String name) {
                                return true;
                            }
                        },
                        null
                )
        );
        // 排一下序列
        files.sort(Comparator.comparing(File::getName));
        return files;
    }

    private int seek(File file) {
        for (int i = 0; i < binlogs.size(); i++) {
            File binlog = binlogs.get(i);
            if (binlog.getName().equals(file.getName())) {
                return i;
            }
        }
        return -1;
    }

    // ================== setter / getter ===================

    public void setBaseName(String baseName) {
        this.baseName = baseName;
    }
}
