package gobblin.metastore.testing;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;


public class TestMetastoreDatabaseFactory {
    private static final Object syncObject = new Object();
    private static TestMetastoreDatabase testMetastoreDatabase;
    private static Set<ITestMetastoreDatabase> instances = new HashSet<>();

    private TestMetastoreDatabaseFactory() {
    }

    public static ITestMetastoreDatabase get() throws Exception {
        synchronized (syncObject) {
            ensureDatabaseExists();
            PrivateTestMetadataDatabase instance = new PrivateTestMetadataDatabase(testMetastoreDatabase);
            instances.add(instance);
            return instance;
        }
    }

    static void release(ITestMetastoreDatabase instance) throws IOException {
        synchronized (syncObject) {
            if (instances.remove(instance) && instances.size() == 0) {
                testMetastoreDatabase.close();
                testMetastoreDatabase = null;
            }
        }
    }

    private static void ensureDatabaseExists() throws Exception {
        if (testMetastoreDatabase == null) {
            try (Mutex ignored = new Mutex()) {
                if (testMetastoreDatabase == null) {
                    testMetastoreDatabase = new TestMetastoreDatabase();
                }
            }
        }
    }

    private static class Mutex implements Closeable {
        private final Object syncObject = new Object();
        private final AtomicBoolean isLocked = new AtomicBoolean(false);
        private FileChannel fileChannel;
        private FileLock fileLock;

        public Mutex() throws IOException {
            take();
        }

        @Override
        public void close() {
            release();
        }

        private boolean take() throws IOException {
            if (!isLocked.get()) {
                synchronized (syncObject) {
                    if (!isLocked.get()) {
                        if (fileChannel == null) {
                            Path lockPath = Paths.get(System.getProperty("user.home")).resolve(".embedmysql.lock");
                            fileChannel = FileChannel.open(lockPath, StandardOpenOption.CREATE,
                                    StandardOpenOption.WRITE, StandardOpenOption.READ);
                        }
                        fileLock = fileChannel.lock();
                        isLocked.set(true);
                        return true;
                    }
                    return true;
                }
            }
            return true;
        }

        private boolean release() {
            if (isLocked.get()) {
                synchronized (syncObject) {
                    if (isLocked.get()) {
                        if (fileLock != null) {
                            boolean result = true;
                            try {
                                fileLock.close();
                                fileLock = null;
                                isLocked.set(false);
                            } catch (IOException ignored) {
                                result = false;
                            }
                            if (fileChannel != null) {
                                try {
                                    fileChannel.close();
                                } catch (IOException ignored) {
                                    result = false;
                                }
                            }
                            return result;
                        }
                        isLocked.set(false);
                        return true;
                    }
                    return true;
                }
            }
            return true;
        }
    }
}

