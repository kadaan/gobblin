package gobblin.metastore.testing;

import java.io.Closeable;
import java.net.URISyntaxException;


public interface ITestMetastoreDatabase extends Closeable {
    String getJdbcUrl() throws URISyntaxException;

    void resetDatabase() throws Exception;
}
