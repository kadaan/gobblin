package gobblin.metastore.testing;

import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.Objects;
import java.util.UUID;


class PrivateTestMetadataDatabase implements ITestMetastoreDatabase {
  private final TestMetastoreDatabase testMetastoreDatabase;
  private final String database;

  PrivateTestMetadataDatabase(TestMetastoreDatabase testMetastoreDatabase) throws Exception {
    this.testMetastoreDatabase = testMetastoreDatabase;
    this.database = String.format("gobblin_%s", UUID.randomUUID().toString().replace("-", StringUtils.EMPTY));
    this.resetDatabase();
  }

  @Override
  public void close() throws IOException {
    try {
      this.testMetastoreDatabase.drop(database);
    } catch (URISyntaxException | SQLException ignored) {
    } finally {
      TestMetastoreDatabaseFactory.release(this);
    }
  }

  @Override
  public String getJdbcUrl() throws URISyntaxException {
    return this.testMetastoreDatabase.getJdbcUrl(this.database).toString();
  }

  @Override
  public void resetDatabase() throws Exception {
    this.testMetastoreDatabase.prepareDatabase(database);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    PrivateTestMetadataDatabase that = (PrivateTestMetadataDatabase) o;
    return Objects.equals(database, that.database);
  }

  @Override
  public int hashCode() {
    return Objects.hash(database);
  }
}
