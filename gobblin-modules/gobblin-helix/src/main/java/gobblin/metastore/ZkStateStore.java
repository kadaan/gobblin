/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gobblin.metastore;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.io.Text;
import org.apache.helix.AccessOption;
import org.apache.helix.manager.zk.ByteArraySerializer;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.helix.store.zk.ZkHelixPropertyStore;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import gobblin.configuration.State;
import gobblin.metastore.util.StateStoreTableInfo;
import gobblin.util.io.StreamUtils;

import lombok.extern.slf4j.Slf4j;


/**
 * An implementation of {@link StateStore} backed by ZooKeeper.
 *
 * <p>
 *
 *     This implementation stores serialized {@link State}s as a blob in ZooKeeper in the Sequence file format.
 *     The ZK path is in the format /STORE_ROOT_DIR/STORE_NAME/TABLE_NAME.
 *     State keys are state IDs (see {@link State#getId()}), and values are objects of {@link State} or
 *     any of its extensions. Keys will be empty strings if state IDs are not set
 *     (i.e., {@link State#getId()} returns <em>null</em>). In this case, the
 *     {@link ZkStateStore#get(String, String, String)} method may not work.
 * </p>
 *
 * @param <T> state object type
 **/
@Slf4j
public class ZkStateStore<T extends State> implements StateStore<T> {

  // Class of the state objects to be put into the store
  private final Class<T> stateClass;
  private final HelixPropertyStore<byte[]> propStore;
  private final boolean compressedValues;
  private final String extension;

  /**
   * State store that stores instances of {@link State}s in a ZooKeeper-backed {@link HelixPropertyStore}
   * storeRootDir will be created when the first entry is written if it does not exist
   * @param connectString ZooKeeper connect string
   * @param storeRootDir The root directory for the state store
   * @param compressedValues should values be compressed for storage?
   * @param stateClass The type of state being stored
   * @throws IOException
   */
  public ZkStateStore(String connectString, String storeRootDir, boolean compressedValues, Class<T> stateClass,
          String extension)
      throws IOException {
    this.compressedValues = compressedValues;
    this.stateClass = stateClass;
    this.extension = normalizeExtension(extension);

    ZkSerializer serializer = new ByteArraySerializer();
    propStore = new ZkHelixPropertyStore<byte[]>(connectString, serializer, storeRootDir);
  }

  @Override
  public boolean create(String storeName) throws IOException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(storeName), "Store name is null or empty.");

    String path = formPath(storeName);
    return propStore.exists(path, 0) || propStore.create(path, ArrayUtils.EMPTY_BYTE_ARRAY,
        AccessOption.PERSISTENT);
  }

  @Override
  public boolean create(String storeName, String tableName) throws IOException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(storeName), "Store name is null or empty.");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName), "Table name is null or empty.");
    Preconditions.checkArgument(!isCurrent(tableName),
        String.format("Table name is %s or %s%s", StateStoreTableInfo.CURRENT_NAME, StateStoreTableInfo.CURRENT_NAME,
        this.extension));

    String path = formPath(storeName, tableName);
    if (propStore.exists(path, 0)) {
      throw new IOException(String.format("State already exists for storeName %s tableName %s", storeName,
          tableName));
    }

    return propStore.create(path, ArrayUtils.EMPTY_BYTE_ARRAY, AccessOption.PERSISTENT);
  }

  @Override
  public boolean exists(String storeName, String tableName) throws IOException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(storeName), "Store name is null or empty.");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName), "Table name is null or empty.");

    String path = formPath(storeName, tableName);
    return propStore.exists(path, 0);
  }

  @Override
  public void put(String storeName, String tableName, T state) throws IOException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(storeName), "Store name is null or empty.");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName), "Table name is null or empty.");
    Preconditions.checkArgument(!isCurrent(tableName),
        String.format("Table name is %s or %s%s", StateStoreTableInfo.CURRENT_NAME, StateStoreTableInfo.CURRENT_NAME,
        this.extension));
    Preconditions.checkNotNull(state, "State is null.");

    putAll(storeName, tableName, Collections.singletonList(state));
  }

  @Override
  public void putAll(String storeName, String tableName, Collection<T> states) throws IOException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(storeName), "Store name is null or empty.");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName), "Table name is null or empty.");
    Preconditions.checkArgument(!isCurrent(tableName),
        String.format("Table name is %s or %s%s", StateStoreTableInfo.CURRENT_NAME, StateStoreTableInfo.CURRENT_NAME,
        this.extension));
    Preconditions.checkNotNull(states, "States is null.");

    String path = formPath(storeName, tableName);
    try (ByteArrayOutputStream byteArrayOs = new ByteArrayOutputStream();
        OutputStream os = compressedValues ? new GZIPOutputStream(byteArrayOs) : byteArrayOs;
        DataOutputStream dataOutput = new DataOutputStream(os)) {

      for (T state : states) {
        addStateToDataOutputStream(dataOutput, state);
      }

      if (!exists(storeName, tableName) && !create(storeName, tableName)) {
        throw new IOException("Failed to create a state file for table " + tableName);
      }

      dataOutput.close();
      propStore.set(path, byteArrayOs.toByteArray(), AccessOption.PERSISTENT);
    }
  }

  @Override
  public T get(String storeName, String tableName, String stateId) throws IOException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(storeName), "Store name is null or empty.");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName), "Table name is null or empty.");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(stateId), "State id is null or empty.");

    String path = getTablePath(storeName, tableName);
    if (path == null) {
      return null;
    }

    byte[] data = propStore.get(path, null, 0);
    List<T> states = Lists.newArrayList();

    deserialize(data, states, stateId);

    if (states.isEmpty()) {
      return null;
    } else {
      return states.get(0);
    }
  }

  /**
   * Get a {@link State} with a given state ID from a the current table.
   *
   * @param storeName store name
   * @param stateId state ID
   * @return {@link State} with the given state ID or <em>null</em>
   *         if the state with the given state ID does not exist
   * @throws IOException
   */
  @Override
  public T getCurrent(String storeName, String stateId) throws IOException {
    return get(storeName, StateStoreTableInfo.CURRENT_NAME, stateId);
  }

  @Override
  public List<T> getAll(String storeName, String tableName) throws IOException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(storeName), "Store name is null or empty.");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName), "Table name is null or empty.");

    List<T> states = Lists.newArrayList();
    String path = getTablePath(storeName, tableName);
    byte[] data = propStore.get(path, null, 0);

    deserialize(data, states);

    return states;
  }

  @Override
  public List<T> getAllCurrent(String storeName) throws IOException {
    return getAll(storeName, StateStoreTableInfo.CURRENT_NAME);
  }

  @Override
  public List<T> getAll(String storeName) throws IOException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(storeName), "Store name is null or empty.");

    return getAll(storeName, Predicates.<String>alwaysTrue());
  }

  /**
   * Retrieve child names from the state store based on the path and a filtering predicate
   * @param path The path enclosing the children
   * @param predicate The predicate for child file filtering
   * @return list of child names matching matching the predicate
   * @throws IOException
   */
  protected Iterable<String> getAllChildren(String path, Predicate<String> predicate) throws IOException {
    List<String> children = propStore.getChildNames(path, 0);
    if (children == null) {
      return Collections.emptyList();
    }

    return Iterables.filter(children, predicate);
  }

  /**
   * Retrieve states from the state store based on the store name and a filtering predicate
   * @param storeName The store name enclosing the state files
   * @param predicate The predicate for state file filtering
   * @return list of states matching matching the predicate
   * @throws IOException
   */
  protected List<T> getAll(String storeName, Predicate<String> predicate) throws IOException {
    List<T> states = Lists.newArrayList();
    String path = formPath(storeName);
    byte[] data;

    for (String c : getAllChildren(storeName, predicate)) {
      data = propStore.get(path + "/" + c, null, 0);
      deserialize(data, states);
    }

    return states;
  }

  @Override
  public void delete(String storeName, String tableName) throws IOException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(storeName), "Store name is null or empty.");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName), "Table name is null or empty.");
    Preconditions.checkArgument(!isCurrent(tableName),
        String.format("Table name is %s or %s%s", StateStoreTableInfo.CURRENT_NAME, StateStoreTableInfo.CURRENT_NAME,
        this.extension));

    propStore.remove(formPath(storeName, tableName), 0);
  }

  @Override
  public void delete(String storeName) throws IOException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(storeName), "Store name is null or empty.");

    propStore.remove(formPath(storeName), 0);
  }

  /**
   * Deserialize data into a list of {@link State}s.
   * @param data byte array
   * @param states output list of states
   * @param stateId optional key filter. Set to null for no filtering.
   * @throws IOException
   */
  private void deserialize(byte[] data, List<T> states, String stateId) throws IOException {
    if (data != null) {
      Text key = new Text();

      try (ByteArrayInputStream bais = new ByteArrayInputStream(data);
          InputStream is = StreamUtils.isCompressed(data) ? new GZIPInputStream(bais) : bais;
          DataInputStream dis = new DataInputStream(is)){
        // keep deserializing while we have data
        while (dis.available() > 0) {
          T state = this.stateClass.newInstance();

          key.readFields(dis);
          state.readFields(dis);
          states.add(state);

          if (stateId != null && key.toString().equals(stateId)) {
            return;
          }
        }
      } catch (EOFException e) {
        // no more data. GZIPInputStream.available() doesn't return 0 until after EOF.
      } catch (RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
  }

  /**
   * Deserialize data into a list of {@link State}s.
   * @param data byte array
   * @param states output list of states
   * @throws IOException
   */
  private void deserialize(byte[] data, List<T> states) throws IOException {
    deserialize(data, states, null);
  }

  private boolean isCurrent(String tableName) {
    StateStoreTableInfo tableInfo = StateStoreTableInfo.get(tableName, this.extension);
    return tableInfo.isCurrent();
  }

  private String getTablePath(String storeName, String tableName) throws IOException {
    StateStoreTableInfo tableInfo = StateStoreTableInfo.get(tableName, this.extension);
    if (tableInfo.isCurrent()) {
      return getLatestTablePath(storeName, tableInfo.getPrefix());
    }
    return formPath(storeName, tableName);
  }

  private String getLatestTablePath(String storeName, final String tableNamePrefix) throws IOException {
    String path = Paths.get("/", storeName).toString();
    List<String> children = propStore.getChildNames(path, 0);

    if (children == null) {
      return null;
    }

    String latestStatePath = null;
    for (String c : children) {
      if ((Strings.isNullOrEmpty(tableNamePrefix) ||
        c.startsWith(tableNamePrefix + StateStoreTableInfo.TABLE_PREFIX_SEPARATOR)) &&
        c.endsWith(extension)) {
        if (latestStatePath == null) {
          log.debug("Latest table for {}/{}* set to {}", storeName, tableNamePrefix, c);
          latestStatePath = c;
        } else if (c.compareTo(latestStatePath) > 0) {
          log.debug("Latest table for {}/{}* set to {} instead of {}", storeName, tableNamePrefix, c, latestStatePath);
          latestStatePath = c;
        } else {
          log.debug("Latest table for {}/{}* left as {}. Previous table {} is being ignored", storeName, tableNamePrefix, latestStatePath, c);
        }
      }
    }

    return latestStatePath == null ? null : formPath(storeName, latestStatePath);
  }

  protected String formPath(String storeName) {
    return "/" + storeName;
  }

  protected String formPath(String storeName, String tableName) {
    return "/" + storeName + "/" + normalizeTableName(tableName);
  }

  private String normalizeExtension(String extension) {
    return extension.startsWith(".") ? extension : "." + extension;
  }

  private String normalizeTableName(String tableName) {
    return tableName.endsWith(this.extension) ? tableName : tableName + this.extension;
  }

  /**
   * Serializes the state to the {@link DataOutput}
   * @param dataOutput output target receiving the serialized data
   * @param state the state to serialize
   * @throws IOException
   */
  private void addStateToDataOutputStream(DataOutput dataOutput, T state) throws IOException {
    new Text(Strings.nullToEmpty(state.getId())).write(dataOutput);
    state.write(dataOutput);
  }
}
