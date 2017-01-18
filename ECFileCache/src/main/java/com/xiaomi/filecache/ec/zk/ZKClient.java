// Copyright 2016 Xiaomi, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package com.xiaomi.filecache.ec.zk;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.BytesPushThroughSerializer;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Properties;

public class ZKClient {
  private static final Logger LOGGER = LoggerFactory.getLogger(ZKChildMonitor.class);

  private static final Charset DEFAULT_CHARSET = Charset.forName("UTF-8");
  private static final String SLASH = "/";
  private static final int SESSION_TIMEOUT = 30000;
  private static final int CONNECTION_TIMEOUT = 30000;

  private final ZkClient client;

  /**
   * Create new ZkClient of servers
   *
   * @param servers ZooKeeper client hosts
   */
  public ZKClient(String servers) {
    client = new ZkClient(servers, SESSION_TIMEOUT, CONNECTION_TIMEOUT, new BytesPushThroughSerializer());
  }

  /**
   * Create persistent node of path
   *
   * @param path ZooKeeper node path
   */
  public void createPersistent(String path) {
    client.createPersistent(getRealPath(path), true);
  }

  /**
   * Get children nodes of path
   *
   * @param path list children of path
   * @return children list
   */
  public List<String> getChildren(String path) {
    return client.getChildren(getRealPath(path));
  }

  public void registerChildChanges(final String path, final ZKChildListener listener) {
    String realPath = getRealPath(path);
    final IZkChildListener underlyingListener = new IZkChildListener() {
      @Override
      public void handleChildChange(String parentPath, List<String> currentChilds) {
        listener.onChanged(parentPath, currentChilds);
      }
    };
    client.subscribeChildChanges(realPath, underlyingListener);
  }

  /**
   * Get data of node path
   *
   * @param clazz target Class type
   * @param path get data of path
   * @param <T> Class type
   * @return node data of clazz type
   */
  @SuppressWarnings("unchecked")
  public <T> T getData(Class<T> clazz, String path) {
    byte[] bytes = client.readData(getRealPath(path));

    if (bytes == null) {
      return null;
    }

    if (clazz == Properties.class) {
      Reader inputReader = new InputStreamReader(new ByteArrayInputStream(bytes), DEFAULT_CHARSET);
      try {
        Properties p = new Properties();
        p.load(inputReader);
        return (T) p;
      } catch (IOException e) {
        LOGGER.error("Deserialize properties failed.", e);
        return null;
      } finally {
        IOUtils.closeQuietly(inputReader);
      }
    } else if (clazz == String.class) {
      return (T) new String(bytes);
    }

    LOGGER.error(String.format("The class %s is not supported.", clazz));
    return null;
  }

  private String getRealPath(String path) {
    return SLASH.equals(path) ? path : StringUtils.removeEnd(path, SLASH);
  }
}
