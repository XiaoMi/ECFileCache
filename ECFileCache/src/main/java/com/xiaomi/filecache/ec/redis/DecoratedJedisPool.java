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
package com.xiaomi.filecache.ec.redis;

import com.xiaomi.filecache.ec.Config;
import com.xiaomi.filecache.ec.exceptions.ECFileCacheException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

/**
 * A wrapper of JedisPool
 * save redis host and port
 */
public class DecoratedJedisPool extends JedisPool {

  private static final String ADDRESS_SEP = ":";

  static final Logger LOGGER = LoggerFactory.getLogger(DecoratedJedisPool.class.getName());

  private String host;
  private int port;

  private DecoratedJedisPool(JedisPoolConfig jedisPoolConfig, String host, int port) {
    super(jedisPoolConfig, host, port,
        Config.getInstance().getJedisConnectTimeoutMs(),
        Config.getInstance().getJedisSocketTimeoutMs(),
        Config.getInstance().getRedisPassword(),
        Protocol.DEFAULT_DATABASE, null);

    this.host = host;
    this.port = port;
  }

  /**
   * Constructs a redis pool
   *
   * @param redisAddress redis address, like host:port
   * @return DecoratedJedisPool
   * @throws ECFileCacheException
   */
  public static DecoratedJedisPool create(String redisAddress)
      throws ECFileCacheException {
    String[] address = redisAddress.split(ADDRESS_SEP);
    if (address.length < 2) {
      String verbose = String.format("invalid redis address[%s]", redisAddress);
      LOGGER.error(verbose);
      throw new ECFileCacheException(verbose);
    }
    String host = address[0];
    int port = Integer.parseInt(address[1]);

    Validate.isTrue(StringUtils.isNotEmpty(host));
    Validate.isTrue(port > 0);

    JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
    jedisPoolConfig.setMaxTotal(Config.getInstance().getJedisPoolMax());

    return new DecoratedJedisPool(jedisPoolConfig, host, port);
  }

  /**
   * Get redis address for log
   *
   * @return redis address, host:port
   */
  public String getRedisAddress() {
    return host + ":" + port;
  }
}
