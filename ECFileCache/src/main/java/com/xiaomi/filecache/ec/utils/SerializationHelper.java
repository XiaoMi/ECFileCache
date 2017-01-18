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
package com.xiaomi.filecache.ec.utils;

import org.apache.commons.lang3.Validate;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransport;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

public class SerializationHelper {
  /**
   * convert bytes to thrift object
   *
   * @param clazz target Class type
   * @param bytes data
   * @param <T> Class type
   * @return thrift object
   */
  public static <T extends TBase<T, ?>> T toThriftObject(Class<T> clazz, byte[] bytes) {
    Validate.notNull(clazz);
    Validate.notNull(bytes);

    ByteArrayInputStream buffer = new ByteArrayInputStream(bytes);
    TTransport trans = new TIOStreamTransport(buffer);
    TCompactProtocol protocol = new TCompactProtocol(trans);
    try {
      T obj = clazz.newInstance();
      obj.read(protocol);
      return obj;
    } catch (InstantiationException e) {
      throw new IllegalStateException("unexpected", e);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException("unexpected", e);
    } catch (TException e) {
      String msg = "corrupted binary for thrift object:" + clazz.getName();
      throw new IllegalStateException(msg, e);
    }
  }

  /**
   * convert thrift object to bytes
   *
   * @param obj thrift object
   * @param <T> Class type
   * @return bytes data
   */
  public static <T extends TBase<T, ?>> byte[] toBytes(T obj) {
    Validate.notNull(obj);

    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    TTransport trans = new TIOStreamTransport(buffer);
    TCompactProtocol protocol = new TCompactProtocol(trans);
    try {
      obj.write(protocol);
      return buffer.toByteArray();
    } catch (TException e) {
      throw new IllegalStateException("unexpected", e);
    }
  }
}
