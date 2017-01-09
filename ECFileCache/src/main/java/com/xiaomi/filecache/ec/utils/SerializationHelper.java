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
