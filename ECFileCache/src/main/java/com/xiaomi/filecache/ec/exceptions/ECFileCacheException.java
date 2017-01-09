package com.xiaomi.filecache.ec.exceptions;

public class ECFileCacheException extends Exception {
  private static final long serialVersionUID = -3212481760184821262L;

  public ECFileCacheException(String message, Throwable cause) {
    super(message, cause);
  }

  public ECFileCacheException(String message) {
    super(message);
  }
}
