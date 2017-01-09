package com.xiaomi.filecache.ec.zk;

import java.util.List;

public interface ZKChildListener {

  /**
   * Will be called when there is child added/deleted.
   *
   * @param parentPath the parent ZK node path;
   * @param currentChildren the node name list of current children.
   */
  void onChanged(String parentPath, List<String> currentChildren);
}
