/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.xiaomi.infra.ec;

/**
 * CodecInterface defines the interfaces the a codec class must implement.
 */
public interface CodecInterface {

  /**
   * Encodes specified data blocks. This method is thread safe and reenterable.
   *
   * @param data The data blocks matrix
   * @return The coding blocks matrix
   */
  public byte[][] encode(byte[][] data);

  /**
   * Decodes specified failed data blocks. This method is thread safe and
   * reenterable.
   *
   * @param erasures The failed data blocks list
   * @param data The data blocks matrix
   * @param coding The coding blocks matrix
   */
  public void decode(int[] erasures, byte[][]data, byte[][] coding);
}
