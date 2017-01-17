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

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public final class Pair<T, U> {
  private T first;
  private U second;

  private Pair(T first, U second) {
    this.first = first;
    this.second = second;
  }

  public T getFirst() {
    return this.first;
  }

  public U getSecond() {
    return this.second;
  }

  @Override
  public boolean equals(Object obj) {
    if(this == obj) {
      return true;
    } else if(obj != null && obj.getClass() == this.getClass()) {
      Pair other = (Pair)obj;
      return (new EqualsBuilder()).append(this.first, other.first).append(this.second, other.second).isEquals();
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return (new HashCodeBuilder()).append(this.first).append(this.second).toHashCode();
  }

  @Override
  public String toString() {
    return (new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)).append(this.first).append(this.second).toString();
  }

  public static <T, U> Pair<T, U> create(T first, U second) {
    return new Pair<T, U>(first, second);
  }
}
