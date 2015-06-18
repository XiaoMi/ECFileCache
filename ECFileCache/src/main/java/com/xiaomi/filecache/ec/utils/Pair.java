package com.xiaomi.filecache.ec.utils;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public final class Pair<TFirst, TSecond> {
    private TFirst first;
    private TSecond second;

    private Pair(TFirst first, TSecond second) {
        this.first = first;
        this.second = second;
    }

    public TFirst getFirst() {
        return this.first;
    }

    public TSecond getSecond() {
        return this.second;
    }

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

    public int hashCode() {
        return (new HashCodeBuilder()).append(this.first).append(this.second).toHashCode();
    }

    public String toString() {
        return (new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)).append(this.first).append(this.second).toString();
    }

    public static <TFirst, TSecond> Pair<TFirst, TSecond> create(TFirst first, TSecond second) {
        return new Pair(first, second);
    }
}
