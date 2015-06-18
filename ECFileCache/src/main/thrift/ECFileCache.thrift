namespace java com.xiaomi.filecache.thrift

struct FileCacheKey {
    1:required string uuid; // uuid, indicate an uniq session
    2:required byte version; // EC strategy version
    3:required i16 deviceClusterSize; // record current redis cluster 'size'
    4:required i16 deviceOffset; // use devices from 'deviceOffset' to '(k+m) % size' in redis cluster
    5:optional i64 fileSize;
}
