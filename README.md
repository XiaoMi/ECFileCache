# ECFileCache
---
ECFileCache is a distributed file cache, which based on Erasure Code and uses Redis for storage.


## Background
---
The files uploaded with file storage service are usually very large which need to be split into several fragments. File storage service would cache the file fragments.

A real scene is file storage service buffers file fragments in local cache first. Until the whole file is uploaded, they are transferred to persistent storage service and saved there.

Using local cache as temporary date buffer is risky. The cached data may get lost during service update or downtime. Moreover, to ensure the uploaded data is complete, all fragments of the file must be sent to the same server node.

We developed a distributed file cache based on erasure code to resolve this issue. As a standalone service, distributed file cache encodes and stores the file fragments in distributed service nodes.


## Distributed File Cache
---
Compared with other distributed caches, ECFileCache has no master and states for master-slave switches while keeping high availability. ECFileCache will still work even when several cache nodes fail at the same time.

Data will be encoded into a number of coded blocks (data block and redundant block) with erasure code. The original data can be recovered even several encoded blocks are lost.
For example, with RS erasure codes, the original data can be encoded into k data blocks and m redundant blocks. It can still be restored with up to m blocks missing.

Redis with closed persistence is used for distributed storage. Redis has high read and write performance and supports multiple data structures. ECFileCache stores coding block to Redis hash map.

ECFileCache client obtains the real-time status of the Redis Cluster via ZooKeeper. The state of the Redis Cluster is registered to ZooKeeper node. A new node is registered automatically at the time new Redis service is added. The corresponding node is deleted when signing off a Redis service.


## Architecture Diagrams
---


                         +-------------+
                         |     user    |
                         +------+------+
                                |
                                |
             ECFileCache Client |
            +-------------------v---------------------+
            |                                         |
            |             +------------+              |
            |             |    data    |              |
            |             +-+--------^-+              |
            |       encode  |        |  decode        |
            |               |        |                |
            |   +------+ +--v---+ +--+---+ +------+   |
            |   |data.0| |data.1| |data.2| |data.3|   | monitor Redis Cluster changing
            |   +------+ +------+ +------+ +------+   |         +---------------+
            |                                         +- - - - -+               |
            +------+---------+--------+--------+------+         |               |
                   |         |        |        |                |               |
    transfer data                                               |   ZooKeeper   |
                   |         |        |        |                |               |
            +------+---------+--------+--------+------+         |               |
            |Cache Cluster                            +- - - - -+               |
            |   +------+ +------+ +------+ +------+   |         +---------------+
            |   |Redis0| |Redis1| |Redis2| |Redis3|   | register Redis address
            |   +------+ +------+ +------+ +------+   |
            |                                         |
            +-----------------------------------------+


## Cache Data Structure
---
When data is uploaded from clients, a large file is split into several fragments, which can be uploaded simultaneously.
The whole file structure is shown in fig by jointing all fragments. Pos is the relative location of a fragment and size is the length of the fragment.

    +-----------------------------------------------------------+
    | pos0,size0 |   data0   | pos1,size1 |   data1   | ......  |
    +-----------------------------------------------------------+

The above structure can be shown with Redis hash structure.

                   Redis
    +--------------------------------+
    |Hash                            |
    |                                |
    |     Field            Value     |
    | +------------+    +---------+  |
    | | pos0,size0 +--> |  data0  |  |
    | +------------+    +---------+  |
    | +------------+    +---------+  |
    | | pos1,size1 +--> |  data1  |  |
    | +------------+    +---------+  |
    |                                |
    +--------------------------------+

Erasure code is used to make sure the original file can be recovered when several service nodes fail. The fig below shows that a data block is encoded into two coded blocks in cache.

When cache data, coded block sequence is encoded from fragments data block. Coded blocks are stored into corresponding Redis servers.

When restore data, cached data are read from Redis servers in order. Original data can be restored if the number of failing node is less than the number of Erasure code redundant blocks.

  Encoded data:

     +------------+-----------+------------+-----------+
     | pos0,size0 |   data0   | pos1,size1 |   data1   |
     +------------+-----+-----+------------+------+----+
                        |                         |
                   +----+----+               +----+----+
                   | data0.0 |               | data1.0 |
                   +---------+               +---------+
                   +---------+               +---------+
                   | data0.1 |               | data1.1 |
                   +---------+               +---------+

  Cached data:

                   Redis0                                 Redis1
    +----------------------------------+  +----------------------------------+
    |Hash                              |  |Hash                              |
    |                                  |  |                                  |
    |     Field            Value       |  |     Field            Value       |
    | +------------+    +-----------+  |  | +------------+    +-----------+  |
    | | pos0,size0 +--> |  data0.0  |  |  | | pos0,size0 +--> |  data0.1  |  |
    | +------------+    +-----------+  |  | +------------+    +-----------+  |
    | +------------+    +-----------+  |  | +------------+    +-----------+  |
    | | pos1,size1 +--> |  data1.0  |  |  | | pos1,size1 +--> |  data1.1  |  |
    | +------------+    +-----------+  |  | +------------+    +-----------+  |
    |                                  |  |                                  |
    +----------------------------------+  +----------------------------------+


## Distributed File Cache Components
---
### ZooKeeper
As the bridge of Redis cache and FileCache client, ZooKeeper keeps the real-time status of Redis Clusters for FileCache client.

## Redis Cluster
Redis Cluster saves EC encoded data.
Service redis_supervisor registers Redis address in ZooKeeper, maintains the connection to ZooKeeper and automatically reconnects when session expires.

### FileCache Client
Client does EC encoding/decoding and acquires real-time status, read/write cached data of Redis Cluster based on the registered information in ZooKeeper.


## Usage of Distributed File Cache
---
### Dependencies
*  FileCache clients
    *    ECCodec: gf-comlete, jerasure.
*  Redis Cluster
    *    Redis: Redis (>2.8.5)
    *    Python: python(>2.8.6). Pyhton library Kazoo.

### Configuration
*  redis_supervisor
    *   Use config file ‘supervisor.conf’ to specify executable file path, ZooKeeper server address and node path
*  FileCache client
    *   Use config file ‘cluster.properties’ to specify ZooKeeper server address. The parameters to access Redis are saved in ZooKeeper.

### Data Write/Read Interface
*  Write Data:
    *   Both of serial and parallel interfaces are available.
    *   Serial interface is used for high throughput, latency insensitive requests and parallel interface is for low throughput, latency sensitive requests.
*  Read Data:
    *   Return stream of decoded data

### Performance
*  For files of 1.5 MB, 99% latency is
    *   Serial interface: write time is 203ms, read time is 56ms
    *   Parallel interface: write time is 54ms, read time is 32ms
*  Service QPS is only restricted by the bandwidth from client to Redis Cluster.


* * * * * *

* * * * * *


# ECFileCache
---
  ECFileCache是一个分布式文件缓存服务，基于纠删编码（Erasure Code）对数据进行编码，使用Redis全内存存储缓存数据。


## 背景介绍
---
  在文件存储服务中，用户上传的文件通常比较大，需要分片上传，文件存储服务中对用户上传的文件分片做缓存。
  一个实际场景是，处理用户上传/下载的服务，在服务本地缓存用户上传的数据，接收完整个文件数据后，再传输给持久化存储服务中保存。

  在服务本地做数据临时缓存，当服务升级或服务器宕机时，将导致缓存到本地的数据丢失。而且，同一文件的所有分片数据要发送到同一个服务节点，这样才能保证最后得到完整的文件。

  基于纠删码的分布式文件缓存作为一个单独的服务，通过就删编码对文件分片编码，并分布存储在多个节点，解决了数据缓存在本地时存在的问题。


## 分布式文件缓存
---
  相比于其他分布式缓存，ECFileCache不存在单点，没有状态，不需要主从切换，同时仍然具有高可用性。即使多个缓存节点同时故障，ECFileCache仍然可以正常工作。

  纠删码将数据编码为多个编码块(数据块和冗余块)，丢失部分编码块后仍能恢复出原始数据。例如RS纠删码将原始数据编码为k个数据块，m个冗余块，允许最多丢失m个块，仍能正确解码出原始数据。

  分布式存储使用Redis全内存存储，关闭持久化。Redis具有较高的读写性能，且支持多种数据结构。服务中使用Redis的hash结构存储编码块。

  FileCache客户端通过ZooKeeper获取Redis集群的实时状态。Redis集群的状态注册到ZooKeeper节点。新增Redis服务时主动注册一个新节点；下线Redis服务时自动删除自己注册的节点。

## 架构图
---
                         +-------------+
                         |     user    |
                         +------+------+
                                |
                                |
             ECFileCache Client |
            +-------------------v---------------------+
            |                                         |
            |             +------------+              |
            |             |    data    |              |
            |             +-+--------^-+              |
            |       encode  |        |  decode        |
            |               |        |                |
            |   +------+ +--v---+ +--+---+ +------+   |
            |   |data.0| |data.1| |data.2| |data.3|   | monitor Redis Cluster changing
            |   +------+ +------+ +------+ +------+   |         +---------------+
            |                                         +- - - - -+               |
            +------+---------+--------+--------+------+         |               |
                   |         |        |        |                |               |
    transfer data                                               |   ZooKeeper   |
                   |         |        |        |                |               |
            +------+---------+--------+--------+------+         |               |
            |Cache Cluster                            +- - - - -+               |
            |   +------+ +------+ +------+ +------+   |         +---------------+
            |   |Redis0| |Redis1| |Redis2| |Redis3|   | register Redis address
            |   +------+ +------+ +------+ +------+   |
            |                                         |
            +-----------------------------------------+


## 缓存的数据结构
---
  客户端上传数据时，将一个大文件分为多片分别上传，将所有分片标记位移和长度，拼成的完整文件结构如图。
  pos表示该分片相对于整个文件的位置，size表示该分片的长度。

    +-----------------------------------------------------------+
    | pos0,size0 |   data0   | pos1,size1 |   data1   | ......  |
    +-----------------------------------------------------------+

  把以上的结构用Redis的Hash结构表示:

                   Redis
    +--------------------------------+
    |Hash                            |
    |                                |
    |     Field            Value     |
    | +------------+    +---------+  |
    | | pos0,size0 +--> |  data0  |  |
    | +------------+    +---------+  |
    | +------------+    +---------+  |
    | | pos1,size1 +--> |  data1  |  |
    | +------------+    +---------+  |
    |                                |
    +--------------------------------+


  为了保证多个服务节点故障时仍能恢复原始文件，在此基础上引入纠删编码。

  下图展示了将一个数据片经过EC编码为两个编码块缓存的情况。
  缓存数据时，分片数据编码为编码块序列，编码块按顺序存储到按编号的Redis服务器中。
  恢复数据时，按顺序读取Redis服务器中该文件的缓存数据，只要故障节点数量小于纠删编码的冗余块数量，即可恢复出原始数据。

  数据编码:

     +------------+-----------+------------+-----------+
     | pos0,size0 |   data0   | pos1,size1 |   data1   |
     +------------+-----+-----+------------+------+----+
                        |                         |
                   +----+----+               +----+----+
                   | data0.0 |               | data1.0 |
                   +---------+               +---------+
                   +---------+               +---------+
                   | data0.1 |               | data1.1 |
                   +---------+               +---------+

  缓存数据:

                   Redis0                                 Redis1
    +----------------------------------+  +----------------------------------+
    |Hash                              |  |Hash                              |
    |                                  |  |                                  |
    |     Field            Value       |  |     Field            Value       |
    | +------------+    +-----------+  |  | +------------+    +-----------+  |
    | | pos0,size0 +--> |  data0.0  |  |  | | pos0,size0 +--> |  data0.1  |  |
    | +------------+    +-----------+  |  | +------------+    +-----------+  |
    | +------------+    +-----------+  |  | +------------+    +-----------+  |
    | | pos1,size1 +--> |  data1.0  |  |  | | pos1,size1 +--> |  data1.1  |  |
    | +------------+    +-----------+  |  | +------------+    +-----------+  |
    |                                  |  |                                  |
    +----------------------------------+  +----------------------------------+


## 分布式文件缓存组件
---
### ZooKeeper
  作为Redis缓存和FileCache客户端的桥梁，保存Redis集群的实时信息，供FileCache客户端使用。

### Redis集群 (redis_supervisor目录)
  Redis集群存储EC编码后的数据。
  redis_supervisor服务将Redis地址注册到ZooKeeper节点，并维护与ZooKeeper的连接、会话过期自动重连等。

### FileCache客户端 (ECFileCache目录)
  客户端本地进行EC编码和解码。根据ZooKeeper中的注册信息，获取Redis集群的实时情况，写入和读出缓存数据。


## 分布式文件缓存使用
---
### 依赖
*  FileCache客户端
    *    ECCodec：gf-complete, jerasure.
*  Redis缓存集群
    *    Redis：Redis (>2.8.5).
    *    Python：Python(>2.6.6) .Python library Kazoo.

### 配置
*  redis_supervisor
    *    使用配置文件supervisor.conf，指定Redis可执行文件路径，ZooKeeper服务地址和节点路径。
*  FileCache客户端
    *    配置文件cluster.properties中指定ZooKeeper服务器地址。访问Redis相关的参数配置在ZooKeeper节点中。

### 数据写入/读出接口
*  写入数据:提供串行和并行两套接口。串行接口适合数据量大，对延时不敏感的请求；并行接口适合数据量小，要求低延时的请求。
*  读出数据:返回解码得到的原始数据的数据流。

### 性能
*  对于1.5MB的文件，99% 延时为
    *    串行接口：写入耗时 203ms，读出耗时 56ms。
    *    并行接口：写入耗时 54ms，读出耗时 32ms。
*  服务QPS只受限于客户端到Redis缓存集群的带宽。


## Example
---
    +------------------------------------------------------------------------------------------------
    |  short clusterId = 0;
    |  short partitionId = 0;
    |  ECFileCache fileCache = new ECFileCache(clusterId, partitionId);
    |
    |  int fileSize = 2048;
    |  int chunkSize = 1024;
    |  byte[] data = new byte[fileSize];
    |  new Random().nextBytes(data);
    |
    |  // create cache key
    |  String key = fileCache.createFileCacheKey(fileSize);
    |
    |  // put file
    |  InputStream inputStream = new ByteArrayInputStream(ArrayUtils.subarray(data, 0, chunkSize));
    |  fileCache.putFile(key, 0, inputStream, 0L);
    |  inputStream.close();
    |
    |  inputStream = new ByteArrayInputStream(ArrayUtils.subarray(data, chunkSize, fileSize));
    |  fileCache.putFile(key, chunkSize, inputStream, 0L);
    |  inputStream.close();
    |
    |  // get file
    |  InputStream cachedStream = fileCache.asInputStream(key);
    |  byte[] cachedFile = IOUtils.toByteArray(cachedStream);
    |  Assert.assertArrayEquals(data, cachedFile);
    |
    |  // delete file
    |  fileCache.deleteFile(key);
    +------------------------------------------------------------------------------------------------

