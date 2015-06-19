# ECFileCache

ECFileCache is a distributed file cache system, based on Erasure code.
Cached file data store in redis.
ECFileCache allow you read cached file correctly while several data blocks erased.

## Dependency
Install gf-complete, jerasure. see java-erasure/README.md. (Thanks WuZesheng)

Install redis (>2.8.5).
Install Python(>2.6.6), pip-python install kazoo redis requests.

Install zookeeper.

## Usage
### Prepare zookeeper
Just start a zookeeper. Do not need to create any nodes,
redis_supervisor will create nodes automatically.

### Deploy redis
Deploy redis and python in several server,
 and redis info will be registered in zookeeper.

### Use ECFileCache
Install gf-complete, jerasure.
Import ECFileCache in your java code.

## Performance
Read/write 1.5MB files. List 99% percentile as below.

Serial interface: write 203ms, read 56ms.
Parallel interface: write 54ms, read 32ms.
