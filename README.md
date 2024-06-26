# MIT6.824 Labs

## Lab2 Raft

### 100 次测试通过 🎉🎉

- PASS
- ok 6.824/raft 354.681s

### 问题一：加入快照后的并发问题

前三个根据图 2 写的比较简单，当写 2D 时，前面的逻辑都要改，很痛苦！

1. 接收成功，并记录快照后舍弃 219 后的日志
2. 还没更新，就又发送了一个 `appendentry`，导致越界。`AppendEntry` 内部锁了，但是 leader 的发送没锁。

   ![alt text](images/image-1.png)

### 解决方法：

在 Follower receive `appendentry` 的时候加入判定，如果已有更新的快照版本，则返回 rf.lastSnapshotIndex + 1 作为下一条需要的日志

![alt text](images/image-3.png)

![alt text](images/image-2.png)

### 问题二：持久化后，followers 的 `nextindex` 已在磁盘？

解决方法：在接收到 reply 后判断 xindex 和 lastsnapshot 之间的关系，如果小于则发送快照

![alt text](images/image.png)

### 问题三：加入 SnapShot 后，测试时日志数量错误

测试时报错：预期有 100 个 index，却出现 104 个。原因：

这个错误信息表示服务器 0 在应用日志时出现了顺序错误。它期望应用的日志索引为 100，但实际上得到的日志索引为 104。
这可能是因为在服务器崩溃并恢复后，它没有正确地从快照中恢复其状态，导致它试图跳过一些日志条目并直接应用索引为 104 的日志。

#### 解决思路

- 确保在服务器崩溃后，你正确地从快照中恢复了服务器的状态。这包括恢复 `lastApplied` 和 `commitIndex` 的值，以及恢复日志数组。
- 确保在应用日志之前，你已经检查了日志的索引是否与 `lastApplied` 相等。如果不相等，那么你可能需要跳过一些日志条目，直到找到一个索引与 `lastApplied` 相等的日志条目。
- 确保你在应用日志之后，正确地更新了 `lastApplied` 的值。你应该将 `lastApplied` 设置为刚刚应用的日志的索引。

解决方法：
每次 apply 前检查 lastapplied 和快照版本的关系，如果版本落后则安装本地的快照

![alt text](images/image-7.png)

- Tips：在持久化中未保存 rf.lastApplied 的状态，因为 rf.lastApplied 只能在 applier 函数中递增或者从快照中获得
- 本项目中用“rf.lastApplied = rf.lastSnapshotIndex”代替 apply 追赶的过程，实际工程中应根据磁盘中的日志文件（wal）进行追赶

### 问题 4：6.824 的持久化方式太粗暴

目前持久化方式为，每次发生一些变动就要把所有的状态都编码持久化一遍，这显然是生产不可用的。对 I/O 耗时、网络带宽压力都很大。解决方法：生产环境中，至少对于 raft 日志，应该是通过一个类似于 WAL 的方式来顺序写磁盘。

#### 构想可能的解决方法：

系统中的服务器使用 WAL，把执行操作的日志存储到磁盘中。Leader 会定期进行快照并将快照持久化，快照中包含内存中的状态、磁盘中已成功提交的日志。Leader 定期将日志快照发送给 follower，如果 follower 发现自己的日志版本低于快照，则进行快照安装和日志追赶。数据的快照本地存储，不通过网络传播，数据快照记录最后一条应用成功的日志编号，和 WAL 的日志序列比对。

### 问题 5：日志冲突靠递减恢复太慢

![alt text](images/image-4.png)

解决方法：

发生冲突时，将 `nextindex` 改为冲突日志任期的第一个日志。

![alt text](images/image-5.png)
![alt text](images/image-6.png)

## Lab3 KVRaft
## Notes:

### 写请求是线性一致性的 (linearizable writes)
- 对于所有的客户端发起的写请求，整体是线性一致性的。
- 如果一个客户端说，先完成这个写操作，再完成另一个写操作，之后是第三个写操作，那么在最终整体的写请求的序列中，可以看到这个客户端的写请求以相同顺序出现（虽然可能不是相邻的）。所以，对于写请求，最终会以客户端确定的顺序执行。
- 不同用户之间的写与读可能会出现交叉，但是对于同一个客户端的读写请求，是线性一致性的。即如果相同的client执行完写操作后，立即进行读操作，那么至少可以看到自己的写入结果。换言之，如果是其他client在前一个client写后进行读，zookeeper并不保证能读取到前一个client的写结果。

### 同一个客户端的请求按照 FIFO 顺序被执行 (FIFO client order)
- 如果一个特定的客户端发送了一个写请求之后是一个读请求或者任意请求，那么首先，所有的写请求会以这个客户端发送的相对顺序，加入到所有客户端的写请求中（满足保证1）。
- 对于读请求，在 Zookeeper 中读请求不需要经过 Leader，只有写请求经过 Leader，读请求可以到达某个副本。读请求只能看到那个副本的 Log 对应的状态。对于读请求，我们应该这么考虑 FIFO 客户端序列：
   - 客户端会以某种顺序读某个数据，之后读第二个数据，之后是第三个数据，对于那个副本上的 Log 来说，每一个读请求必然要在 Log 的某个特定的点执行，或者说每个读请求都可以在 Log 一个特定的点观察到对应的状态。
   - 如果一个客户端正在与一个副本交互，客户端发送了一些读请求给这个副本，之后这个副本故障了，客户端需要将读请求发送给另一个副本。这时，尽管客户端切换到了一个新的副本，FIFO 客户端序列仍然有效。所以这意味着，如果你知道在故障前，客户端在一个副本执行了一个读请求并看到了对应于 Log 中这个点的状态，当客户端切换到了一个新的副本并且发起了另一个读请求，假设之前的读请求在这里执行，那么尽管客户端切换到了一个新的副本，客户端的在新的副本的读请求，必须在 Log 这个点或者之后的点执行。

### FIFO 客户端序列的原理
- 每个 Log 条目都会被 Leader 打上 zxid 的标签，这些标签就是 Log 对应的条目号。任何时候一个副本回复一个客户端的读请求，首先这个读请求是在 Log 的某个特定点执行的，其次回复里面会带上 zxid，对应的就是 Log 中执行点的前一条 Log 条目号。
- 客户端在收到响应后，会记住最高的 zxid，当客户端发出一个请求到一个相同或者不同的副本时，它会在它的请求中带上这个最高的 zxid。这样，其他的副本就知道，应该至少在 Log 中这个点或者之后执行这个读请求。
- 如果第二个副本并没有最新的 Log，当它从客户端收到一个请求，客户端请求的 zxid 大于副本的最新 log，那么在获取到对应这个位置的 Log 之前，这个副本不能响应客户端请求。
- FIFO 客户端请求序列是对同一个客户端的所有读请求，写请求生效（如果客户端写请求发送给 leader，而又向副本发送了读请求，则需要等待副本也 apply 了该写请求，才能正确执行读）。

### zxid 判定的两种方案
- 一种是每个副本维护一个全局的 zxid，为最新 apply 的 log 的 index，每次有请求到来，将请求中的 zxid 与自己的 zxid 比较，如果请求的 zxid 大于自己的 zxid，则拒绝请求，等待自己 apply 了对应的 log 后再响应请求。
- 另一种是在每个 value 后标注最后一次更改该 value 的 zxid，当读该 value 的请求到来时，将请求中的 zxid 与 value 的 zxid 比较，如果请求的 zxid 大于 value 的 zxid，则拒绝请求，等待自己 apply 了对应的 log 后再响应请求。

## ZNodes
- Zookeeper 的数据模型是一个树形结构，类似于文件系统，树中的每个节点称为 ZNode。
- ZNode 有三种类型：持久节点，临时节点（客户端需要定期发送心跳）和顺序节点（节点名后面会加上一个唯一的单调递增的数字，保持唯一）。

## Zookeeper API in GO（可见zookeeper）
- `Create`：创建一个 ZNode，可以是持久节点、临时节点或者顺序节点。调用方式为`create(path, data, flags)`，其中`flags`参数对应上述 ZNode 的三种类型。
- `Delete`：删除一个 ZNode。调用方式为`delete(path, version)`，其中`version`参数指定版本。
- `Exists`：检查一个 ZNode 是否存在。调用方式为`exists(path, watch)`，其中`watch`参数指定是否监视该 ZNode 的变化。
- `Get`：获取一个 ZNode 的数据。调用方式为`getData(path, version)`，其中`version`参数指定版本。
- `GetW`：获取一个 ZNode 的数据，并且传入 `chan watcher`，监视 ZNode 的变化。调用方式为`getData(path, version)`，并在 `watch` 参数中传入 `chan watcher`。
- `Set`：设置一个 ZNode 的数据。调用方式为`setData(path, data, version)`，其中`version`参数指定版本。
- `Children`：获取一个 ZNode 的子节点。调用方式为`getChildren(path, watch)`，其中`watch`参数指定是否监视子节点的变化。
- `Sync`：同步一个 ZNode。
- `ACL`：访问控制列表。


## 非扩展锁 unscaleable locks
- 非扩展锁是指在一个节点上加锁，这样的锁可以用于实现分布式锁，但是会受到羊群效应的影响。
```
  WHILE TRUE:
  IF CREATE("f", data, ephemeral=TRUE): RETURN
  IF EXIST("f", watch=TRUE):
  WAIT
 ```

## 扩展锁 scalable locks
- 扩展锁是指每个节点都创建一个唯一序号的锁，若存在序号更低的锁则阻塞。这样可以避免羊群效应。
```
  CREATE("f", data, sequential=TRUE, ephemeral=TRUE)
  WHILE TRUE:
  LIST("f*")
  IF NO LOWER #FILE: RETURN
  IF EXIST(NEXT LOWER #FILE, watch=TRUE):
  WAIT
```
- 拓展锁的缺点：如果持有锁的客户端挂了，它会释放锁，另一个客户端可以接着获得锁，所以它并不确保原子性。因为你在分布式系统中可能会有部分故障（Partial Failure），但是你在一个多线程代码中不会有部分故障。如果当前锁的持有者需要在锁释放前更新一系列被锁保护的数据，但是更新了一半就崩溃了，之后锁会被释放。然后你可以获得锁，然而当你查看数据的时候，只能看到垃圾数据，因为这些数据是只更新了一半的随机数据。所以，Zookeeper实现的锁，并没有提供类似于线程锁的原子性保证。

- 对于这些锁的合理的场景是：Soft Lock。Soft Lock用来保护一些不太重要的数据。举个例子，当你在运行MapReduce Job时，你可以用这样的锁来确保一个Task同时只被一个Work节点执行。例如，对于Task 37，执行它的Worker需要先获得相应的锁，再执行Task，并将Task标记成执行完成，之后释放锁。MapReduce本身可以容忍Worker节点崩溃，所以如果一个Worker节点获得了锁，然后执行了一半崩溃了，之后锁会被释放，下一个获得锁的Worker会发现任务并没有完成，并重新执行任务。这不会有问题，因为这就是MapReduce定义的工作方式。所以你可以将这里的锁用在Soft Lock的场景。

## 链式复制 chain replication
- Chain Replication并不能抵御网络分区，也不能抵御脑裂。在实际场景中，这意味它不能单独使用。Chain Replication是一个有用的方案，但是它不是一个完整的复制方案。它在很多场景都有使用，但是会以一种特殊的方式来使用。总是会有一个外部的权威（External Authority）来决定谁是活的，谁挂了，并确保所有参与者都认可由哪些节点组成一条链，这样在链的组成上就不会有分歧。这个外部的权威通常称为Configuration Manager
- 相比CR，raft更能抵抗某些服务器的瞬态减速，因为它的日志复制是多数派决定的，而不是链式决定的。
## Lab4 ShardedKV
