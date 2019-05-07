# TaaS
TaaS是一个兼容[Seata](https://github.com/seata/seata)（0.5版本以后）的分布式事务解决方案的高可用的服务端组件，使用Golang开发，由InfiniVision (http://infinivision.cn) 贡献给Seata开源社区。

## 特性
- 高可用，容错
- Auto-Rebalance
- 高性能，Taas性能可根据机器数量线性伸缩
- 强一致元数据存储

## 架构
![](./images/taas.png)

### Seata-TC
事务协调器进程，每个进程包含多个Fragment

### Proxy
无状态节点，对外提供正确的路由，把请求发送到对应的Fragment的Leader节点

### Elasticell
[Elasticell](https://github.com/deepfabric/elasticell)是高可用多副本强一致的分布式KV存储，提供元数据的存储。

## 设计
### 高性能
Taas的性能和机器数量成正比，为了支持这个特性，在Taas中处理全局事务的最小单位是一个`Fragment`，系统在启动的时候会设定每个Fragment支持的活跃全局事务的并发数，同时Taas会对每个Fragment进行采样，达到设定的饱和比例，Taas会生成新的Fragment来处理更多的并发。

### 高可用
每个`Fragment`有多个副本和一个Leader，由Leader来处理请求。当Leader出现故障，系统会产生一个新的Leader来处理请求，在新Leader的选举过程中，这个Fragment对外不提供服务，通常这个间隔时间是几秒钟。

### 强一致
Taas本身不存储全局事务的元数据，元数据存储在[Elasticell](https://github.com/deepfabric/elasticell)中，Elasticell是一个兼容redis协议的分布式的KV存储，它基于Raft协议来保证数据的一致性。

### Auto-Rebalance
随着系统的运行，在系统中会存在许多`Fragment`以及它们的副本，这样会导致在每个机器上，`Fragment`的分布不均匀，特别是当旧的机器下线或者新的机器上线的时候。Taas在启动的时候，会选择3个节点作为调度器的角色，调度器负责调度这些`Fragment`，用来保证每个机器上的Fragment的数量以及Leader个数大致相等，同时还会保证每个Fragment的副本数维持在指定的副本个数。

## 快速体验
```bash
git clone https//github.com/seata/taas.git
docker-compse up -d
```

### Seata服务地址
服务默认监听在8091端口，修改Seata对应的服务端地址体验

### Seata UI 
访问WEB UI `http://127.0.0.1:8084/ui/index.html`

## 关于深见
深见网络是一家技术驱动的企业级服务提供商，致力于利用人工智能、云计算、区块链、大数据，以及物联网边缘计算技术助力传统企业的数字化转型和升级。深见网络积极拥抱开源文化并将核心算法和架构开源，知名人脸识别软件[InsightFace](https://github.com/deepinsight/insightface)(曾多次获得大规模人脸识别挑战冠军)，以及分布式存储引擎[Elasticell](https://github.com/deepfabric/elasticell)等均是深见网络的开源产品。


