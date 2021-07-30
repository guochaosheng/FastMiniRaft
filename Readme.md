# FastMiniRaft

 [![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)

轻量，高吞吐，强一致。FastMiniRaft 是 Raft 共识协议的 Java 实现。非常轻，源代码仅 200 ~ 300 Kb。高吞吐，8 核16G 内存 140M/S 云盘单机  TPS 可稳定维持 100 万左右（查看测试用例）。强一致，遵从 Raft 协议，补充异步性能优化（查看 Raft 异步性能优化日志安全性论证），同时通过 Jepsen 验证，确保强一致实现的可靠性（[查看测试用例](https://github.com/guochaosheng/FastMiniRaft/tree/master/docs/test/testcase_list.md)）。

## API Simple Example

* RaftClusterClientExample

```
// 1. 创建 RPC 客户端对象
RpcClient rpcClient = new RpcClient(new ServiceSerializer());

// 2. 访问集群的任一节点，获取集群领导者节点 ID
String serverCluster = "n1-127.0.0.1:6001;n2-127.0.0.1:6002;n3-127.0.0.1:6003;";
Map<String, String> servers = Arrays.asList(serverCluster.split(";")).stream()
								.collect(Collectors.toMap(s -> s.split("-")[0], s -> s.split("-")[1]));

String host = servers.values().iterator().next();
ClientService clientService = rpcClient.getService(host, ClientService.class);
String leaderId = clientService.getLeaderId().get();
System.out.printf("request get leader id, response: %s %n", leaderId);

// 3. 远程调用领导者节点 SET 接口
StoreService storeService = rpcClient.getService(servers.get(leaderId), StoreService.class);
byte[] key = "hello".getBytes();
byte[] value = "hi".getBytes();
CompletableFuture<Void> setFuture = storeService.set(key, value);
setFuture.get(30, TimeUnit.SECONDS);
System.out.printf("request set [key: %s, value: %s] %n", new String(key), new String(value));
```

## Requirements

* Java 8+
* slf4j library
* netty library
* rocksdb

## Quick Jepsen Test (Docker)

打开一个 shell，下载最新的项目代码并启动 jepsen 容器脚本

```
wget -O FastMiniRaft-master.zip https://github.com/guochaosheng/FastMiniRaft/archive/refs/heads/master.zip
unzip FastMiniRaft-master.zip
cd FastMiniRaft-master/fastminiraft-jepsen/docker
sh up.sh --dev
```

打开另一个 shell，使用 'docker exec-it jepsen control bash' 进入控制节点，然后

```
control run jepsen build
control run jepsen deploy
sh run_test.sh --nemesis partition-random-halves
```

如果最终显示 Everything looks good! 说明通过线性一致性验证，如果最终显示 Analysis invalid! 说明失败。

了解更多 jepsen 看 [jepsen-website](https://jepsen.io/)

了解更多测试用例和报告看 [fastminiraft-testcase-list](https://github.com/guochaosheng/FastMiniRaft/tree/master/docs/test/testcase_list.md)

## Features

1. 支持 KV 存储
2. 支持队列存储

## License

[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html) Copyright (C) Guo Chaosheng