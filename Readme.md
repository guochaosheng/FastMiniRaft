# FastMiniRaft

[![Github Build Status](https://github.com/guochaosheng/FastMiniRaft/workflows/CI/badge.svg?branch=master)](https://github.com/guochaosheng/FastMiniRaft/actions)  [![Coverage Status](https://coveralls.io/repos/github/guochaosheng/FastMiniRaft/badge.svg)](https://coveralls.io/github/guochaosheng/FastMiniRaft)  [![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)

轻量，高吞吐，强一致。FastMiniRaft 是 Raft 共识协议的 Java 实现。非常轻，源代码仅 200 ~ 300 Kb。高吞吐，16 核 32G 内存 140 Mb/s 云盘单机性能  TPS 100 万（[查看测试用例和报告](https://github.com/guochaosheng/FastMiniRaft/tree/master/docs/test/testcase_list.md)）。强一致，遵从 Raft 协议，同时通过 Jepsen 验证，确保强一致实现的可靠性（[查看测试用例和报告](https://github.com/guochaosheng/FastMiniRaft/tree/master/docs/test/testcase_list.md)）。

## 应用

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

示例看：[fastminiraft-examples](https://github.com/guochaosheng/FastMiniRaft/tree/master/fastminiraft-example)

## 要求

* Java 8+
* slf4j library
* netty library
* rocksdb

## 验证

Jepsen 线性一致性验证

①  打开一个 shell，下载最新的项目代码并启动容器部署脚本

```
wget -O FastMiniRaft-master.zip https://github.com/guochaosheng/FastMiniRaft/archive/refs/heads/master.zip
unzip FastMiniRaft-master.zip
cd FastMiniRaft-master/fastminiraft-jepsen/docker
sh up.sh --dev
```

②  打开另一个 shell，使用 'docker exec -it jepsen-control bash' 进入控制节点命令行界面，进行构建、部署、运行一致性验证脚本

```
control run jepsen build
control run jepsen deploy
sh run_test.sh --nemesis partition-random-halves
```

测试用例和报告看：[fastminiraft-testcase-list](https://github.com/guochaosheng/FastMiniRaft/tree/master/docs/test/testcase_list.md)

## 功能

* 容错性，允许少数派故障
* 分区容忍性
* 线性一致读

## 许可

[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html) Copyright (C) Guo Chaosheng