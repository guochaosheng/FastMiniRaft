# 测试用例

## 测试用例规范
* 原则：可观测，可操作，可复现

* 为了避免实际操作过程中不同测试组的测试结果和结论的出现显著偏差，所有案例至少必须明确如下要点信息：
  1. 源码包
  2. 编译 JDK 和 运行环境 JDK 详细名称和版本（推荐提供对应下载链接）
  3. Maven 版本，非默认配置需要提供相应配置文件
  4. 操作系统名称和版本（推荐提供对应下载链接）
  5. 服务器参数信息，包括机型，CPU型号和核心数，内存大小，网络IO吞吐量，磁盘IOPS和最大吞吐量
  6. 所需部署节点的关系结构图或者拓扑图
  7. 执行命令清单和操作流程说明


## 基准测试清单
| 基准测试清单                                                                                                  | 状态 |
| :---------------------------------------------------------- | :--- |
| 异步刷盘，3 个 服务器节点（8C16G 140 Mb/s）, 8 个客户端节点              | 待完 |
| 同步刷盘，3 个 服务器节点（8C16G 140 Mb/s）, 8 个客户端节点              | 待完 |

## 可靠性测试清单
| 基准测试清单                       | 状态                                                         |
| :--------------------------------- | :----------------------------------------------------------- |
| 随机 KILL                          | [已完](https://github.com/guochaosheng/FastMiniRaft/tree/master/docs/test/reliability/testcase_jepsen_kill_random_processes.md) |
| 随机 KILL 并清空操作系统缓存       | 待完                                                         |
| 随机集群节点网络分成两半           | [已完](https://github.com/guochaosheng/FastMiniRaft/tree/master/docs/test/reliability/testcase_jepsen_partition_random_halves.md) |
| 随机将单个节点与网络的其余部分隔离 | [已完](https://github.com/guochaosheng/FastMiniRaft/tree/master/docs/test/reliability/testcase_jepsen_partition_random_node.md) |
| 随机 SIGSTOP/SIGCONT               | [已完](https://github.com/guochaosheng/FastMiniRaft/tree/master/docs/test/reliability/testcase_jepsen_hammer_time.md) |

