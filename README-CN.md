CeresDB 是一款高性能、分布式的云原生时序数据库。

## RoadMap
项目 [Roadmap](https://docs.ceresdb.io/dev/roadmap.html)。

## 快速开始

### 通过 Docker 运行
确保开发环境安装了 docker，通过仓库中的提供的 Dockerfile 进行镜像的构建：
```shell
docker build -t ceresdb .
```

使用编译好的镜像，启动服务：
```shell
docker run -d -t --name ceresdb -p 5440:5440 -p 8831:8831 ceresdb
```

### 进行数据读写
CeresDB 支持自定义扩展的 SQL 协议，目前可以通过 http 服务以 SQL 语句进行数据的读写、表的创建。
#### 建表
```shell
curl --location --request POST 'http://127.0.0.1:5440/sql' \
--header 'Content-Type: application/json' \
--data-raw '{
    "query": "CREATE TABLE `demo` (`name` string TAG, `value` double NOT NULL, `t` timestamp NOT NULL, TIMESTAMP KEY(t)) ENGINE=Analytic with (enable_ttl='\''false'\'')"
}'
```

#### 插入数据
```shell
curl --location --request POST 'http://127.0.0.1:5440/sql' \
--header 'Content-Type: application/json' \
--data-raw '{
    "query": "INSERT INTO demo(t, name, value) VALUES(1651737067000, '\''ceresdb'\'', 100)"
}'
```

#### 查询数据
```shell
curl --location --request POST 'http://127.0.0.1:5440/sql' \
--header 'Content-Type: application/json' \
--data-raw '{
    "query": "select * from demo"
}'
```

#### 查看建表信息
```shell
curl --location --request POST 'http://127.0.0.1:5440/sql' \
--header 'Content-Type: application/json' \
--data-raw '{
    "query": "show create table demo"
}'
```

#### 删除表
```shell
curl --location --request POST 'http://127.0.0.1:5440/sql' \
--header 'Content-Type: application/json' \
--data-raw '{
    "query": "DROP TABLE demo"
}'
```

## 平台支持

|          target          |         OS        |         status        |
|:------------------------:|:-----------------:|:---------------------:|
| x86_64-unknown-linux-gnu |    kernel 4.9+    |       构建及运行        |
|    x86_64-apple-darwin   | 10.15+, Catalina+ |          构建          |
|    aarch64-apple-darwin  |   11+, Big Sur+   |          构建          |
| aarch64-unknown-linux-gnu|        TBD        | 详见 [#63](https://github.com/CeresDB/ceresdb/issues/63)|
|         windows          |         *         |         未支持         |

## 如何贡献
[如何参与 CeresDB 代码贡献](CONTRIBUTING.md)

[约定式提交](https://docs.ceresdb.io/cn/dev/conventional_commit)

## 架构及技术文档
相关技术文档请参考[docs](https://docs.ceresdb.io/)。

## 致谢
CeresDB 部分设计参考 [influxdb_iox](https://github.com/influxdata/influxdb_iox), 部分代码实现参考 [tikv](https://github.com/tikv/tikv) 以及其他的优秀开源项目，感谢 InfluxDB 团队、TiKV 团队，以及其他优秀的开源项目。

## 社区
- [CeresDB 社区角色](docs/community/ROLES-CN.md)
- [Slack](https://join.slack.com/t/ceresdbcommunity/shared_invite/zt-1dcbv8yq8-Fv8aVUb6ODTL7kxbzs9fnA)
- 邮箱: ceresdbservice@gmail.com
- [官方微信公众号](https://github.com/CeresDB/community/blob/main/communication/wechat/official-account-qrcode.jpg)
- 钉钉群: CeresDB 开源: 44602802
