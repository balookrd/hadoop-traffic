# hdfs-traffic-agent

Java-агент для DataNode HDFS: считает трафик в разрезе пользователей и публикует
счётчики через Hadoop metrics2 / JMX. Поддерживаются Apache Hadoop 3.3.x и HDP 3.1
(Hadoop 3.1.1), Java 8.

## Что считается

| Разрез    | Значения |
|-----------|----------|
| `user`    | пользователь из block access token; `__unknown__`, если токена нет; `__other__` сверх лимита `maxUsers` |
| `op`      | `read`, `write`, `transfer`, `copy`, `replace`, `scr_fds`, `webhdfs_read`, `webhdfs_create`, `webhdfs_append` |
| `role`    | `client`, `pipeline` (пересылка реплик), `datanode` (перерепликация, балансировщик, восстановление), `webhdfs_proxy` (внутреннее плечо WebHDFS) |
| `locality`| `local`, `remote` |

`__unknown__` — нет block token либо токен без пользователя: Balancer и Mover создают
токены без пользователя, поэтому их трафик `replace`/`copy` виден как `__unknown__`.

Счётчики каждого ряда: `Bytes` (полезные данные, без checksum и заголовков),
`EstimatedBytes` (short-circuit чтения: длина блока на каждый запрос дескриптора —
сами байты DataNode не видит), `Ops`. Значения нарастают с момента старта DataNode.

Клиентский трафик — все ряды с `role=client`. Роль `webhdfs_proxy` дублирует
HTTP-трафик `webhdfs_*` на внутреннем плече DataTransferProtocol; исключайте её,
чтобы не считать дважды.

## Сборка

```bash
export JAVA_HOME=/path/to/jdk8   # полный JDK 8 (с lib/tools.jar)
mvn clean verify -P hadoop-3.3.6
mvn clean verify -P hadoop-3.1.1
```

Для jar, пригодного к раздаче, собирайте через `mvn clean verify` (или
`mvn clean package`): сборка без `clean` повторно шейдит уже зашейденный jar.

Результат — `target/hdfs-traffic-agent-<version>.jar`, один jar для обеих версий Hadoop.

## Установка

1. Скопировать jar на каждую DataNode, например в
   `/opt/hdfs-traffic-agent/hdfs-traffic-agent.jar`.
2. Добавить в `hadoop-env.sh` (HDP: Ambari → HDFS → Configs → Advanced hadoop-env, шаблон):

   ```bash
   export HDFS_DATANODE_OPTS="$HDFS_DATANODE_OPTS -javaagent:/opt/hdfs-traffic-agent/hdfs-traffic-agent.jar=maxUsers=2000"
   ```

   Старое имя `HADOOP_DATANODE_OPTS` Hadoop 3 тоже понимает.

   Опции агента: единственная опция — `maxUsers` (по умолчанию 2000, должна быть ≥ 1).
   Слоты пользователей выдаются по принципу «кто первый» с момента старта DataNode;
   остальные пользователи попадают в `__other__`. Неизвестная или неверная опция
   отключает агент (WARN в логе DataNode), но сама DataNode запускается.
3. Перезапустить DataNode. В логе появятся строки `hdfs-traffic-agent installed` и
   `hdfs-traffic-agent: instrumented ...`. Строка `WARN ... hook is disabled` значит, что
   в этой сборке Hadoop не найден метод; остальные перехваты продолжают работать.

Нужен `dfs.block.access.token.enable=true` (с Kerberos он включён всегда); без него все
пользователи будут `__unknown__`.

## Метрики

JMX-бин `Hadoop:service=DataNode,name=UserTraffic`, атрибуты вида
`<Counter>.user.<u>.op.<op>.role.<role>.locality.<loc>` (как в nntop), например
`Bytes.user.alice.op.read.role.client.locality.remote`. Те же данные доступны по
`http://<dn>:<http-port>/jmx?qry=Hadoop:service=DataNode,name=UserTraffic`.
Сервлет `/jmx` в Hadoop молча пропускает атрибуты, в имени которых есть `=`, `:` или
пробел, поэтому такие символы в имени пользователя заменяются на `_`. В имени
пользователя могут быть точки: разбирайте имя справа, как в правиле ниже.

Кардинальность: на каждого пользователя приходится до одного ряда на каждую реально
встреченную комбинацию (op, role, locality) — обычно 8–10 — умноженную на 3 счётчика;
при `maxUsers=2000` это до ~50–60 тыс. атрибутов на одну DataNode. Источник уходит во
все sink'и metrics2, а не только в JMX. В HDP sink Ambari Metrics
(`datanode.sink.timeline`) включён по умолчанию; чтобы UserTraffic в него не попадал,
добавьте в `hadoop-metrics2.properties`:

```
datanode.sink.timeline.source.filter.class=org.apache.hadoop.metrics2.filter.GlobFilter
datanode.sink.timeline.source.filter.exclude=UserTraffic
```

При необходимости уменьшите `maxUsers`.

Правило для Prometheus jmx_exporter (с `lowercaseOutputName` имена получаются вида
`hdfs_datanode_user_traffic_bytes_total`):

```yaml
lowercaseOutputName: true
rules:
  - pattern: 'Hadoop<service=DataNode, name=UserTraffic><>(Bytes|EstimatedBytes|Ops)\.user\.(.+)\.op\.(\w+)\.role\.(\w+)\.locality\.(\w+)'
    name: hdfs_datanode_user_traffic_$1_total
    type: COUNTER
    labels:
      user: "$2"
      op: "$3"
      role: "$4"
      locality: "$5"
```

## Docker-стенд

`stand/` запускает агент на настоящем защищённом кластере: MIT KDC, NameNode, три
DataNode с `-javaagent` и клиентский контейнер. Как в продакшене, включены Kerberos,
block access token и SASL-защита передачи данных. Поддерживаются два дистрибутива:
HDP 3.1.0.0-78 (из локальной копии: эти jar нельзя распространять) и Apache Hadoop 3.3.6
(копируется из образа `apache/hadoop:3.3.6`).

```bash
mvn clean package -P hadoop-3.1.1      # JDK 8; стенд монтирует target/*.jar
cd stand
./prepare.sh hdp                        # HDP_DIST_DIR, по умолчанию ~/hdp/3.1.0.0-78
docker compose --env-file .env.hdp up -d --build
./run-scenario.sh hdp
docker compose --env-file .env.hdp down -v
```

Для Apache вместо этого — `apache` и `.env.apache`. `run-scenario.sh` прогоняет
известный трафик, на каждом шаге свой пользователь: запись клиентом с репликацией 3,
перерепликацию после `setrep`, удалённое и локальное чтение, запись и чтение через
WebHDFS. Затем проверяет, что все 8 перехватываемых классов инструментированы, и сверяет
прирост `UserTraffic`, просуммированный по `/jmx` всех DataNode, с точными ожидаемыми
байтами. `/jmx` DataNode выведен на порты хоста 29864–29866, веб-интерфейс NameNode
(SPNEGO) — на 29870.

## Ограничения

- Байты записи блока учитываются при завершении операции записи (блок закрыт), поэтому
  долго открытые блоки (WAL HBase, потоковые писатели) появляются скачком при закрытии;
  байты, ещё не учтённые на момент рестарта DataNode, теряются.
- Точный объём short-circuit чтений не виден (см. `EstimatedBytes`: длина блока один раз
  на запрос дескриптора; последующие чтения через кэш дескрипторов на стороне клиента не
  учитываются). Чтения WebHDFS локальных реплик могут идти через short-circuit внутри
  DataNode и тогда отражаются как `op=scr_fds, role=client`, а не `webhdfs_proxy`.
- Чтения: позиционные чтения, не выровненные по chunk'ам checksum, могут быть
  учтены на величину до одного chunk'а (по умолчанию 512 Б) больше запрошенного.
- Байты пакета записи, который оборвался посреди приёма, не учитываются.
- Поток WebHDFS, который так и не закрыт, теряет своё событие.
- Locality в DataTransferProtocol определяется по собственным адресам сокета (клиент
  подключился к тому же IP, с которого пришёл); локальный клиент с другим локальным IP
  многоинтерфейсного хоста считается `remote`.
- Сопоставление блоков с файлами пока не реализовано (события содержат block pool id и
  block id).
