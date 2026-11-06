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

Результат — `agent/target/hdfs-traffic-agent-<version>.jar`, один jar для обеих версий Hadoop.

## Установка

1. Скопировать jar на каждую DataNode, например в
   `/opt/hdfs-traffic-agent/hdfs-traffic-agent.jar`.
2. Добавить в `hadoop-env.sh` (HDP: Ambari → HDFS → Configs → Advanced hadoop-env, шаблон):

   ```bash
   export HDFS_DATANODE_OPTS="$HDFS_DATANODE_OPTS -javaagent:/opt/hdfs-traffic-agent/hdfs-traffic-agent.jar=maxUsers=2000"
   ```

   Старое имя `HADOOP_DATANODE_OPTS` Hadoop 3 тоже понимает.

   Опции агента (через запятую, `key=value`; неизвестная или неверная опция отключает агент
   с WARN в логе, сама DataNode запускается):

   | Опция | По умолчанию | Смысл |
   |---|---|---|
   | `maxUsers` | 2000 | слоты пользователей в JMX по принципу «кто первый»; остальные — `__other__` |
   | `events` | выкл. | активный JSONL-файл событий, например `/var/log/hadoop/hdfs-traffic/events.jsonl` (без запятых) |
   | `eventsQueue` | 65536 | буфер событий для потока записи; переполнение отбрасывается со счётчиком |
   | `eventsMaxFileMb` | 256 | ротация активного файла по размеру |
   | `eventsRotateSec` | 300 | ротация активного файла по возрасту |
   | `eventsMaxFiles` | 100 | сколько закрытых неотправленных файлов хранить; старейшие сверх лимита удаляются |
   | `flushIntervalSec` | 10 | долгие операции отчитываются о байтах каждые N секунд (JMX и события); 0 — только в конце |

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

## События (JSONL)

С опцией `events` агент дополнительно пишет журнал событий по операциям: одна JSON-строка
на каждую эмиссию, поля идут в таком порядке:

| Поле | Смысл |
|---|---|
| `ts` | время эмиссии, мс с начала эпохи |
| `start` | время начала операции, мс |
| `dn` | имя хоста DataNode (определяется один раз в потоке записи; `unknown`, если определить не удалось) |
| `op_id` | `<6 hex>-<n>`, общий для всех событий одной операции; уникален только в пределах одного DataNode, поэтому операция в кластере определяется парой (`dn`, `op_id`) |
| `user` | пользователь, как в метриках, но без ограничения `maxUsers` |
| `op`, `role`, `locality` | те же значения, что в метриках |
| `bpid` | id block pool |
| `block_id` | id блока; `-1`, если неприменимо |
| `file_block_id` | id группы блоков для erasure-coded (striped) блоков, иначе id блока |
| `bytes`, `est_bytes` | прирост с предыдущего события той же операции: чтобы получить итог, просуммируйте по `op_id` |
| `dur_ms` | время от начала операции до этого события, мс |
| `last` | `true` закрывает операцию и считается одной op |
| `peer` | адрес удалённой стороны |
| `path` | только для событий HTTP-плеча WebHDFS: путь, который запросили у DataNode (например, временное имя `._COPYING_` при `hdfs dfs -put`) |

Долгие операции выдают событие каждые `flushIntervalSec`, поэтому долго открытый блок
виден серией событий, а не одним скачком при закрытии.

Файлы: активный файл — `events.jsonl` (имя из опции `events`). По размеру или возрасту он
ротируется в `events-<yyyyMMdd-HHmmss>-<seq>.jsonl`; имя задаёт UTC-время первого события.
Дата в имени — UTC-дата событий файла (`ts`): файл никогда не пересекает границу UTC-суток
(события, переставленные на несколько миллисекунд через полночь, остаются в более раннем
файле). Непустой активный файл, оставшийся после сбоя, ротируется при старте агента и
датируется по первому событию в нём. Лимит `eventsMaxFiles` считает и удаляет только файлы
с именами в точности такого вида. Пользователь DataNode должен иметь право создать каталог
событий (он создаётся при первой записи и при сбое создаётся повторно).

DataNode никогда не блокируется на приёмнике: при переполненной очереди или ошибке
ввода-вывода события отбрасываются и учитываются, WARN — не чаще раза в минуту.
Атрибуты состояния в бине `UserTraffic`: `EventsWritten`, `EventsDropped`,
`EventFilesDropped` (счётчики) и `EventsQueueSize` (gauge).

Отправка: `deploy/upload-events.sh -d <каталог событий> -r <корень в HDFS> [-n <префикс файлов>]
[-k <keytab> -p <principal>] [-H <host>]` кладёт закрытые файлы в HDFS по пути
`<root>/dt=YYYY-MM-DD/dn=<host>/<файл>` и удаляет локальный файл только после успешного
put. Скрипт берёт `flock`, поэтому запуски не накладываются, и использует собственный
кэш Kerberos-билетов в каталоге событий. Партиция `dn=` — ключ узла: по умолчанию
`hostname -f`, `-H` переопределяет его; он может отличаться от поля `dn` внутри событий.
Сбой одного файла не останавливает запуск: остальные всё равно отправляются, код выхода — 1.

Скопируйте `deploy/upload-events.sh` на DataNode (например, рядом с jar агента,
`/opt/hdfs-traffic-agent/`) и запускайте из cron, например:

```
*/5 * * * * hdfs /opt/hdfs-traffic-agent/upload-events.sh -d /var/log/hadoop/hdfs-traffic \
    -r /data/hdfs-traffic/raw -k /etc/security/keytabs/hdfs.headless.keytab -p hdfs@REALM
```

Собственные записи загрузки в HDFS видны как трафик сервисного пользователя.

## Docker-стенд

`stand/` запускает агент на настоящем защищённом кластере: MIT KDC, NameNode, три
DataNode с `-javaagent` и клиентский контейнер. Как в продакшене, включены Kerberos,
block access token и SASL-защита передачи данных. Поддерживаются два дистрибутива:
HDP 3.1.0.0-78 (из локальной копии: эти jar нельзя распространять) и Apache Hadoop 3.3.6
(копируется из образа `apache/hadoop:3.3.6`).

```bash
mvn clean package -P hadoop-3.1.1      # JDK 8; стенд монтирует agent/target/*.jar
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
байтами. Также отправляет события через `upload-events.sh` и сверяет их с теми же
ожидаемыми байтами и путями WebHDFS. `/jmx` DataNode выведен на порты хоста 29864–29866,
веб-интерфейс NameNode (SPNEGO) — на 29870.

## Ограничения

- Байты записи отражаются каждые `flushIntervalSec`, пока блок открыт, остаток — при
  закрытии; байты, ещё не отражённые к моменту рестарта DataNode, теряются.
- Точный объём short-circuit чтений не виден (см. `EstimatedBytes`: длина блока один раз
  на запрос дескриптора; последующие чтения через кэш дескрипторов на стороне клиента не
  учитываются). Чтения WebHDFS локальных реплик могут идти через short-circuit внутри
  DataNode и тогда отражаются как `op=scr_fds, role=client`, а не `webhdfs_proxy`.
- Чтения: позиционные чтения, не выровненные по chunk'ам checksum, могут быть
  учтены на величину до одного chunk'а (по умолчанию 512 Б) больше запрошенного.
- Байты пакета записи, который оборвался посреди приёма, не учитываются.
- Поток WebHDFS, который так и не закрыт, теряет своё событие. Чтения WebHDFS отражают
  свои байты при закрытии потока (промежуточных приростов нет).
- Гонка между поздним `addBytes` и `finish()` в другом потоке может потерять эти байты —
  и в JMX, и в событиях одинаково.
- Locality в DataTransferProtocol определяется по собственным адресам сокета (клиент
  подключился к тому же IP, с которого пришёл); локальный клиент с другим локальным IP
  многоинтерфейсного хоста считается `remote`.
- События содержат только id блоков; пути файлов даёт сопоставление по fsimage
  (block-mapper, см. `docs/superpowers/specs/2026-11-06-block-to-file-mapping-design.md`).
