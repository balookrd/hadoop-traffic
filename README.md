# hdfs-traffic-agent

[English version](README.en.md)

Этот репозиторий считает трафик DataNode HDFS по пользователям и по таблицам Hive. В этом
документе описано, что и как считается: измерения, метрики JMX, поля событий, таблицы
отчётов и известные ограничения, а также сборка и Docker-стенд. Установка на кластер,
настройка и эксплуатация описаны в [руководстве администратора](docs/admin-guide.md).

Три части:

- `agent/`: Java-агент, который загружается в каждую DataNode. Пишет одно JSONL-событие на
  операцию и может публиковать счётчики по пользователям в JMX.
- `block-mapper/`: утилита, которая превращает fsimage в таблицы (блок → файл, файл →
  каталог).
- `spark-jobs/`: две Spark-джобы, которые соединяют события с этими таблицами и пишут в Hive
  поминутный трафик по пользователям и таблицам.

Поддерживаются Apache Hadoop 3.3.x и HDP 3.1 (Hadoop 3.1.1), Java 8. Отчёты работают на
Spark 3.5.1 (Scala 2.12); Spark 2 не поддерживается.

Плейсхолдеры в этом документе:

| Плейсхолдер | Значение |
|---|---|
| `<version>` | версия проекта из `pom.xml`, например `0.1.0-SNAPSHOT` |
| `<dn_host>`, `<dn_http_port>` | хост DataNode и её HTTP-порт (`dfs.datanode.http.address`) |
| `<root>` | каталог отчётов в HDFS, по умолчанию `/data/hdfs-traffic` |
| `<dt>` | день по UTC, `YYYY-MM-DD` |

## Измерения

| Измерение | Значения |
|-----------|----------|
| `user`    | пользователь из block access token; `__unknown__` без токена; `__other__` сверх `maxUsers` |
| `op`      | `read`, `write`, `transfer`, `copy`, `replace`, `scr_fds`, `webhdfs_read`, `webhdfs_create`, `webhdfs_append` |
| `role`    | `client`, `pipeline` (пересылка реплики), `datanode` (ре-репликация, балансировщик, recovery), `webhdfs_proxy` (внутреннее плечо WebHDFS) |
| `locality`| `local`, `remote` |

`__unknown__` означает, что токена нет или в нём нет пользователя. Balancer и Mover
выпускают токены без пользователя, поэтому их трафик `replace` и `copy` идёт в
`__unknown__`.

Счётчики каждой серии:

| Счётчик | Что считает |
|---|---|
| `Bytes` | байты полезной нагрузки, без контрольных сумм и заголовков |
| `EstimatedBytes` | short-circuit чтения: длина блока на каждый запрос дескриптора (DataNode не видит, сколько байт прочитал клиент) |
| `Ops` | завершённые операции |

Счётчики накопительные с момента старта DataNode.

Клиентский трафик — сумма серий `role=client`. `webhdfs_proxy` повторяет HTTP-трафик
`webhdfs_*` на внутреннем плече DataTransferProtocol: исключайте его, иначе байты WebHDFS
посчитаются дважды.

## Сборка

Нужны полный JDK 8 (с `lib/tools.jar`) и Maven.

```bash
export JAVA_HOME=/path/to/jdk8
mvn clean verify -P hadoop-3.1.1   # HDP 3.1
mvn clean verify -P hadoop-3.3.6   # Apache Hadoop 3.3.x (профиль по умолчанию)
```

Соберите один раз, с профилем той версии Hadoop, что стоит на кластере. Профиль задаёт
версию Hadoop, на которой идут тесты (MiniDFSCluster, MiniKDC). Hadoop подключён как
`provided` и в jar не попадает. Jar агента и `spark-jobs` одинаковы для обеих версий;
`block-mapper` нужно собирать под версию кластера (см.
[Сопоставление блоков и файлов](docs/admin-guide.md#сопоставление-блоков-и-файлов-block-mapper)).

Jar для раздачи собирайте с `clean`: без него shade-плагин повторно шейдит уже зашейденный
jar.

Результат:

| Модуль | Jar |
|---|---|
| agent | `agent/target/hdfs-traffic-agent-<version>.jar` |
| block-mapper | `block-mapper/target/block-mapper-<version>-hadoop-<версия hadoop>.jar` |
| spark-jobs | `spark-jobs/target/spark-jobs-<version>.jar` |

## Метрики

JMX-бин `Hadoop:service=DataNode,name=UserTraffic`. Что в нём есть, зависит от опций:

| Опции | Содержимое бина |
|---|---|
| `userMetrics=true` | счётчики по пользователям (ниже) и, если задан `events`, счётчики пишущего потока (см. [Файлы событий](docs/admin-guide.md#файлы-событий)) |
| `userMetrics=false` (по умолчанию), задан `events` | только счётчики пишущего потока: `EventsWritten`, `EventsDropped`, `EventFilesDropped`, `EventsQueueSize` |
| `userMetrics=false`, `events` не задан | бин не регистрируется; агент пишет WARN, что трафик никуда не записывается |

С `userMetrics=false` агент не держит счётчики по пользователям в памяти; трафик по
пользователям есть только в событиях.

Счётчики по пользователям: имена атрибутов имеют вид
`<Counter>.user.<u>.op.<op>.role.<role>.locality.<loc>` (как в nntop), например
`Bytes.user.alice.op.read.role.client.locality.remote`. По HTTP:

```bash
curl -s 'http://<dn_host>:<dn_http_port>/jmx?qry=Hadoop:service=DataNode,name=UserTraffic'
```

Сервлет `/jmx` в Hadoop без ошибки выбрасывает атрибуты, в имени которых есть `=`, `:` или
пробел, поэтому агент заменяет эти символы в именах пользователей на `_`. В имени
пользователя могут быть точки: разбирайте имя справа, как это делает правило jmx_exporter
в руководстве администратора.

Кардинальность счётчиков по пользователям и подключение к Ambari Metrics и Prometheus
описаны в [руководстве администратора](docs/admin-guide.md#метрики-в-системе-мониторинга).

## События (JSONL)

С опцией `events` агент пишет ещё и журнал событий: одна JSON-строка на каждую выдачу, поля
в таком порядке:

| Поле | Значение |
|---|---|
| `ts` | время выдачи, мс от epoch |
| `start` | время начала операции, мс |
| `dn` | имя хоста DataNode, резолвится один раз в пишущем потоке; `unknown`, если не удалось |
| `op_id` | `<6 hex>-<n>`, общий для всех событий одной операции; уникален только в пределах DataNode, поэтому в кластере операцию определяет пара (`dn`, `op_id`) |
| `user` | пользователь, как в метриках, но без ограничения `maxUsers` |
| `op`, `role`, `locality` | те же значения, что в метриках |
| `bpid` | block pool id |
| `block_id` | id блока; `-1`, если блока нет |
| `file_block_id` | id группы блоков для erasure-coded (striped) блоков, иначе id блока |
| `bytes`, `est_bytes` | прирост с предыдущего события той же операции; итог — сумма по `op_id` |
| `dur_ms` | время от начала операции до этого события, мс |
| `last` | `true` у события, закрывающего операцию; считается за одну операцию |
| `peer` | удалённый адрес |
| `path` | только события HTTP-плеча WebHDFS: путь, который запросили у DataNode, например временное имя `._COPYING_` при `hdfs dfs -put` |

Длинная операция выдаёт событие каждые `flushIntervalSec`, поэтому блок, открытый час,
выглядит как серия событий, а не как один пик при закрытии.

Где лежат файлы событий, как они ротируются и отправляются в HDFS, описано в
[руководстве администратора](docs/admin-guide.md#события-локальные-файлы-и-отправка-в-hdfs).

## Отчёты (spark-jobs)

В `spark-jobs` две Spark-джобы. Они превращают отправленные события и результат
block-mapper в поминутный трафик по пользователям и таблицам.

Что делают джобы:

- `CompactEvents` читает `<root>/raw/dt=<dt>/dn=*/*.jsonl` и пишет `<root>/events/dt=<dt>`.
  Битые строки считаются и пропускаются; если битые все строки, джоба падает и не
  перезаписывает партицию. Файлы `*._COPYING_` незаконченного `put` не читаются.
  Отсутствующая сырая партиция роняет джобу.
- `ResolvePaths` соединяет события дня `<dt>` с моделью fsimage и пишет
  `<root>/event_path/dt=<dt>` (события плюс `inode_id` и `table_dir`, с заполненным `path`) и
  `<root>/traffic_minute/dt=<dt>`.
  - Образ — `--image-dt`; по умолчанию последний `fsimage/dt=<dt>`, в `meta/` которого есть
    `.orc`-файл (скрытые временные каталоги пропускаются).
  - Файл, созданный в день `<dt>`, появляется только в checkpoint'е после конца `<dt>`.
    Поэтому ежедневный скрипт передаёт `--image-dt <today>` — образ, скачанный после
    checkpoint'а за полночь. Если дата образа не позже `<dt>`, джоба пишет WARN: файлы,
    созданные в `<dt>`, могут получить `?`.
  - Пути — текущие на момент этого checkpoint'а, с учётом переименований.
  - События WebHDFS сохраняют `path` из события. Блоки, которых нет в образе, получают `?`.

Сборка и запуск джоб, ежедневный отчёт и настройка Spark на HDP описаны в
[руководстве администратора](docs/admin-guide.md#отчёты-запуск-spark-jobs).

Таблицы:

| Таблица | Колонки (все партиционированы по `dt`) |
|---|---|
| `events` | поля событий JSONL (`ts`, `start`, `dn`, `op_id`, `user`, `op`, `role`, `locality`, `bpid`, `block_id`, `file_block_id`, `bytes`, `est_bytes`, `dur_ms`, `last`, `peer`, `path`) |
| `fs_inode`, `fs_edge`, `fs_block_inode`, `fs_meta` | таблицы block-mapper; `dt` — дата образа |
| `event_path` | колонки `events` плюс `inode_id`, `table_dir` |
| `traffic_minute` | `minute` (epoch, мс начала минуты UTC), `user`, `op`, `role`, `locality`, `table_dir`, `bytes`, `est_bytes`, `ops` (число событий `last`) |

Все таблицы — EXTERNAL ORC с ZLIB. DDL лежит в `hive/schema.sql`, сгенерированном из
`org.hadooptraffic.spark.Ddl`; джобы создают таблицы через `CREATE ... IF NOT EXISTS`.
Партиции джобы добавляют через `ALTER TABLE ... ADD IF NOT EXISTS PARTITION`.

Джобы читают JSONL и ORC напрямую; таблицы Hive над сырыми файлами нет.

### Привязка к таблице (`table_dir`)

`table_dir` вычисляется по пути, без Hive metastore:

1. Отбросить имя файла.
2. Обрезать каталог на первом компоненте одного из видов:
   - партиция: `key=value`;
   - служебный: начинается с `.` или `_` (`.hive-staging_*`, `_temporary`);
   - Hive ACID: `base_N`, `delta_N_N`, `delete_delta_N_N`, у каждого может быть суффикс
     `_v<n>`.

Следствия:

- Файлы, записанные в `.hive-staging_*` и затем перенесённые в таблицу, относятся к таблице:
  в образе у них уже конечный путь.
- Файлы, удалённые до следующего checkpoint'а NameNode (scratch-каталоги Hive, staging-jar'ы
  джоб, временные выходы), в образ не попадают и получают `?`.
- У непартиционированной таблицы с подкаталогами `table_dir` — подкаталог, на уровень ниже
  таблицы.

### Пример запроса

Топ-20 пар (минута, таблица) по скорости клиентской записи за час, МБ/с:

```sql
SELECT table_dir, SUM(bytes) / 1048576 / 60 AS mb_per_s, minute
FROM hdfs_traffic.traffic_minute
WHERE dt = '2026-11-06'
  AND op IN ('write', 'webhdfs_create', 'webhdfs_append') AND role = 'client'
  AND minute >= 1793923200000 AND minute < 1793926800000   -- 2026-11-06 00:00-01:00 UTC
GROUP BY minute, table_dir
ORDER BY mb_per_s DESC
LIMIT 20;
```

`user` и `start` — зарезервированные слова в Hive 3: в beeline берите их в обратные кавычки.

Как одна операция проходит от JSONL до `traffic_minute` и как посчитать скорость операции по
`dur_ms`, показано на примере данных в [docs/data-examples.md](docs/data-examples.md).

## Docker-стенд

`stand/` запускает агент на защищённом кластере в Docker: MIT KDC, NameNode, три DataNode с
`-javaagent` и клиентский контейнер. Kerberos, block access tokens и SASL-защита передачи
данных включены. Два дистрибутива:

- HDP 3.1.0.0-78 из локальной копии (его jar'ы нельзя распространять);
- Apache Hadoop 3.3.6, копируется из образа `apache/hadoop:3.3.6`.

Что нужно: Docker с Compose, JDK 8, для HDP — локальная копия дистрибутива в
`HDP_DIST_DIR` (по умолчанию `~/hdp/3.1.0.0-78`).

```bash
mvn clean package -P hadoop-3.1.1      # стенд монтирует agent/target/*.jar
cd stand
./prepare.sh hdp
docker compose --env-file .env.hdp up -d --build
./run-scenario.sh hdp
docker compose --env-file .env.hdp down -v
```

Для Apache Hadoop используйте `apache` и `.env.apache`.

Стенд по умолчанию запускает агент с `userMetrics=true`, потому что сверяет счётчики
`/jmx`; при своём `AGENT_ARGS` оставьте эту опцию.

`run-scenario.sh` гоняет известный трафик, один пользователь на шаг: клиентская запись с
репликацией 3, ре-репликация после `setrep`, удалённое и локальное чтение, запись и чтение
через WebHDFS. Затем проверяет:

- все 8 перехватываемых классов инструментированы;
- прирост `UserTraffic`, просуммированный по `/jmx` всех DataNode, равен ожидаемому числу
  байт;
- события, отправленные через `upload-events.sh`, дают те же байты и ожидаемые пути WebHDFS.

Порты на хосте: `/jmx` DataNode — 29864–29866, UI NameNode (SPNEGO) — 29870.

## Известные проблемы агента

- Байты записи отчитываются каждые `flushIntervalSec`, пока блок открыт, остаток — при
  закрытии. Байты, не отчитанные к перезапуску DataNode, теряются.
- Точные байты short-circuit чтения не видны. `EstimatedBytes` считает длину блока один раз
  на запрос дескриптора; последующие чтения из кэша дескрипторов клиента не считаются.
  Чтения WebHDFS локальных реплик могут идти через short-circuit внутри DataNode и тогда
  видны как `op=scr_fds, role=client`, а не `webhdfs_proxy`.
- Позиционные чтения, не выровненные по чанкам контрольных сумм, могут посчитать до одного
  чанка (по умолчанию 512 Б) больше запрошенного.
- Байты пакета записи, упавшего посреди приёма, не считаются.
- Поток WebHDFS, который так и не закрыли, теряет своё событие. Чтения WebHDFS отчитываются
  о байтах при закрытии потока, без промежуточных приростов.
- Поздний `addBytes`, гонящийся с `finish()` в другом потоке, может потерять эти байты — и в
  JMX, и в событиях.
- Locality в DataTransferProtocol сравнивает адреса самого сокета (клиент подключился к тому
  же IP, с которого пришёл). Локальный клиент с другого IP multi-homed хоста считается
  `remote`.
- В событиях только id блоков; пути файлов берутся из сопоставления по fsimage
  (block-mapper, см. `docs/superpowers/specs/2026-11-06-block-to-file-mapping-design.md`).
