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
[-k <keytab> -p <principal>] [-H <host>] [-b <файлов на put>] [-j <макс. задержка, с>]` кладёт
закрытые файлы в HDFS по пути `<root>/dt=YYYY-MM-DD/dn=<host>/<файл>` и удаляет локальные
файлы только после успешного put. Файлы уходят пачками: один `hdfs dfs -mkdir` на все
целевые каталоги и один `hdfs dfs -put` на каждый целевой каталог и пачку из `-b` файлов
(по умолчанию 100). Поэтому за запуск стартуют две-три JVM, а не по одной на файл; если
закрытых файлов нет, скрипт ничего не делает (ни `kinit`, ни JVM). Скрипт берёт `flock`,
поэтому запуски не накладываются, и использует собственный кэш Kerberos-билетов в каталоге
событий. Партиция `dn=` — ключ узла: по умолчанию `hostname -f`, `-H` переопределяет его;
он может отличаться от поля `dn` внутри событий. Сбой одной пачки не останавливает запуск:
остальные пачки всё равно отправляются, файлы из неудачной остаются до следующего запуска
(повторный put с `-f` безопасен: закрытые файлы не меняются), код выхода — 1. `-j <с>`
перед стартом ждёт случайное время от 0 до заданного числа секунд.

### Запуск через systemd timer

В `deploy/systemd/` лежат oneshot-сервис и таймер, который запускает его каждые 5 минут со
случайной задержкой до 4 минут (`RandomizedDelaySec=240`): DataNode кластера не обращаются к
NameNode и KDC в одну и ту же секунду. Пока предыдущий запуск не закончился, таймер новый не
стартует. Сервис работает от `hdfs` с пониженным приоритетом CPU и ввода-вывода, heap
клиента 256 МБ и таймаутом 30 минут.

На каждой DataNode, от root:

```bash
install -d /opt/hdfs-traffic-agent
install -m 0755 deploy/upload-events.sh /opt/hdfs-traffic-agent/
install -m 0644 deploy/systemd/hdfs-traffic-upload.service \
    deploy/systemd/hdfs-traffic-upload.timer /etc/systemd/system/
install -m 0644 deploy/systemd/hdfs-traffic-upload.env /etc/default/hdfs-traffic-upload
# поправьте /etc/default/hdfs-traffic-upload: каталог событий, корень в HDFS, keytab и principal
systemctl daemon-reload
systemctl enable --now hdfs-traffic-upload.timer
```

Опции скрипта задаются в `UPLOAD_ARGS` в `/etc/default/hdfs-traffic-upload`; там же можно
задать `JAVA_HOME` и `HADOOP_CLIENT_OPTS`. Unit ищет `hdfs` в
`/usr/hdp/current/hadoop-client/bin`, `/opt/hadoop/bin` и системном `PATH`; если он лежит в
другом месте, переопределите `PATH` через `systemctl edit hdfs-traffic-upload.service`.

`RandomizedDelaySec` работает начиная с systemd 229. Более старый systemd (в RHEL/CentOS 7 —
219) пропускает его с предупреждением в журнале, и все ноды стартуют одновременно: там
добавьте `-j 240` в `UPLOAD_ARGS`.

Проверка:

```bash
systemctl list-timers hdfs-traffic-upload.timer   # следующий запуск
systemctl start hdfs-traffic-upload.service       # запустить сейчас
journalctl -u hdfs-traffic-upload.service         # что отправлено, ошибки
```

После неудачного запуска сервис остаётся в состоянии `failed` до следующего успешного,
поэтому его можно мониторить как любой другой unit.

Без systemd подойдёт и cron (добавьте `-j`, чтобы ноды не стартовали одновременно):

```
*/5 * * * * hdfs /opt/hdfs-traffic-agent/upload-events.sh -j 240 -d /var/log/hadoop/hdfs-traffic \
    -r /data/hdfs-traffic/raw -k /etc/security/keytabs/hdfs.headless.keytab -p hdfs@REALM
```

Собственные записи загрузки в HDFS видны как трафик сервисного пользователя.

## Сопоставление блоков и файлов (block-mapper)

События содержат только id блоков. `block-mapper/` превращает скачанный fsimage в модель
inode (блок → inode, inode → родитель и имя), по которой Spark-задачи сопоставляют
`file_block_id` события с текущим путём файла (само сопоставление делают Spark-задачи,
см. «Отчёты»).

Сборка, отдельно для каждой версии Hadoop:

```bash
mvn clean verify -P hadoop-3.1.1      # HDP
mvn clean verify -P hadoop-3.3.6      # Apache Hadoop 3.3.x
```

Результат — `block-mapper/target/block-mapper-<version>-hadoop-<версия hadoop>.jar`.
Используйте jar, собранный под версию Hadoop вашего кластера: сгенерированные
protobuf-классы в 3.1 и 3.3 различаются. В jar упакованы `orc-core` 1.5.13,
`orc-shims`, `hive-storage-api`, `aircompressor`, `commons-lang` 2.6 и `threeten-extra`, все
перенесены (relocate) в `org.hadooptraffic.mapper.shaded`, чтобы jar-файлы Hive из
`HADOOP_CLASSPATH` их не подменяли; `protobuf-java` 2.5.0 берётся из Hadoop. Hadoop 3.4+
(где protobuf 2.5 необязателен) не поддерживается.

Запускайте на скачанном образе, а не на собственном хранилище NameNode. `-i` — локальный
файл (скачанный образ), `-o` — любой URI файловой системы Hadoop:

```bash
hdfs dfsadmin -fetchImage /data/fsimg      # от администратора кластера HDFS
hadoop jar block-mapper-0.1.0-SNAPSHOT-hadoop-3.1.1.jar org.hadooptraffic.mapper.FsImageMapper \
  -i /data/fsimg/fsimage_<txid> -o hdfs:///data/hdfs-traffic/fsimage/dt=<YYYY-MM-DD> [--format tsv]
```

`fetchImage` отдаёт последний checkpoint NameNode, а не живое пространство имён: файлов,
созданных или переименованных после него, в образе нет (как ежедневный скрипт ждёт свежий
checkpoint, см. «Отчёты»). Никогда не вызывайте `saveNamespace` на боевом NameNode, чтобы
получить образ новее.

Результат — по одному файлу на таблицу, `<out>/<table>/part-00000.orc`:

| Таблица | Колонки |
|---|---|
| `inode` | `inode_id`, `type` (`F` файл, `D` каталог, `S` symlink), `name` |
| `edge` | `child_id`, `parent_id` |
| `block_inode` | `block_id`, `inode_id` |
| `meta` | `txid`, `image_file`, `parsed_at` (epoch в миллисекундах) |

По умолчанию ORC: версия файла 0.12, ZLIB, bloom-фильтры на `inode_id`, `child_id` и
`block_id`; читается Hive 3.1 и Spark 3.5. `--format tsv` пишет `part-00000.tsv` с теми же
колонками через табуляцию; `\`, табуляция, перевод строки и возврат каретки в именах
экранируются как `\\`, `\t`, `\n`, `\r`.

Mapper пишет в скрытый соседний каталог `<parent>/.<name>._tmp` (например
`fsimage/.dt=2026-11-06._tmp`, Spark и Hive не примут его за партицию) и при успехе
переименовывает его в `<out>`; существующий `<out>` никогда не перезаписывается. Код выхода
0 — успех, 1 — сбой, 2 — ошибка в аргументах; сообщения начинаются с `block-mapper:`. Память:
пространство имён читается потоком; в памяти хранятся ссылки снапшотов и буферы записи ORC
(их ограничивает memory manager ORC).

Ограничения:

- Inode, видимые только в снапшотах (удалённые после снапшота), не имеют родителя и
  остаются неразрешёнными.
- Сжатый образ, сохранённый параллельно (`dfs.image.parallel.save=true` вместе с
  `dfs.image.compress=true`, Hadoop 3.3), отклоняется: скачайте образ, сохранённый без
  одной из этих опций.

## Отчёты (spark-jobs)

`spark-jobs/` — две Spark-задачи, которые превращают доставленные события и результат
block-mapper в трафик по минутам, пользователям и таблицам. Сборка (Spark 3.5.1,
Scala 2.12; Spark 2 не поддерживается):

```bash
mvn clean verify
```

Результат — `spark-jobs/target/spark-jobs-<version>.jar`, один для обеих версий Hadoop.
На HDP 3.1 запускайте его отдельным дистрибутивом `spark-3.5.1-bin-hadoop3`, а не
Spark 2 кластера.

Обе задачи принимают `--dt YYYY-MM-DD` (обязательно), `--root <URI каталога>` (по умолчанию
`/data/hdfs-traffic`), `--db <имя>` (по умолчанию `hdfs_traffic`) и `--catalog hive|none`
(по умолчанию `hive`); `ResolvePaths` также принимает `--image-dt YYYY-MM-DD`.

- `CompactEvents` читает `<root>/raw/dt=D/dn=*/*.jsonl` и пишет `<root>/events/dt=D`.
  Битые строки считаются и пропускаются; файлы `*._COPYING_` незавершённого `put` не
  читаются. Если партиции raw нет, задача падает.
- `ResolvePaths` соединяет события дня `D` с моделью fsimage и пишет
  `<root>/event_path/dt=D` (события плюс `inode_id`, `table_dir`; `path` заполнен) и
  `<root>/traffic_minute/dt=D`. Образ — `--image-dt`, по умолчанию последний
  `fsimage/dt=YYYY-MM-DD`, в `meta/` которого есть файл `.orc` (скрытые временные
  каталоги игнорируются). Файл, созданный в день `D`, попадает только в checkpoint,
  снятый после окончания `D`, поэтому ежедневный скрипт передаёт образ сегодняшнего дня
  (`--image-dt <сегодня>`), скачанный после послеполуночного checkpoint. Пути — текущие на
  момент этого checkpoint, переименования уже учтены. События WebHDFS сохраняют `path` из
  самого события; блоки, которых нет в образе, получают `?`. Если дата выбранного образа не
  позже `D`, задача пишет в лог WARN: файлы, созданные в день `D`, могут получить `?`.

Каждый запуск перезаписывает только свои партиции, поэтому упавший запуск просто
повторяют.

| Таблица | Колонки (все партиционированы по `dt`) |
|---|---|
| `events` | поля события из JSONL (`ts`, `start`, `dn`, `op_id`, `user`, `op`, `role`, `locality`, `bpid`, `block_id`, `file_block_id`, `bytes`, `est_bytes`, `dur_ms`, `last`, `peer`, `path`) |
| `fs_inode`, `fs_edge`, `fs_block_inode`, `fs_meta` | таблицы block-mapper; `dt` — дата образа |
| `event_path` | колонки `events` плюс `inode_id`, `table_dir` |
| `traffic_minute` | `minute` (epoch в мс начала минуты UTC), `user`, `op`, `role`, `locality`, `table_dir`, `bytes`, `est_bytes`, `ops` (число событий `last`) |

Все таблицы — EXTERNAL ORC с ZLIB (`hive/schema.sql` генерируется из
`org.hadooptraffic.spark.Ddl`; задачи создают их через `CREATE ... IF NOT EXISTS`).
Партиции задачи добавляют явно через `ALTER TABLE ... ADD IF NOT EXISTS PARTITION`;
`MSCK REPAIR TABLE` запускать нельзя: четыре таблицы `fs_*` лежат в одном каталоге образа
и имеют разные схемы. Задачи читают файлы JSONL и ORC напрямую, поэтому Hive-таблицы над
сырыми файлами нет.

С `--catalog none` задачи только пишут файлы и ничего не регистрируют.

### Spark 3.5.1 на HDP 3.1 (YARN, Kerberos, Hive metastore 3.1)

Положите в `$SPARK_HOME/conf/spark-defaults.conf` (а не в разовую командную строку):

```
spark.sql.hive.metastore.version   3.1.3
spark.sql.hive.metastore.jars      path
spark.sql.hive.metastore.jars.path file:///usr/hdp/current/hive-client/lib/*
spark.hadoop.metastore.catalog.default hive
spark.yarn.populateHadoopClasspath false
```

Кроме того:

- Скопируйте `hive-site.xml` из `/etc/hive/conf` в `$SPARK_HOME/conf`. Не берите его из
  `/etc/spark2/conf`: там `metastore.catalog.default=spark`, и таблицы не видны из Hive.
- В `$SPARK_HOME/conf/spark-env.sh` добавьте
  `export HADOOP_CONF_DIR=/etc/hadoop/conf YARN_CONF_DIR=/etc/hadoop/conf` (у cron нет
  окружения).
- По желанию задайте `spark.yarn.archive` с заранее загруженным архивом jar-файлов Spark,
  чтобы не отправлять их при каждом запуске.

Путь YARN / Kerberos / metastore стенд не проверяет (на нём Spark работает с
`--master local` и `--catalog none`). При первом развёртывании проверьте:

1. Один запуск с `--catalog hive` за недавний день проходит.
2. `SHOW PARTITIONS hdfs_traffic.traffic_minute;` в beeline показывает этот день.
3. `SELECT * FROM hdfs_traffic.traffic_minute LIMIT 10;` в beeline возвращает строки
   (Hive читает ORC, записанный Spark).
4. `ADD PARTITION` в таблицы `fs_*` работает для принципала Spark (после запуска
   `SHOW PARTITIONS` для `fs_*` показывает новый `dt`).
5. Возможное исправление: если клиент Spark падает на timeline service YARN, добавьте
   `spark.hadoop.yarn.timeline-service.enabled false` в `spark-defaults.conf`.

`table_dir` выводится из пути без Hive metastore: отбрасывается имя файла, затем путь
обрезается по первому компоненту-партиции (`key=value`), служебному (начинается с `.` или
`_`: `.hive-staging_*`, `_temporary`) или ACID-компоненту Hive (`base_N`, `delta_N_N`,
`delete_delta_N_N`, у каждого возможен суффикс `_v<n>`). Поэтому staging-файлы, перенесённые
в таблицу, относятся к ней; файлы, удалённые до следующего checkpoint NameNode (scratch-каталоги
Hive, staging-jar задач, временные результаты), в образе отсутствуют и дают `?`. Непартиционированная таблица с
подкаталогами сворачивается на уровень ниже (до подкаталога).

Ограничения:

- События соединяются с образом только по id блока: федеративные кластеры (несколько
  block pool) не поддерживаются.
- Цепочка путей глубже 256 уровней или циклическая (повреждённый образ) разрешается в `?`
  с предупреждением в логе задачи.
- Inode, видимые только в снапшотах, остаются `?` (см. ограничения block-mapper).
- Тесты и стенд привязывают драйвер Spark к `127.0.0.1`, поэтому не зависят от разрешения
  имён хоста.

Пример: таблицы с наибольшей скоростью записи за один час, МБ/с по минутам:

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

### Ежедневный запуск

`deploy/daily-report.sh -n <HTTP-адрес активного NameNode> [-D YYYY-MM-DD] [-r root]
[-w workdir] [-m mapper.jar] [-j spark-jobs.jar] [-c hive|none] [-k keytab -p principal]
[-s principal Spark -t keytab Spark]`. Нужны GNU `date`, `flock`, `python3` и
`curl --negotiate`.

`hdfs dfsadmin -fetchImage` отдаёт последний checkpoint NameNode, а не живое пространство
имён, и скрипт никогда не вызывает `saveNamespace`. Если `<root>/fsimage/dt=<сегодня>` ещё
нет, скрипт сначала опрашивает JMX активного NameNode (`LastCheckpointTime`) каждые
`POLL_SEC` (по умолчанию 300) не дольше `MAX_WAIT_SEC` (по умолчанию 10800), пока не
появится checkpoint после 00:00 UTC сегодняшнего дня; иначе завершается с понятным
сообщением. Для этого нужен рабочий checkpointing: standby NameNode (HA) или
SecondaryNameNode, `dfs.namenode.checkpoint.period` не более 1 часа. В HA передавайте
активный NameNode (`hdfs haadmin -getAllServiceState`), например `http://nn1.example:50070`
на HDP. Затем скрипт скачивает образ, запускает mapper в `fsimage/dt=<сегодня>`, после чего
`CompactEvents` и `ResolvePaths` за предыдущие сутки UTC с `--image-dt <сегодня>`. Чтобы
заменить устаревший `fsimage/dt=<сегодня>`, сначала удалите его. `-D` перезапускает
указанный день по последнему образу (без `--image-dt`). Второй запуск при активном первом
завершается с сообщением (`flock` на `<workdir>/.lock`); скачанный образ удаляется при
выходе. `SPARK_ARGS` (по умолчанию `--master yarn --deploy-mode client`) и `SPARK_HOME`
берутся из окружения.

Пользователи и права:

- Скрипт запускается от администратора кластера HDFS (член `dfs.cluster.administrators`,
  например `hdfs`): это нужно `fetchImage` и mapper. `-k/-p` логинят этого пользователя
  (оба или ни одного).
- Spark-задачи на защищённом кластере нельзя запускать от `hdfs` (LinuxContainerExecutor
  YARN это запрещает). `-s/-t` передают spark-submit отдельные `--principal/--keytab`; они
  обязательны при `-c hive` или `--master yarn` и могут быть опущены для локальных тестов.
  Этому пользователю нужна запись в `<root>/{events,event_path,traffic_minute}` и чтение
  `<root>/{raw,fsimage}`. Если metastore использует storage-based authorization, нужна ещё
  запись (или ACL) на `<root>/fsimage`: `ResolvePaths` создаёт там таблицы `fs_*` и добавляет
  партиции.
- Keytab Spark (`-t`) должен читаться пользователем, от которого работает cron.
- Для NameNode только с HTTPS передайте `-n https://<nn>:50470`.

Cron, 02:00 UTC (после послеполуночного checkpoint). Запись в cron должна занимать одну
физическую строку, поэтому её нельзя переносить. `CRON_TZ=UTC` стоит отдельной строкой над
ней; без неё действует часовой пояс хоста, и 02:00 — это местное время хоста:

```
CRON_TZ=UTC
0 2 * * * hdfs /opt/hdfs-traffic/daily-report.sh -n http://nn1.example:50070 -k /etc/security/keytabs/hdfs.headless.keytab -p hdfs-cluster@REALM -s spark-report@REALM -t /etc/security/keytabs/spark-report.keytab >>/var/log/hdfs-traffic-report.log 2>&1
```

Эксплуатация:

- Сырой файл, пришедший после компактации своего дня (например, DataNode не работал в
  полночь), в этот день не попадает: перезапустите день с `-D` (обе задачи идемпотентны
  по партициям).
- Потеря executor роняет запуск (задачи используют локальные checkpoint): запустите
  заново.
- `user` и `start` — зарезервированные слова Hive 3: в beeline берите их в обратные кавычки.
- Смена `--root` позже требует `ALTER TABLE ... SET LOCATION` для существующих таблиц:
  `ADD IF NOT EXISTS PARTITION` сохраняет старые пути.

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
