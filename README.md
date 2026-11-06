# hdfs-traffic-agent

[English version](README.en.md)

Этот репозиторий считает трафик DataNode HDFS по пользователям и по таблицам Hive. Документ
для администраторов HDFS, которые ставят его на кластер, и для аналитиков, которые читают
отчёты. Три части:

- `agent/`: Java-агент, который загружается в каждую DataNode. Считает байты по
  пользователям в JMX и может писать одно JSONL-событие на операцию.
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
| `<nn_host>` | хост активной NameNode |
| `<realm>` | Kerberos realm |
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
[Сопоставление блоков и файлов](#сопоставление-блоков-и-файлов-block-mapper)).

Jar для раздачи собирайте с `clean`: без него shade-плагин повторно шейдит уже зашейденный
jar.

Результат:

| Модуль | Jar |
|---|---|
| agent | `agent/target/hdfs-traffic-agent-<version>.jar` |
| block-mapper | `block-mapper/target/block-mapper-<version>-hadoop-<версия hadoop>.jar` |
| spark-jobs | `spark-jobs/target/spark-jobs-<version>.jar` |

## Установка агента

Условие: `dfs.block.access.token.enable=true`. С Kerberos оно включено всегда; без него
все пользователи считаются как `__unknown__`.

На каждой DataNode:

1. Скопируйте jar в `/opt/hdfs-traffic-agent/hdfs-traffic-agent.jar`.
2. Добавьте агент в JVM-опции DataNode в `hadoop-env.sh`. На HDP это делается в Ambari:
   **HDFS → Configs → Advanced hadoop-env → hadoop-env template**.

   ```bash
   export HDFS_DATANODE_OPTS="$HDFS_DATANODE_OPTS -javaagent:/opt/hdfs-traffic-agent/hdfs-traffic-agent.jar=maxUsers=2000"
   ```

   Hadoop 3 читает и устаревшее имя `HADOOP_DATANODE_OPTS`.
3. Перезапустите DataNode.

   Проверьте лог DataNode: в нём есть строка `hdfs-traffic-agent installed` и по строке
   `hdfs-traffic-agent: instrumented ...` на каждый перехваченный класс. Строка
   `WARN ... hook is disabled` значит, что метод не найден в этой сборке Hadoop: этот хук
   не считает, остальные работают.

### Опции агента

Опции пишутся после `=` в `-javaagent`, через запятую, в виде `key=value`. Неизвестная или
неверная опция отключает агент с WARN в логе DataNode; сама DataNode всё равно стартует.

| Опция | По умолчанию | Что делает | Когда менять |
|---|---|---|---|
| `maxUsers` | 2000 | сколько пользователей получают свои серии в JMX, в порядке появления; остальные идут в `__other__` | уменьшить, если sink метрик не тянет ~60 тыс. атрибутов на DataNode (см. [Метрики](#метрики)) |
| `events` | выкл. | путь к активному JSONL-файлу событий, например `/var/log/hadoop/hdfs-traffic/events.jsonl`; без запятых | задать, чтобы включить события |
| `eventsQueue` | 65536 | очередь событий к пишущему потоку; переполнение отбрасывается и считается в `EventsDropped` | увеличить, если растёт `EventsDropped` |
| `eventsMaxFileMb` | 256 | размер, при котором активный файл ротируется, МБ | |
| `eventsRotateSec` | 300 | возраст, при котором активный файл ротируется, секунды | |
| `eventsMaxFiles` | 100 | сколько закрытых неотправленных файлов хранить; сверх этого самые старые удаляются и считаются в `EventFilesDropped` | увеличить, если отправка может стоять дольше 100 × `eventsRotateSec` |
| `flushIntervalSec` | 10 | длинные операции отчитываются о байтах каждые N секунд (JMX и события); 0 — только в конце | |

## Метрики

JMX-бин `Hadoop:service=DataNode,name=UserTraffic`. Имена атрибутов имеют вид
`<Counter>.user.<u>.op.<op>.role.<role>.locality.<loc>` (как в nntop), например
`Bytes.user.alice.op.read.role.client.locality.remote`. По HTTP:

```bash
curl -s 'http://<dn_host>:<dn_http_port>/jmx?qry=Hadoop:service=DataNode,name=UserTraffic'
```

Сервлет `/jmx` в Hadoop без ошибки выбрасывает атрибуты, в имени которых есть `=`, `:` или
пробел, поэтому агент заменяет эти символы в именах пользователей на `_`. В имени
пользователя могут быть точки: разбирайте имя справа, как это делает правило jmx_exporter
ниже.

Кардинальность: у пользователя одна серия на каждую встреченную комбинацию (op, role,
locality), обычно 8–10, умноженная на 3 счётчика. При `maxUsers=2000` это до ~50–60 тыс.
атрибутов на DataNode. Источник уходит во все sink'и metrics2, не только в JMX. На HDP sink
Ambari Metrics (`datanode.sink.timeline`) включён по умолчанию; чтобы `UserTraffic` в него не
попадал, добавьте в `hadoop-metrics2.properties`:

```properties
datanode.sink.timeline.source.filter.class=org.apache.hadoop.metrics2.filter.GlobFilter
datanode.sink.timeline.source.filter.exclude=UserTraffic
```

Правило для Prometheus jmx_exporter. С `lowercaseOutputName` имена становятся
`hdfs_datanode_user_traffic_bytes_total` и т. д.:

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

### Файлы событий

Активный файл называется так, как задано в опции `events`, например `events.jsonl`. По
размеру или возрасту он ротируется в `events-<yyyyMMdd-HHmmss>-<seq>.jsonl`; в имени — время
UTC первого события. Файл никогда не захватывает два дня UTC по `ts`: дата в имени — дата
всех его событий (события, переставленные на несколько миллисекунд через полночь, остаются
в более раннем файле). Непустой активный файл, оставшийся после падения, ротируется при
старте агента и датируется по первому событию. `eventsMaxFiles` считает и удаляет только
файлы с именем строго такого вида.

Пользователь DataNode должен иметь право создать каталог событий. Агент создаёт его при
первой записи и повторяет попытку при ошибке.

DataNode никогда не ждёт пишущий поток: если очередь полна или запись упала, события
отбрасываются и считаются, WARN — не чаще раза в минуту. Атрибуты бина `UserTraffic` для
мониторинга:

| Атрибут | Тип | Значение |
|---|---|---|
| `EventsWritten` | счётчик | событий записано в файлы |
| `EventsDropped` | счётчик | событий отброшено (очередь полна, ошибка ввода-вывода) |
| `EventFilesDropped` | счётчик | закрытых файлов удалено по `eventsMaxFiles` до отправки |
| `EventsQueueSize` | gauge | событий ждёт пишущий поток |

### Отправка событий в HDFS

`deploy/upload-events.sh` кладёт закрытые файлы в HDFS как
`<raw_root>/dt=<dt>/dn=<host>/<file>` и удаляет локальный файл только после успешного `put`.

```
upload-events.sh [-d events_dir] [-r hdfs_root] [-n file_prefix] [-k keytab -p principal]
                 [-H host] [-b batch_files] [-j max_jitter_sec]
```

| Опция | По умолчанию | Что делает |
|---|---|---|
| `-d` | `/var/log/hadoop/hdfs-traffic` | каталог событий (каталог файла из опции агента `events`) |
| `-r` | `/data/hdfs-traffic/raw` | корень сырых файлов в HDFS (`<raw_root>`) |
| `-n` | `events` | префикс имён файлов (имя файла `events` без `.jsonl`) |
| `-k`, `-p` | нет | keytab и principal для `kinit`; без Kerberos не указывайте оба |
| `-H` | `hostname -f` | ключ узла в партиции `dn=`; может отличаться от поля `dn` в событиях |
| `-b` | 100 | файлов на один `hdfs dfs -put` |
| `-j` | 0 | перед стартом подождать случайные 0..N секунд |

Как проходит запуск:

- Если закрытых файлов нет, скрипт выходит без `kinit` и без запуска JVM.
- Один `hdfs dfs -mkdir` на все целевые каталоги и один `hdfs dfs -put` на каждый целевой
  каталог и пачку из `-b` файлов: запуск поднимает две-три JVM, а не по одной на файл.
- Берёт `flock` на `<events_dir>/.upload.lock`, поэтому запуски не пересекаются, и держит
  Kerberos-тикет в `<events_dir>/.upload.krb5cc`.
- Упавшая пачка не останавливает запуск: остальные пачки отправляются, файлы упавшей
  остаются до следующего запуска, код выхода 1. Следующий запуск кладёт их заново с `-f`;
  это безопасно, потому что закрытые файлы не меняются.

Запись в HDFS самой отправкой видна как трафик пользователя, от которого она запущена.

### Запуск отправки по таймеру systemd

В `deploy/systemd/` лежат oneshot-сервис и таймер:

- Таймер запускает сервис каждые 5 минут со случайной задержкой до 4 минут
  (`RandomizedDelaySec=240`), чтобы DataNode кластера не шли в NameNode и KDC в одну
  секунду. Пока предыдущий запуск идёт, новый не стартует.
- Сервис работает от `hdfs` с `Nice=10`, классом ввода-вывода best-effort с приоритетом 7,
  `HADOOP_CLIENT_OPTS=-Xmx256m` и `TimeoutStartSec=30min`.

Установка на каждой DataNode, от root, из копии этого репозитория:

1. Установите скрипт и юниты:

   ```bash
   install -d /opt/hdfs-traffic-agent
   install -m 0755 deploy/upload-events.sh /opt/hdfs-traffic-agent/
   install -m 0644 deploy/systemd/hdfs-traffic-upload.service \
       deploy/systemd/hdfs-traffic-upload.timer /etc/systemd/system/
   install -m 0644 deploy/systemd/hdfs-traffic-upload.env /etc/default/hdfs-traffic-upload
   ```

2. В `/etc/default/hdfs-traffic-upload` задайте опции скрипта в `UPLOAD_ARGS`: каталог
   событий, корень в HDFS, keytab и principal. Там же можно задать `JAVA_HOME` и
   `HADOOP_CLIENT_OPTS`.

   Юнит ищет `hdfs` в `/usr/hdp/current/hadoop-client/bin`, `/opt/hadoop/bin` и
   `/usr/local/bin:/usr/bin:/bin`. Для другого расположения переопределите `PATH` через
   `systemctl edit hdfs-traffic-upload.service`.

   `RandomizedDelaySec` требует systemd 229 или новее. Старый systemd (в RHEL/CentOS 7 —
   219) игнорирует его с предупреждением в журнале, и все узлы стартуют в одну секунду:
   добавьте `-j 240` в `UPLOAD_ARGS`.
3. Включите таймер:

   ```bash
   systemctl daemon-reload
   systemctl enable --now hdfs-traffic-upload.timer
   ```

4. Проверьте:

   ```bash
   systemctl list-timers hdfs-traffic-upload.timer   # время следующего запуска
   systemctl start hdfs-traffic-upload.service       # запустить сейчас
   journalctl -u hdfs-traffic-upload.service         # отправленные файлы и ошибки
   ```

Неудачный запуск оставляет сервис в состоянии `failed` до следующего успешного;
мониторьте его как любой другой юнит.

Без systemd используйте cron с `-j`, чтобы узлы не стартовали одновременно. Одна запись — одна
строка:

```
*/5 * * * * hdfs /opt/hdfs-traffic-agent/upload-events.sh -j 240 -d /var/log/hadoop/hdfs-traffic -r /data/hdfs-traffic/raw -k /etc/security/keytabs/hdfs.headless.keytab -p hdfs@<realm>
```

## Сопоставление блоков и файлов (block-mapper)

В событиях есть только id блоков. `block-mapper` читает скачанный fsimage и пишет модель
inode: блок → inode, inode → родитель и имя. Spark-джобы по ней превращают `file_block_id`
события в текущий путь файла (см. [Отчёты](#отчёты-spark-jobs)).

Собирайте его с профилем версии Hadoop кластера: сгенерированные protobuf-классы fsimage в
3.1 и 3.3 разные, и jar, собранный под одну версию, не читает образ другой.

```bash
mvn clean verify -P hadoop-3.1.1      # HDP
mvn clean verify -P hadoop-3.3.6      # Apache Hadoop 3.3.x
```

В jar входят `orc-core` 1.5.13, `orc-shims`, `hive-storage-api`, `aircompressor`,
`commons-lang` 2.6 и `threeten-extra`, перенесённые в пакет
`org.hadooptraffic.mapper.shaded`, поэтому jar'ы Hive в `HADOOP_CLASSPATH` их не подменяют.
`protobuf-java` 2.5.0 берётся из Hadoop. Hadoop 3.4+ (где protobuf 2.5 необязателен) не
поддерживается.

Запускайте его на скачанном образе, никогда на каталоге хранения NameNode. Обе команды
выполняйте от администратора кластера HDFS (участника `dfs.cluster.administrators`,
например `hdfs`) на любом хосте с клиентом HDFS:

```bash
hdfs dfsadmin -fetchImage /data/fsimg
hadoop jar block-mapper-<version>-hadoop-3.1.1.jar org.hadooptraffic.mapper.FsImageMapper \
  -i /data/fsimg/fsimage_<txid> -o hdfs:///data/hdfs-traffic/fsimage/dt=<dt> [--format tsv]
```

`-i` — локальный файл, `<txid>` — номер транзакции в имени скачанного файла. `-o` — любой
URI файловой системы Hadoop.

`fetchImage` возвращает последний checkpoint NameNode, а не живое пространство имён: файлов,
созданных или переименованных после checkpoint'а, в образе нет. Ежедневный скрипт ждёт
свежий checkpoint (см. [Ежедневный запуск](#ежедневный-запуск)).

**Внимание:** не вызывайте `hdfs dfsadmin -saveNamespace` на боевой NameNode ради более
свежего образа. Он требует safe mode, а в safe mode NameNode отклоняет все записи.

Результат — по одному файлу на таблицу, `<out>/<table>/part-00000.orc`:

| Таблица | Колонки |
|---|---|
| `inode` | `inode_id`, `type` (`F` файл, `D` каталог, `S` symlink), `name` |
| `edge` | `child_id`, `parent_id` |
| `block_inode` | `block_id`, `inode_id` |
| `meta` | `txid`, `image_file`, `parsed_at` (epoch, мс) |

ORC (по умолчанию): версия файла 0.12, ZLIB, bloom-фильтры по `inode_id`, `child_id` и
`block_id`; читается Hive 3.1 и Spark 3.5. `--format tsv` пишет `part-00000.tsv` с теми же
колонками через табуляцию; `\`, табуляция, перевод строки и возврат каретки в именах
экранируются как `\\`, `\t`, `\n`, `\r`.

Mapper пишет в скрытый соседний каталог `<parent>/.<name>._tmp`, например
`fsimage/.dt=2026-11-06._tmp` (Spark и Hive не принимают его за партицию), и при успехе
переименовывает его в `<out>`. Существующий `<out>` никогда не перезаписывается.

| Код выхода | Значение |
|---|---|
| 0 | успех |
| 1 | ошибка; сообщение начинается с `block-mapper:` |
| 2 | неверный вызов |

Память: пространство имён читается потоком. В памяти держатся ссылки снапшотов и буферы
записи ORC (их ограничивает memory manager ORC).

Известные проблемы:

- Inode, которые есть только в снапшотах (удалены после снапшота), не имеют родителя и
  остаются без пути.
- Сжатый образ, сохранённый параллельно (`dfs.image.parallel.save=true` вместе с
  `dfs.image.compress=true`, Hadoop 3.3), отклоняется. Скачайте образ, сохранённый с
  одной из двух настроек выключенной.

## Отчёты (spark-jobs)

В `spark-jobs` две Spark-джобы. Они превращают отправленные события и результат
block-mapper в поминутный трафик по пользователям и таблицам.

```bash
mvn clean verify
```

На HDP 3.1 запускайте jar отдельным дистрибутивом `spark-3.5.1-bin-hadoop3`, не Spark 2
кластера.

Опции обеих джоб:

| Опция | По умолчанию | Что делает |
|---|---|---|
| `--dt <dt>` | обязательная | день для обработки |
| `--root <URI каталога>` | `/data/hdfs-traffic` | корень всех таблиц |
| `--db <имя>` | `hdfs_traffic` | база Hive |
| `--catalog hive\|none` | `hive` | `none` только пишет файлы и ничего не регистрирует в Hive |
| `--image-dt <dt>` | последний образ | только `ResolvePaths`: дата используемого fsimage |

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

Каждый запуск перезаписывает только свои партиции `dt`, поэтому упавший запуск повторяют с
тем же `--dt`.

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

**Внимание:** не запускайте `MSCK REPAIR TABLE` на этих таблицах. У четырёх таблиц `fs_*`
одна локация, `<root>/fsimage`, а джобы регистрируют каждую партицию в
`<root>/fsimage/dt=<dt>/<table>`. Для ещё не зарегистрированной даты образа MSCK добавит
`dt=<dt>` с локацией `<root>/fsimage/dt=<dt>`, где лежат файлы всех четырёх таблиц, и запросы
будут читать файлы с чужой схемой. Чтобы откатить, выполните
`ALTER TABLE hdfs_traffic.<table> DROP PARTITION (dt='<dt>')` на таблицах `fs_*` (они
EXTERNAL, файлы останутся); следующий запуск `ResolvePaths` с этим образом зарегистрирует
партицию заново с правильной локацией.

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

### Spark 3.5.1 на HDP 3.1 (YARN, Kerberos, Hive metastore 3.1)

Стенд запускает Spark с `--master local` и `--catalog none`, поэтому эта настройка на
кластере не проверялась. При первом развёртывании выполните проверки в конце раздела.

1. В `$SPARK_HOME/conf/spark-defaults.conf` (не в разовой командной строке, чтобы их
   получали и запуски из cron):

   ```properties
   spark.sql.hive.metastore.version   3.1.3
   spark.sql.hive.metastore.jars      path
   spark.sql.hive.metastore.jars.path file:///usr/hdp/current/hive-client/lib/*
   spark.hadoop.metastore.catalog.default hive
   spark.yarn.populateHadoopClasspath false
   ```

2. Скопируйте `/etc/hive/conf/hive-site.xml` в `$SPARK_HOME/conf`. Не берите его из
   `/etc/spark2/conf`: там `metastore.catalog.default=spark`, и таблицы, созданные в этом
   каталоге, Hive не видит.
3. В `$SPARK_HOME/conf/spark-env.sh` (cron стартует с пустым окружением):

   ```bash
   export HADOOP_CONF_DIR=/etc/hadoop/conf YARN_CONF_DIR=/etc/hadoop/conf
   ```

4. Необязательно: задайте `spark.yarn.archive` — архив jar'ов Spark, заранее загруженный в
   HDFS, чтобы jar'ы не загружались при каждом запуске.

Проверки при первом развёртывании:

1. Запустите джобы с `--catalog hive` за недавний день; обе завершаются с кодом 0.
2. В beeline `SHOW PARTITIONS hdfs_traffic.traffic_minute;` показывает этот день.
3. В beeline `SELECT * FROM hdfs_traffic.traffic_minute LIMIT 10;` возвращает строки (Hive
   читает ORC, записанный Spark).
4. `SHOW PARTITIONS` на таблице `fs_*` показывает новый `dt`: principal Spark может
   добавлять туда партиции.

Если клиент Spark падает на YARN timeline service, добавьте
`spark.hadoop.yarn.timeline-service.enabled false` в `spark-defaults.conf`.

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

### Ежедневный запуск

`deploy/daily-report.sh` строит отчёт за один день UTC: ждёт checkpoint NameNode, скачивает
образ, запускает block-mapper, `CompactEvents` и `ResolvePaths`.

Что нужно на хосте: клиент HDFS, GNU `date`, `flock`, `python3`, `curl` с SPNEGO
(`--negotiate`), Spark 3.5.1 в `$SPARK_HOME`. NameNode должна регулярно делать checkpoint:
standby NameNode (HA) или SecondaryNameNode, `dfs.namenode.checkpoint.period` не больше
1 часа.

```
daily-report.sh -n <active NameNode http URL> [-D <dt>] [-r root] [-w workdir]
                [-m mapper.jar] [-j spark-jobs.jar] [-c hive|none]
                [-k keytab -p principal] [-s spark_principal -t spark_keytab]
```

| Опция | По умолчанию | Что делает |
|---|---|---|
| `-n` | обязательная | HTTP URL активной NameNode, например `http://<nn_host>:50070` на HDP или `https://<nn_host>:50470` для NameNode только с HTTPS. При HA активную найдите через `hdfs haadmin -getAllServiceState` |
| `-D` | вчера (UTC) | перезапустить указанный день на последнем образе (без `--image-dt`) |
| `-r` | `/data/hdfs-traffic` | `--root` джоб |
| `-w` | `/var/tmp/hdfs-traffic` | локальный рабочий каталог: lock, Kerberos-тикет, скачанный образ |
| `-m` | `/opt/hdfs-traffic/block-mapper.jar` | jar block-mapper |
| `-j` | `/opt/hdfs-traffic/spark-jobs.jar` | jar spark-jobs |
| `-c` | `hive` | `--catalog` джоб |
| `-k`, `-p` | нет | keytab и principal администратора HDFS; оба или ни одного |
| `-s`, `-t` | нет | principal и keytab для spark-submit (`--principal/--keytab`); обязательны с `-c hive` или `--master yarn` |

Переменные окружения:

| Переменная | По умолчанию | Что делает |
|---|---|---|
| `SPARK_HOME` | `/opt/spark` | дистрибутив Spark |
| `SPARK_ARGS` | `--master yarn --deploy-mode client` | опции spark-submit |
| `POLL_SEC` | 300 | интервал между проверками checkpoint'а, секунды |
| `MAX_WAIT_SEC` | 10800 | сколько ждать checkpoint, секунды |

Шаги запуска без `-D`:

1. Если `<root>/fsimage/dt=<today>` нет, скрипт каждые `POLL_SEC` опрашивает
   `LastCheckpointTime` активной NameNode (JMX `Hadoop:service=NameNode,name=FSNamesystem`),
   пока тот не станет позже 00:00 UTC сегодняшнего дня. Через `MAX_WAIT_SEC` скрипт
   выходит с кодом 1 и сообщением
   `daily-report: last checkpoint <time> is before <today> 00:00:00 UTC; is checkpointing (standby NameNode / SecondaryNameNode) healthy?`.
   HTTP 401 или 403 от JMX — немедленная ошибка: нет Kerberos-тикета или пользователю нельзя
   читать JMX.
2. Скачивает образ через `hdfs dfsadmin -fetchImage` в `<workdir>/fsimg` и запускает
   block-mapper в `<root>/fsimage/dt=<today>`.
3. Запускает `CompactEvents` и `ResolvePaths` за вчера с `--image-dt <today>`.

Второй запуск, пока идёт первый, выходит с кодом 0 и сообщением (`flock` на
`<workdir>/.lock`). Скачанный образ удаляется при выходе.

Пользователи и права:

- Скрипт работает от администратора кластера HDFS (участника `dfs.cluster.administrators`,
  например `hdfs`): это нужно `fetchImage` и mapper'у. `-k/-p` логинят этого пользователя.
- На защищённом кластере Spark-джобы не могут работать от `hdfs`: LinuxContainerExecutor
  YARN запрещает этого пользователя. `-s/-t` дают им отдельный principal. Ему нужны:
  - запись в `<root>/{events,event_path,traffic_minute}`;
  - чтение `<root>/{raw,fsimage}`;
  - запись (или ACL) на `<root>/fsimage`, если Hive metastore использует storage-based
    авторизацию: `ResolvePaths` создаёт там таблицы `fs_*` и добавляет партиции.
- Keytab Spark (`-t`) должен быть доступен на чтение пользователю, от которого cron
  запускает скрипт.

Cron в 02:00 UTC, после checkpoint'а за полночь. Cron читает одну запись из одной строки,
поэтому запись не переносится. `CRON_TZ=UTC` поставьте отдельной строкой над ней; без неё
cron берёт часовой пояс хоста:

```
CRON_TZ=UTC
0 2 * * * hdfs /opt/hdfs-traffic/daily-report.sh -n http://<nn_host>:50070 -k /etc/security/keytabs/hdfs.headless.keytab -p hdfs-<cluster>@<realm> -s spark-report@<realm> -t /etc/security/keytabs/spark-report.keytab >>/var/log/hdfs-traffic-report.log 2>&1
```

`hdfs-<cluster>@<realm>` — headless principal HDFS кластера (на HDP в его имени — имя
кластера).

Эксплуатация:

- Сырой файл, пришедший после того, как его день уже обработан (например, с DataNode,
  которая лежала в полночь), в этот день не попадает. Перезапустите день с `-D <dt>`; обе
  джобы перезаписывают только свои партиции.
- Джобы используют локальные checkpoint'ы Spark, поэтому потеря executor'а роняет запуск.
  Перезапустите его.
- Чтобы заменить устаревший `fsimage/dt=<today>`, удалите его перед запуском:

  ```bash
  hdfs dfs -rm -r -skipTrash <root>/fsimage/dt=<today>
  ```

- После смены `--root` перенесите существующие таблицы через `ALTER TABLE ... SET LOCATION`:
  `ADD IF NOT EXISTS PARTITION` оставляет старые локации существующих партиций.

Известные проблемы:

- События соединяются с образом только по id блока, поэтому федеративные кластеры (несколько
  block pool) не поддерживаются.
- Цепочка пути глубже 256 уровней или цикл (битый образ) дают `?` с WARN в логе джобы.
- Inode, которые есть только в снапшотах, получают `?` (см. известные проблемы
  block-mapper).

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
