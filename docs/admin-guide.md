# hdfs-traffic-agent: руководство администратора

[English version](admin-guide.en.md) · [Общее описание](../README.md)

Руководство для администраторов HDFS: установка агента на DataNode, его опции, подключение
метрик к системе мониторинга, отправка событий в HDFS, запуск на большом кластере и
ежедневные отчёты (block-mapper и spark-jobs). Что означают измерения, метрики, поля
событий и таблицы отчётов, описано в [README](../README.md).

Требования:

- Apache Hadoop 3.3.x или HDP 3.1 (Hadoop 3.1.1), Java 8;
- для отчётов — Spark 3.5.1 (Scala 2.12), Spark 2 не поддерживается;
- jar'ы, собранные по [инструкции в README](../README.md#сборка).

Плейсхолдеры в этом документе:

| Плейсхолдер | Значение |
|---|---|
| `<version>` | версия проекта из `pom.xml`, например `0.1.0-SNAPSHOT` |
| `<dn_host>`, `<dn_http_port>` | хост DataNode и её HTTP-порт (`dfs.datanode.http.address`) |
| `<nn_host>` | хост активной NameNode |
| `<realm>` | Kerberos realm |
| `<root>` | каталог отчётов в HDFS, по умолчанию `/data/hdfs-traffic` |
| `<dt>` | день по UTC, `YYYY-MM-DD` |

## Установка агента

Условие: `dfs.block.access.token.enable=true`. С Kerberos оно включено всегда; без него
все пользователи считаются как `__unknown__`.

На каждой DataNode:

1. Скопируйте jar в `/opt/hdfs-traffic-agent/hdfs-traffic-agent.jar`.
2. Добавьте агент в JVM-опции DataNode в `hadoop-env.sh`. На HDP это делается в Ambari:
   **HDFS → Configs → Advanced hadoop-env → hadoop-env template**.

   ```bash
   export HDFS_DATANODE_OPTS="$HDFS_DATANODE_OPTS -javaagent:/opt/hdfs-traffic-agent/hdfs-traffic-agent.jar=events=/var/log/hadoop/hdfs-traffic/events.jsonl"
   ```

   Без опций агент ничего не записывает: события включает `events`, счётчики по
   пользователям в JMX — `userMetrics=true` (см. [Опции агента](#опции-агента)).

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
| `userMetrics` | `false` | `true` публикует счётчики по пользователям в JMX (см. [Метрики](../README.md#метрики)) | включить, если серии по пользователям нужны в системе метрик и она выдержит до ~60 тыс. атрибутов на DataNode |
| `maxUsers` | 2000 | только с `userMetrics=true`: сколько пользователей получают свои серии в JMX, в порядке появления; остальные идут в `__other__` | уменьшить, если sink метрик не тянет ~60 тыс. атрибутов на DataNode |
| `events` | выкл. | путь к активному JSONL-файлу событий, например `/var/log/hadoop/hdfs-traffic/events.jsonl`; без запятых | задать, чтобы включить события |
| `eventsQueue` | 65536 | очередь событий к пишущему потоку; переполнение отбрасывается и считается в `EventsDropped` | увеличить, если растёт `EventsDropped` |
| `eventsMaxFileMb` | 256 | размер, при котором активный файл ротируется, МБ | |
| `eventsRotateSec` | 300 | возраст, при котором активный файл ротируется, секунды | |
| `eventsMaxFiles` | 100 | сколько закрытых неотправленных файлов хранить; сверх этого самые старые удаляются и считаются в `EventFilesDropped` | увеличить, если отправка может стоять дольше, чем переживёт буфер (см. [Локальный буфер на DataNode](#локальный-буфер-на-datanode)) |
| `flushIntervalSec` | 10 | длинные операции отчитываются о байтах каждые N секунд (JMX и события); 0 — только в конце | |

## Метрики в системе мониторинга

Этот раздел нужен только с `userMetrics=true`: без неё в бине `UserTraffic` четыре
счётчика пишущего потока.

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

## События: локальные файлы и отправка в HDFS

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

## Запуск на большом кластере

Раздел для тех, кто включает агент на сотнях DataNode. На каждом узле агент добавляет
доли микросекунды на операцию. Ограничения на таком кластере другие: суммарный объём
событий, число серий метрик и сырые файлы в HDFS, которые никто не удаляет. Их нужно
оценить до запуска и проверить на пилоте.

Цифры ниже получены синтетическим прогоном пути данных агента без DataNode: JDK 8, 11 ядер,
локальный SSD, события чтения пользователей `user0`…`user199`. На этом стенде пишущий поток
записывал ~735 тыс. событий/с; всё, что не успевало, уходило в `EventsDropped`, потоки
операций не ждали.

### Оценка объёма событий

Агент пишет одно событие на операцию: на операции DataXceiver (`readBlock`, `writeBlock`,
`transferBlock`, `copyBlock`, `replaceBlock`, `requestShortCircuitFds`), на
`DataNode$DataTransfer` и на запрос WebHDFS. Длинная операция дополнительно пишет событие
каждые `flushIntervalSec`. Средняя строка в прогоне — 343 байта; в реальных событиях длина
зависит от имён пользователей, адресов и `path`.

Число операций оцените по счётчикам DataNode, которые есть и без агента. Снимите их дважды
с интервалом 60 секунд; разница, делённая на 60, — операций в секунду:

```bash
curl -s 'http://<dn_host>:<dn_http_port>/jmx?qry=Hadoop:service=DataNode,name=DataNodeActivity-*' \
  | grep -E '"(ReadBlockOp|WriteBlockOp|CopyBlockOp|ReplaceBlockOp)NumOps"'
```

`<dn_http_port>` — HTTP-порт DataNode (`dfs.datanode.http.address`). Если HTTP DataNode
закрыт SPNEGO, добавьте `--negotiate -u :` после `kinit`. Запросы short-circuit
дескрипторов и передачи, начатые самой DataNode, в эти четыре счётчика не входят, поэтому
оценка снизу. Сделайте её на нескольких DataNode с разной нагрузкой: узлы с HBase или
мелкими позиционными чтениями дают на порядки больше операций, чем узлы с крупными
последовательными чтениями.

Объём в сутки — `операций/с × 343 Б × 86 400`:

| Операций/с на DataNode | На одну DataNode в сутки | 500 DataNode в сутки | 500 DataNode в HDFS с репликацией 3 |
|---|---|---|---|
| 500 | 14,8 ГБ | 7,4 ТБ | 22 ТБ |
| 1 000 | 29,6 ГБ | 14,8 ТБ | 44 ТБ |
| 5 000 | 148 ГБ | 74 ТБ | 222 ТБ |

Точный объём даёт пилот (см. [Пилот](#пилот)).

### Локальный буфер на DataNode

Пока отправка не работает (NameNode недоступна, нет тикета Kerberos), закрытые файлы копятся
в каталоге событий. Активный файл ротируется по тому, что наступит раньше: `eventsRotateSec`
или `eventsMaxFileMb`. Отсюда:

- время, которое переживёт буфер, — `eventsMaxFiles × min(eventsRotateSec, eventsMaxFileMb / поток)`;
- место на диске — до `eventsMaxFiles × eventsMaxFileMb`, с настройками по умолчанию 25,6 ГБ.

| Событий/с | Размер закрытого файла | Буфер по умолчанию | Место |
|---|---|---|---|
| 1 000 | ~103 МБ (ротация по 300 с) | ~8,3 ч | ~10 ГБ |
| 5 000 | 256 МБ (ротация каждые ~150 с) | ~4,2 ч | 25,6 ГБ |

Когда файлов больше `eventsMaxFiles`, самые старые удаляются и растёт `EventFilesDropped`:
события за этот период потеряны.

**Внимание:** каталог по умолчанию `/var/log/hadoop/hdfs-traffic` лежит рядом с логами
DataNode. Если этот раздел меньше места из таблицы, при простое отправки он заполнится,
и DataNode перестанет писать свой лог. Вынесите каталог событий на раздел с запасом места
или уменьшите `eventsMaxFiles`.

### Метрики на кластере

Счётчики по пользователям по умолчанию выключены (`userMetrics=false`); в JMX остаются
четыре счётчика пишущего потока. Этого достаточно для мониторинга агента на любом
кластере. Остальная часть этого подраздела нужна, только если вы включаете
`userMetrics=true`.

При `maxUsers=2000` одна DataNode отдаёт до ~50–60 тыс. атрибутов `UserTraffic`, 500
DataNode — до 25–30 млн серий. В прогоне с 2000 пользователей и 16 комбинациями (op, role,
locality) у каждого снимок занял 27 мс и дал 96 тыс. атрибутов, ответ `/jmx` — ~8 МБ на
DataNode. Счётчики не сбрасываются до перезапуска DataNode: пользователь, однажды попавший
в серию, остаётся в ней.

Перед включением `userMetrics=true`:

- исключите `UserTraffic` из sink'а Ambari Metrics (см. [Метрики в системе мониторинга](#метрики-в-системе-мониторинга)), иначе туда
  уйдут все серии по пользователям;
- для Prometheus задайте `maxUsers` по реальному числу пользователей кластера и опрашивайте
  `/jmx` с `qry=Hadoop:service=DataNode,name=UserTraffic`, а не целиком.

### Ретенция сырых файлов

Ни один компонент не удаляет `<raw_root>/dt=<dt>`: `CompactEvents` только читает её. При
ротации раз в 300 секунд каждая DataNode кладёт не меньше 288 файлов в сутки, 500 DataNode
— не меньше 144 тыс. файлов в сутки. Это объекты в памяти NameNode и место из таблицы
объёма.

Удаляйте сырую партицию после успешного `CompactEvents`, оставив несколько дней на
перезапуски `daily-report.sh -D`. Перед удалением проверьте, что сжатая партиция есть:

```bash
hdfs dfs -test -d /data/hdfs-traffic/events/dt=<dt> \
  && hdfs dfs -rm -r -skipTrash /data/hdfs-traffic/raw/dt=<dt>
```

`<dt>` — день в формате `YYYY-MM-DD`. После удаления перезапуск этого дня через `-D`
невозможен: `CompactEvents` падает на отсутствующей сырой партиции, а
`events/dt=<dt>` остаётся прежней.

### Пилот

1. Оцените число операций на нескольких DataNode и объём по таблице выше.
2. Выберите раздел для каталога событий и `eventsMaxFiles` по формуле буфера.
3. Оставьте `userMetrics=false` (по умолчанию). Если серии по пользователям нужны,
   включите их сначала на пилоте и оцените нагрузку на систему метрик (см.
   [Метрики на кластере](#метрики-на-кластере)).
4. Включите агент на 5–10 DataNode (см. [Установка агента](#установка-агента)) и таймер
   отправки на них же.
5. Через сутки проверьте на каждой пилотной DataNode:

   - `EventsDropped` и `EventFilesDropped` равны 0, `EventsQueueSize` близок к 0;
   - в `journalctl -u hdfs-traffic-upload.service` нет ошибок, каталог событий не растёт;
   - `ReadBlockOpAvgTime` и `WriteBlockOpAvgTime` из `DataNodeActivity-*`, загрузка CPU и
     паузы GC DataNode — на уровне суток до включения агента.

6. Посчитайте реальный объём на узел и умножьте на число DataNode:

   ```bash
   hdfs dfs -du -s -h /data/hdfs-traffic/raw/dt=<dt>/dn=<host>
   ```

   `<host>` — ключ узла из `upload-events.sh -H`, по умолчанию `hostname -f` DataNode.
7. Настройте ретенцию сырых файлов до включения на всём кластере.

Откат: уберите `-javaagent:...` из `HDFS_DATANODE_OPTS` и перезапустите DataNode. Оставьте
таймер отправки, пока каталог событий не опустеет, затем выполните
`systemctl disable --now hdfs-traffic-upload.timer`.

## Сопоставление блоков и файлов (block-mapper)

В событиях есть только id блоков. `block-mapper` читает скачанный fsimage и пишет модель
inode: блок → inode, inode → родитель и имя. Spark-джобы по ней превращают `file_block_id`
события в текущий путь файла (см. [Отчёты](../README.md#отчёты-spark-jobs)).

Один jar работает на Hadoop 3.1 и 3.3. Wire-формат fsimage `block-mapper` разбирает сам и
использует только публичный API `hadoop-common` (`Configuration`, `FileSystem`, кодеки
сжатия). Сгенерированные классы `FsImageProto` из `hadoop-hdfs` он не использует: в 3.1 они
собраны под `com.google.protobuf` 2.5, в 3.3 — под затенённый
`org.apache.hadoop.thirdparty.protobuf`, и байткод, скомпилированный под одну версию, падает
на другой с `NoSuchMethodError`. Номера полей fsimage сверяет тест `FsImageSchemaTest`
с каждой версией Hadoop, под которую идёт сборка.

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
hadoop jar block-mapper-<version>.jar org.hadooptraffic.mapper.FsImageMapper \
  -i /data/fsimg/fsimage_<txid> -o hdfs:///data/hdfs-traffic/fsimage/dt=<dt> \
  [--format tsv] [--threads <n>]
```

`-i` — локальный файл, `<txid>` — номер транзакции в имени скачанного файла. `-o` — любой
URI файловой системы Hadoop. `--threads` — число потоков записи ORC на каждую из таблиц
`inode`, `edge` и `block_inode`, от 1 до 64, по умолчанию 2 (см.
[Время и ресурсы](#время-и-ресурсы-block-mapper)); для TSV не действует.

`fetchImage` возвращает последний checkpoint NameNode, а не живое пространство имён: файлов,
созданных или переименованных после checkpoint'а, в образе нет. Ежедневный скрипт ждёт
свежий checkpoint (см. [Ежедневный запуск](#ежедневный-запуск)).

**Внимание:** не вызывайте `hdfs dfsadmin -saveNamespace` на боевой NameNode ради более
свежего образа. Он требует safe mode, а в safe mode NameNode отклоняет все записи.

Результат — каталог на таблицу, `<out>/<table>/part-<nnnnn>.orc`. `inode`, `edge` и
`block_inode` пишутся в файлы `part-00000.orc` … `part-<threads−1>.orc`, по одному на
поток записи. Порядок строк между файлами не сохраняется, Spark и Hive читают каталог
целиком. Файл потока, которому не досталось ни одной пачки строк (1024 строки), не
создаётся. `part-00000.orc` есть всегда, даже в пустой таблице. `meta` — один файл.

| Таблица | Колонки |
|---|---|
| `inode` | `inode_id`, `type` (`F` файл, `D` каталог, `S` symlink), `name` |
| `edge` | `child_id`, `parent_id` |
| `block_inode` | `block_id`, `inode_id` |
| `meta` | `txid`, `image_file`, `parsed_at` (epoch, мс) |

ORC (по умолчанию): версия файла 0.12, ZLIB, без bloom-фильтров (`ResolvePaths` таблицы
только джойнит, а bloom-фильтр ORC помогает лишь при точечном фильтре вида
`WHERE block_id = <id>`); читается Hive 3.1 и Spark 3.5. `--format tsv` пишет по одному
файлу `part-00000.tsv` на таблицу в один поток, с теми же
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

### Время и ресурсы block-mapper

Образ разбирает один поток. Заполненные пачки строк он передаёт потокам записи ORC по
кругу: `--threads` потоков на каждую из таблиц `inode`, `edge` и `block_inode` и один на
`meta`. Около 80% времени уходит на сжатие ORC, поэтому потоки записи сокращают время,
пока не упираются в разбор.

Замер на синтетическом образе: 20,1 млн inode, 24 млн блоков, 2017 МБ, inode в порядке
бакетов `INodeMap`, как их сохраняет NameNode. Apple M3 Pro, JDK 8 (Liberica 1.8.0_432),
`-Xmx4g`, запись в локальный `file://`. Генератор образа и бенчмарк лежат в тестах:
`SyntheticImage` и `MapperBench`.

| Вариант | Время | CPU | Пик кучи |
|---|---|---|---|
| только разбор, без записи | 3,7–4,1 с | 4,0–4,4 с | 0,3 ГБ |
| до распараллеливания: один поток, bloom-фильтры | 21,6–22,1 с | 25 с | 0,9 ГБ |
| `--threads 1` | 13,2 с | 24 с | 1,4 ГБ |
| `--threads 2` (по умолчанию) | 8,7 с | 24 с | 1,3 ГБ |
| `--threads 4` | 7,5 с | 26 с | 1,6 ГБ |
| `--threads 8` | 7,0 с | 27 с | 1,7 ГБ |

Образ разбирается и пишется потоком, без структур, растущих с числом inode, поэтому время
пропорционально размеру образа; замерен один размер. Для образа на 300 млн блоков
умножайте на 12–15.
Реальные id блоков и inode упорядочены хуже синтетических, поэтому ORC сожмёт их хуже и
потратит больше CPU [проверить на образе боевого кластера]. Запись в HDFS добавляет
передачу результата по сети: для синтетического образа это 0,4 ГБ.

- Ядра: занято примерно `CPU / время`, при `--threads 2` около трёх. Если block-mapper
  делит хост с NameNode или другими сервисами, оставьте 1–2.
- Память: разбор держит в куче только ссылки снапшотов (`INODE_REFERENCE`, около 40 байт на
  ссылку), остальное — буферы ORC. Их ограничивает memory manager ORC (половина кучи), и с
  ростом числа файлов он уменьшает stripe. Пик кучи в таблице снят при `-Xmx4g`, где GC
  не торопится. Тот же образ проходит с `-Xmx512m` и `--threads 2` за 9,2 с, с `-Xmx1g` и
  `--threads 4` за 7,6 с. Куча задаётся через `HADOOP_HEAPSIZE_MAX` или `HADOOP_CLIENT_OPTS`
  [проверить имя переменной для Hadoop 3.1 в HDP].
- Сообщение fsimage больше 64 МБ считается повреждением. Самое крупное сообщение —
  каталог со всеми детьми, около 5 байт на ребёнка, поэтому предел
  `dfs.namenode.fs-limits.max-directory-items` (до 6,4 млн) в него укладывается.

Известные проблемы:

- Inode, которые есть только в снапшотах (удалены после снапшота), не имеют родителя и
  остаются без пути.
- Сжатый образ, сохранённый параллельно (`dfs.image.parallel.save=true` вместе с
  `dfs.image.compress=true`, Hadoop 3.3), отклоняется. Скачайте образ, сохранённый с
  одной из двух настроек выключенной.

## Отчёты: запуск (spark-jobs)

Что делают джобы и какие таблицы пишут, описано в
[README](../README.md#отчёты-spark-jobs). Сборка:

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
| `--nets name=cidr[,name=cidr...]` | нет | только `ResolvePaths`: сети для `traffic_minute.origin`, например `own=10.0.0.0/24,cluster_b=172.16.0.0/24`. Без опции remote-события получают `other`. Правила — в [README](../README.md#источник-обращения-origin) |

Каждый запуск перезаписывает только свои партиции `dt`, поэтому упавший запуск повторяют с
тем же `--dt`.

**Внимание:** не запускайте `MSCK REPAIR TABLE` на этих таблицах. У четырёх таблиц `fs_*`
одна локация, `<root>/fsimage`, а джобы регистрируют каждую партицию в
`<root>/fsimage/dt=<dt>/<table>`. Для ещё не зарегистрированной даты образа MSCK добавит
`dt=<dt>` с локацией `<root>/fsimage/dt=<dt>`, где лежат файлы всех четырёх таблиц, и запросы
будут читать файлы с чужой схемой. Чтобы откатить, выполните
`ALTER TABLE hdfs_traffic.<table> DROP PARTITION (dt='<dt>')` на таблицах `fs_*` (они
EXTERNAL, файлы останутся); следующий запуск `ResolvePaths` с этим образом зарегистрирует
партицию заново с правильной локацией.

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
                [-N name=cidr[,name=cidr...]]
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
| `-N` | нет | `--nets` для `ResolvePaths` |

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
