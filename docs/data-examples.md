# hdfs-traffic-agent: пример данных от событий до отчёта

[English version](data-examples.en.md) · [Общее описание](../README.md)

Документ показывает на одном сценарии, какие строки получаются на каждом шаге: JSONL агента,
`events`, таблицы block-mapper, `event_path` и `traffic_minute`. Он для тех, кто пишет
запросы к отчётам и хочет понимать, откуда берётся каждое число. Описание колонок — в
[README](../README.md#отчёты-spark-jobs).

Данные выдуманы и не получены прогоном на стенде, но собраны по коду `EventJson`,
`WebHdfsHooks`, `XceiverHooks`, `CompactEvents`, `ResolvePaths`, `PathResolver` и `TableDir`.
На каждом шаге показаны все 22 события сценария. Колонка `#` — сквозной номер события: по
нему строку `events` можно найти в `event_path`, а в `traffic_minute` колонка «строки» говорит,
из каких событий собрана группа.

Обозначения:

| Обозначение | Значение |
|---|---|
| `dn1`, `dn2`, `dn3` | DataNode `dn1.example.com` (10.0.0.11), `dn2.example.com` (10.0.0.12), `dn3.example.com` (10.0.0.13) |
| время `10:15:50.000` | UTC, 2026-11-06; в таблицах хранится epoch в мс (`1793960150000`) |
| `BP-1` | block pool id, сокращён; в реальном кластере вида `BP-<n>-<ip>-<ms>` |
| `<root>` | корень данных отчётов, по умолчанию `/data/hdfs-traffic` |

## Сценарий

День `2026-11-06`, `flushIntervalSec=30`, репликация 3. Других операций в кластере нет.

| Буква | Что происходит | События |
|---|---|---|
| A | `alice` (Hive-задача на узле dn1) пишет блок `1073741901` размером 128 МиБ в файл `/warehouse/tablespace/managed/hive/sales.db/orders/dt=2026-11-05/delta_0000012_0000012/bucket_00000`. Конвейер записи: dn1 → dn2 → dn3. Операция идёт около 52 с | 6: по два на каждом DataNode (промежуточное через 30 с и последнее) |
| B | `bob` на узле dn2 читает этот файл через short-circuit | 1: `scr_fds` на dn2 |
| E | `carol` (Spark-задача на узле 10.0.0.22, где нет DataNode) читает этот же блок целиком по сети с dn3. Чтение идёт 40,5 с | 2 на dn3: промежуточное через 30 с и последнее |
| C | `etl` с узла 10.0.0.50 выполняет `hdfs dfs -put` через WebHDFS в `/landing/crm/clients.csv` (10 МиБ, блок `1073741955`). DataNode dn1 принимает HTTP и пишет блок встроенным DFSClient по конвейеру dn1 → dn2 → dn3 | 4: HTTP-плечо и `webhdfs_proxy` на dn1, реплики на dn2 и dn3 |
| D | `alice` (Hive-задача на узле 10.0.0.21, где нет DataNode) пишет 2 МиБ (блок `1073742010`) во временный каталог `/tmp/hive/alice/…`, который удаляется до checkpoint'а NameNode. Конвейер: dn3 → dn1 → dn2 | 3: по одному на каждом DataNode |
| F | `radcom` (Hive-задача на узле dn2) пишет 64 МиБ (блок `1073742100`) во внешнюю таблицу `/warehouse/tablespace/external/hive/radcom.db/cdr/gtpu`, партиция `dt=2026-11-06/hour=10`, файл `000000_0`. Конвейер: dn2 → dn3 → dn1, 12 с | 3: по одному на каждом DataNode |
| G | `radcom` (Hive-задача на узле 10.0.0.23, где нет DataNode) пишет 16 МиБ (блок `1073742120`) во внешнюю таблицу `/warehouse/tablespace/external/hive/radcom.db/cdr/s1ap`, партиция `dt=2026-11-06/hour=10`, файл `000000_0`. Конвейер: dn1 → dn3 → dn2, 4 с | 3: по одному на каждом DataNode |

## Шаг 1. JSONL агента

Агент на каждом DataNode пишет события в локальный файл и ротирует его в
`events-<yyyyMMdd-HHmmss>-<seq>.jsonl`, где время — UTC первого события файла.
`deploy/upload-events.sh` кладёт закрытые файлы в `<root>/raw/dt=2026-11-06/dn=<host>/`.
Строки в файле идут в порядке выдачи событий.

`<root>/raw/dt=2026-11-06/dn=dn1.example.com/events-20261106-101539-000007.jsonl`:

```json
{"ts":1793960139950,"start":1793960136200,"dn":"dn1.example.com","op_id":"a1f3c2-19","user":"etl","op":"write","role":"webhdfs_proxy","locality":"local","bpid":"BP-1","block_id":1073741955,"file_block_id":1073741955,"bytes":10485760,"est_bytes":0,"dur_ms":3750,"last":true,"peer":"/10.0.0.11:51310","path":null}
{"ts":1793960140000,"start":1793960136000,"dn":"dn1.example.com","op_id":"a1f3c2-18","user":"etl","op":"webhdfs_create","role":"client","locality":"remote","bpid":null,"block_id":-1,"file_block_id":-1,"bytes":10485760,"est_bytes":0,"dur_ms":4000,"last":true,"peer":"/10.0.0.50:41234","path":"/landing/crm/clients.csv._COPYING_"}
{"ts":1793960150000,"start":1793960120000,"dn":"dn1.example.com","op_id":"a1f3c2-17","user":"alice","op":"write","role":"client","locality":"local","bpid":"BP-1","block_id":1073741901,"file_block_id":1073741901,"bytes":83886080,"est_bytes":0,"dur_ms":30000,"last":false,"peer":"/10.0.0.11:51234","path":null}
{"ts":1793960154990,"start":1793960154010,"dn":"dn1.example.com","op_id":"a1f3c2-20","user":"alice","op":"write","role":"pipeline","locality":"remote","bpid":"BP-1","block_id":1073742010,"file_block_id":1073742010,"bytes":2097152,"est_bytes":0,"dur_ms":980,"last":true,"peer":"/10.0.0.13:52004","path":null}
{"ts":1793960172000,"start":1793960120000,"dn":"dn1.example.com","op_id":"a1f3c2-17","user":"alice","op":"write","role":"client","locality":"local","bpid":"BP-1","block_id":1073741901,"file_block_id":1073741901,"bytes":50331648,"est_bytes":0,"dur_ms":52000,"last":true,"peer":"/10.0.0.11:51234","path":null}
{"ts":1793960216980,"start":1793960205020,"dn":"dn1.example.com","op_id":"a1f3c2-21","user":"radcom","op":"write","role":"pipeline","locality":"remote","bpid":"BP-1","block_id":1073742100,"file_block_id":1073742100,"bytes":67108864,"est_bytes":0,"dur_ms":11960,"last":true,"peer":"/10.0.0.13:52080","path":null}
{"ts":1793960229000,"start":1793960225000,"dn":"dn1.example.com","op_id":"a1f3c2-22","user":"radcom","op":"write","role":"client","locality":"remote","bpid":"BP-1","block_id":1073742120,"file_block_id":1073742120,"bytes":16777216,"est_bytes":0,"dur_ms":4000,"last":true,"peer":"/10.0.0.23:51522","path":null}
```

`<root>/raw/dt=2026-11-06/dn=dn2.example.com/events-20261106-101539-000007.jsonl`:

```json
{"ts":1793960139940,"start":1793960136210,"dn":"dn2.example.com","op_id":"7be019-41","user":"etl","op":"write","role":"pipeline","locality":"remote","bpid":"BP-1","block_id":1073741955,"file_block_id":1073741955,"bytes":10485760,"est_bytes":0,"dur_ms":3730,"last":true,"peer":"/10.0.0.11:47790","path":null}
{"ts":1793960150010,"start":1793960120010,"dn":"dn2.example.com","op_id":"7be019-40","user":"alice","op":"write","role":"pipeline","locality":"remote","bpid":"BP-1","block_id":1073741901,"file_block_id":1073741901,"bytes":83886080,"est_bytes":0,"dur_ms":30000,"last":false,"peer":"/10.0.0.11:47710","path":null}
{"ts":1793960154980,"start":1793960154020,"dn":"dn2.example.com","op_id":"7be019-42","user":"alice","op":"write","role":"pipeline","locality":"remote","bpid":"BP-1","block_id":1073742010,"file_block_id":1073742010,"bytes":2097152,"est_bytes":0,"dur_ms":960,"last":true,"peer":"/10.0.0.11:47850","path":null}
{"ts":1793960171990,"start":1793960120010,"dn":"dn2.example.com","op_id":"7be019-40","user":"alice","op":"write","role":"pipeline","locality":"remote","bpid":"BP-1","block_id":1073741901,"file_block_id":1073741901,"bytes":50331648,"est_bytes":0,"dur_ms":51980,"last":true,"peer":"/10.0.0.11:47710","path":null}
{"ts":1793960190002,"start":1793960190000,"dn":"dn2.example.com","op_id":"7be019-43","user":"bob","op":"scr_fds","role":"client","locality":"local","bpid":"BP-1","block_id":1073741901,"file_block_id":1073741901,"bytes":0,"est_bytes":134217728,"dur_ms":2,"last":true,"peer":"unix:/var/lib/hadoop-hdfs/dn_socket","path":null}
{"ts":1793960217000,"start":1793960205000,"dn":"dn2.example.com","op_id":"7be019-44","user":"radcom","op":"write","role":"client","locality":"local","bpid":"BP-1","block_id":1073742100,"file_block_id":1073742100,"bytes":67108864,"est_bytes":0,"dur_ms":12000,"last":true,"peer":"/10.0.0.12:52310","path":null}
{"ts":1793960228980,"start":1793960225020,"dn":"dn2.example.com","op_id":"7be019-45","user":"radcom","op":"write","role":"pipeline","locality":"remote","bpid":"BP-1","block_id":1073742120,"file_block_id":1073742120,"bytes":16777216,"est_bytes":0,"dur_ms":3960,"last":true,"peer":"/10.0.0.13:38990","path":null}
```

`<root>/raw/dt=2026-11-06/dn=dn3.example.com/events-20261106-101539-000007.jsonl`:

```json
{"ts":1793960139930,"start":1793960136220,"dn":"dn3.example.com","op_id":"c04d88-6","user":"etl","op":"write","role":"pipeline","locality":"remote","bpid":"BP-1","block_id":1073741955,"file_block_id":1073741955,"bytes":10485760,"est_bytes":0,"dur_ms":3710,"last":true,"peer":"/10.0.0.12:38900","path":null}
{"ts":1793960150020,"start":1793960120020,"dn":"dn3.example.com","op_id":"c04d88-5","user":"alice","op":"write","role":"pipeline","locality":"remote","bpid":"BP-1","block_id":1073741901,"file_block_id":1073741901,"bytes":83886080,"est_bytes":0,"dur_ms":30000,"last":false,"peer":"/10.0.0.12:38822","path":null}
{"ts":1793960155000,"start":1793960154000,"dn":"dn3.example.com","op_id":"c04d88-7","user":"alice","op":"write","role":"client","locality":"remote","bpid":"BP-1","block_id":1073742010,"file_block_id":1073742010,"bytes":2097152,"est_bytes":0,"dur_ms":1000,"last":true,"peer":"/10.0.0.21:60112","path":null}
{"ts":1793960171980,"start":1793960120020,"dn":"dn3.example.com","op_id":"c04d88-5","user":"alice","op":"write","role":"pipeline","locality":"remote","bpid":"BP-1","block_id":1073741901,"file_block_id":1073741901,"bytes":50331648,"est_bytes":0,"dur_ms":51960,"last":true,"peer":"/10.0.0.12:38822","path":null}
{"ts":1793960216990,"start":1793960205010,"dn":"dn3.example.com","op_id":"c04d88-9","user":"radcom","op":"write","role":"pipeline","locality":"remote","bpid":"BP-1","block_id":1073742100,"file_block_id":1073742100,"bytes":67108864,"est_bytes":0,"dur_ms":11980,"last":true,"peer":"/10.0.0.12:38950","path":null}
{"ts":1793960228990,"start":1793960225010,"dn":"dn3.example.com","op_id":"c04d88-10","user":"radcom","op":"write","role":"pipeline","locality":"remote","bpid":"BP-1","block_id":1073742120,"file_block_id":1073742120,"bytes":16777216,"est_bytes":0,"dur_ms":3980,"last":true,"peer":"/10.0.0.11:52120","path":null}
{"ts":1793960230000,"start":1793960200000,"dn":"dn3.example.com","op_id":"c04d88-8","user":"carol","op":"read","role":"client","locality":"remote","bpid":"BP-1","block_id":1073741901,"file_block_id":1073741901,"bytes":100663296,"est_bytes":0,"dur_ms":30000,"last":false,"peer":"/10.0.0.22:43120","path":null}
{"ts":1793960240500,"start":1793960200000,"dn":"dn3.example.com","op_id":"c04d88-8","user":"carol","op":"read","role":"client","locality":"remote","bpid":"BP-1","block_id":1073741901,"file_block_id":1073741901,"bytes":33554432,"est_bytes":0,"dur_ms":40500,"last":true,"peer":"/10.0.0.22:43120","path":null}
```

Как читать строки:

- `op_id` — `<6 hex>-<n>`: префикс случаен для процесса DataNode (`a1f3c2` на dn1, `7be019` на
  dn2, `c04d88` на dn3), `n` — счётчик операций. Операцию в кластере определяет пара (`dn`,
  `op_id`).
- У операции A на каждом DataNode два события с общими `start` и `op_id`. `bytes` — прирост с
  предыдущего события: 80 МиБ, затем 48 МиБ, всего 128 МиБ. `dur_ms` — время от начала
  операции, а не прирост: 30000, затем ~52000.
- Промежуточное событие выдано через 30 с после начала, потому что `flushIntervalSec=30`.
  Операции B, C, D, F и G короче 30 с, поэтому у них одно событие с `last: true`.
- Обычное чтение `carol` (E) устроено так же, как запись A: DataNode сам отправляет байты
  клиенту и считает их (полезная нагрузка, без контрольных сумм). Через 30 с отдано 96 МиБ,
  к закрытию — ещё 32 МиБ. В отличие от `scr_fds` здесь `bytes` точные, а `est_bytes = 0`.
- `peer` — адрес другой стороны соединения. У реплик это предыдущий DataNode конвейера
  (dn2 видит 10.0.0.11, dn3 видит 10.0.0.12), у `scr_fds` — путь UNIX domain socket.
- HTTP-плечо WebHDFS (`webhdfs_create`) не знает блока: `bpid: null`, `block_id: -1`. Зато у
  него есть `path` — путь из HTTP-запроса, здесь временное имя `._COPYING_`. Блок пишет
  встроенный в dn1 DFSClient; эта запись видна как `write`/`webhdfs_proxy`.
- `locality`: `alice` на dn1 подключилась с того же IP, на который пришло соединение, —
  `local`. HTTP-клиент `etl` с 10.0.0.50 — `remote`. `bob` подключился через domain socket —
  `local`. `radcom` в F пишет с dn2 на dn2 — `local`, в G с 10.0.0.23 на dn1 — `remote`.

## Шаг 2. `events` (CompactEvents)

`CompactEvents --dt 2026-11-06` читает все `raw/dt=2026-11-06/dn=*/*.jsonl` и пишет их в
типизированный ORC `<root>/events/dt=2026-11-06` без изменения значений. Здесь строки
отсортированы по `ts` для удобства; сама джоба порядок не задаёт.

| # | ts | start | dur_ms | dn | op_id | user | op | role | locality | bpid | block_id | file_block_id | bytes | est_bytes | last | peer | path |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 C | 10:15:39.930 | 10:15:36.220 | 3710 | dn3 | c04d88-6 | etl | write | pipeline | remote | `BP-1` | 1073741955 | 1073741955 | 10485760 | 0 | true | `/10.0.0.12:38900` | NULL |
| 2 C | 10:15:39.940 | 10:15:36.210 | 3730 | dn2 | 7be019-41 | etl | write | pipeline | remote | `BP-1` | 1073741955 | 1073741955 | 10485760 | 0 | true | `/10.0.0.11:47790` | NULL |
| 3 C | 10:15:39.950 | 10:15:36.200 | 3750 | dn1 | a1f3c2-19 | etl | write | webhdfs_proxy | local | `BP-1` | 1073741955 | 1073741955 | 10485760 | 0 | true | `/10.0.0.11:51310` | NULL |
| 4 C | 10:15:40.000 | 10:15:36.000 | 4000 | dn1 | a1f3c2-18 | etl | webhdfs_create | client | remote | NULL | -1 | -1 | 10485760 | 0 | true | `/10.0.0.50:41234` | `/landing/crm/clients.csv._COPYING_` |
| 5 A | 10:15:50.000 | 10:15:20.000 | 30000 | dn1 | a1f3c2-17 | alice | write | client | local | `BP-1` | 1073741901 | 1073741901 | 83886080 | 0 | false | `/10.0.0.11:51234` | NULL |
| 6 A | 10:15:50.010 | 10:15:20.010 | 30000 | dn2 | 7be019-40 | alice | write | pipeline | remote | `BP-1` | 1073741901 | 1073741901 | 83886080 | 0 | false | `/10.0.0.11:47710` | NULL |
| 7 A | 10:15:50.020 | 10:15:20.020 | 30000 | dn3 | c04d88-5 | alice | write | pipeline | remote | `BP-1` | 1073741901 | 1073741901 | 83886080 | 0 | false | `/10.0.0.12:38822` | NULL |
| 8 D | 10:15:54.980 | 10:15:54.020 | 960 | dn2 | 7be019-42 | alice | write | pipeline | remote | `BP-1` | 1073742010 | 1073742010 | 2097152 | 0 | true | `/10.0.0.11:47850` | NULL |
| 9 D | 10:15:54.990 | 10:15:54.010 | 980 | dn1 | a1f3c2-20 | alice | write | pipeline | remote | `BP-1` | 1073742010 | 1073742010 | 2097152 | 0 | true | `/10.0.0.13:52004` | NULL |
| 10 D | 10:15:55.000 | 10:15:54.000 | 1000 | dn3 | c04d88-7 | alice | write | client | remote | `BP-1` | 1073742010 | 1073742010 | 2097152 | 0 | true | `/10.0.0.21:60112` | NULL |
| 11 A | 10:16:11.980 | 10:15:20.020 | 51960 | dn3 | c04d88-5 | alice | write | pipeline | remote | `BP-1` | 1073741901 | 1073741901 | 50331648 | 0 | true | `/10.0.0.12:38822` | NULL |
| 12 A | 10:16:11.990 | 10:15:20.010 | 51980 | dn2 | 7be019-40 | alice | write | pipeline | remote | `BP-1` | 1073741901 | 1073741901 | 50331648 | 0 | true | `/10.0.0.11:47710` | NULL |
| 13 A | 10:16:12.000 | 10:15:20.000 | 52000 | dn1 | a1f3c2-17 | alice | write | client | local | `BP-1` | 1073741901 | 1073741901 | 50331648 | 0 | true | `/10.0.0.11:51234` | NULL |
| 14 B | 10:16:30.002 | 10:16:30.000 | 2 | dn2 | 7be019-43 | bob | scr_fds | client | local | `BP-1` | 1073741901 | 1073741901 | 0 | 134217728 | true | `unix:/var/lib/hadoop-hdfs/dn_socket` | NULL |
| 15 F | 10:16:56.980 | 10:16:45.020 | 11960 | dn1 | a1f3c2-21 | radcom | write | pipeline | remote | `BP-1` | 1073742100 | 1073742100 | 67108864 | 0 | true | `/10.0.0.13:52080` | NULL |
| 16 F | 10:16:56.990 | 10:16:45.010 | 11980 | dn3 | c04d88-9 | radcom | write | pipeline | remote | `BP-1` | 1073742100 | 1073742100 | 67108864 | 0 | true | `/10.0.0.12:38950` | NULL |
| 17 F | 10:16:57.000 | 10:16:45.000 | 12000 | dn2 | 7be019-44 | radcom | write | client | local | `BP-1` | 1073742100 | 1073742100 | 67108864 | 0 | true | `/10.0.0.12:52310` | NULL |
| 18 G | 10:17:08.980 | 10:17:05.020 | 3960 | dn2 | 7be019-45 | radcom | write | pipeline | remote | `BP-1` | 1073742120 | 1073742120 | 16777216 | 0 | true | `/10.0.0.13:38990` | NULL |
| 19 G | 10:17:08.990 | 10:17:05.010 | 3980 | dn3 | c04d88-10 | radcom | write | pipeline | remote | `BP-1` | 1073742120 | 1073742120 | 16777216 | 0 | true | `/10.0.0.11:52120` | NULL |
| 20 G | 10:17:09.000 | 10:17:05.000 | 4000 | dn1 | a1f3c2-22 | radcom | write | client | remote | `BP-1` | 1073742120 | 1073742120 | 16777216 | 0 | true | `/10.0.0.23:51522` | NULL |
| 21 E | 10:17:10.000 | 10:16:40.000 | 30000 | dn3 | c04d88-8 | carol | read | client | remote | `BP-1` | 1073741901 | 1073741901 | 100663296 | 0 | false | `/10.0.0.22:43120` | NULL |
| 22 E | 10:17:20.500 | 10:16:40.000 | 40500 | dn3 | c04d88-8 | carol | read | client | remote | `BP-1` | 1073741901 | 1073741901 | 33554432 | 0 | true | `/10.0.0.22:43120` | NULL |

Пояснения:

- 22 события — это 18 операций: у операции A на каждом DataNode и у чтения E по два события.
- Первой в конвейере закрывается реплика на последнем DataNode, поэтому события одной записи
  идут от последнего DataNode конвейера к первому: 1–3, 8–10, 11–13, 15–17, 18–20.
- `file_block_id` равен `block_id`: блоки не erasure-coded. Для striped-блоков в
  `file_block_id` был бы id группы блоков, по которому `ResolvePaths` ищет файл.
- `scr_fds` (событие 14) — выдача файловых дескрипторов для short-circuit чтения. Читает
  клиент сам с локального диска, поэтому `bytes = 0`, в `est_bytes` — длина блока, а
  `dur_ms = 2` — время выдачи дескрипторов, а не чтения.

## Шаг 3. Модель fsimage (block-mapper)

`daily-report.sh` после полуночи скачивает checkpoint NameNode и запускает block-mapper,
результат лежит в `<root>/fsimage/dt=2026-11-07/`. Образ берётся за следующий день: файлы,
созданные 2026-11-06, есть только в checkpoint'е после конца этого дня. Нужные для сценария
строки:

`fs_inode` (`type`: `F` — файл, `D` — каталог, `S` — symlink; у корня имя пустое):

| inode_id | type | name |
|---|---|---|
| 16385 | D | `` |
| 16386 | D | `warehouse` |
| 16390 | D | `tablespace` |
| 16391 | D | `managed` |
| 16392 | D | `hive` |
| 16393 | D | `external` |
| 16394 | D | `hive` |
| 16400 | D | `sales.db` |
| 16410 | D | `orders` |
| 16415 | D | `dt=2026-11-05` |
| 16418 | D | `delta_0000012_0000012` |
| 16420 | F | `bucket_00000` |
| 16500 | D | `landing` |
| 16505 | D | `crm` |
| 16512 | F | `clients.csv` |
| 16600 | D | `radcom.db` |
| 16610 | D | `cdr` |
| 16620 | D | `gtpu` |
| 16621 | D | `dt=2026-11-06` |
| 16622 | D | `hour=10` |
| 16625 | F | `000000_0` |
| 16630 | D | `s1ap` |
| 16631 | D | `dt=2026-11-06` |
| 16632 | D | `hour=10` |
| 16635 | F | `000000_0` |

`fs_edge` (текущий родитель):

| child_id | parent_id |
|---|---|
| 16386 | 16385 |
| 16390 | 16386 |
| 16391 | 16390 |
| 16392 | 16391 |
| 16393 | 16390 |
| 16394 | 16393 |
| 16400 | 16392 |
| 16410 | 16400 |
| 16415 | 16410 |
| 16418 | 16415 |
| 16420 | 16418 |
| 16500 | 16385 |
| 16505 | 16500 |
| 16512 | 16505 |
| 16600 | 16394 |
| 16610 | 16600 |
| 16620 | 16610 |
| 16621 | 16620 |
| 16622 | 16621 |
| 16625 | 16622 |
| 16630 | 16610 |
| 16631 | 16630 |
| 16632 | 16631 |
| 16635 | 16632 |

`fs_block_inode`:

| block_id | inode_id |
|---|---|
| 1073741901 | 16420 |
| 1073741955 | 16512 |
| 1073742100 | 16625 |
| 1073742120 | 16635 |

Блока `1073742010` в образе нет: файл в `/tmp/hive/alice/…` удалили до checkpoint'а.
`clients.csv` в образе уже под конечным именем: `hdfs dfs -put` переименовал
`clients.csv._COPYING_` после записи. Партиции `dt=2026-11-06/hour=10` таблиц `gtpu` и `s1ap`
созданы 2026-11-06 и есть в образе, потому что он снят после конца этого дня.

`fs_meta`:

| txid | image_file | parsed_at |
|---|---|---|
| 4815162 | `fsimage_0000000000004815162` | 1794018720000 |

## Шаг 4. `event_path` (ResolvePaths)

`ResolvePaths --dt 2026-11-06 --image-dt 2026-11-07` делает следующее:

1. Берёт `file_block_id` событий дня, кроме `-1`, и находит по `fs_block_inode` их
   `inode_id`.
2. Поднимается по `fs_edge` до корня (16385) и склеивает имена из `fs_inode` в путь.
3. Заполняет `path`: путь из события, если он есть (HTTP-плечо WebHDFS), иначе путь из образа,
   иначе `?`.
4. Вычисляет `table_dir` из `path`: отбрасывает имя файла и обрезает на первом компоненте
   вида `key=value`, `.x`, `_x`, `base_N`, `delta_N_N` или `delete_delta_N_N`.
5. Вычисляет `peer_ip` из `peer`: адрес без порта и без имени хоста перед `/`. Только для
   `locality = remote`; у локальных событий и domain socket — NULL.

Все колонки `events` переносятся без изменений, кроме `path`; добавляются `inode_id`,
`table_dir` и `peer_ip`. Колонки `start`, `bpid` и `block_id` ниже не показаны: они такие же,
как в шаге 2 у строки с тем же `#`.

| # | ts | dn | op_id | user | op | role | locality | file_block_id | dur_ms | bytes | est_bytes | last | peer | path | inode_id | table_dir | peer_ip |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 C | 10:15:39.930 | dn3 | c04d88-6 | etl | write | pipeline | remote | 1073741955 | 3710 | 10485760 | 0 | true | `/10.0.0.12:38900` | `/landing/crm/clients.csv` | 16512 | `/landing/crm` | 10.0.0.12 |
| 2 C | 10:15:39.940 | dn2 | 7be019-41 | etl | write | pipeline | remote | 1073741955 | 3730 | 10485760 | 0 | true | `/10.0.0.11:47790` | `/landing/crm/clients.csv` | 16512 | `/landing/crm` | 10.0.0.11 |
| 3 C | 10:15:39.950 | dn1 | a1f3c2-19 | etl | write | webhdfs_proxy | local | 1073741955 | 3750 | 10485760 | 0 | true | `/10.0.0.11:51310` | `/landing/crm/clients.csv` | 16512 | `/landing/crm` | NULL |
| 4 C | 10:15:40.000 | dn1 | a1f3c2-18 | etl | webhdfs_create | client | remote | -1 | 4000 | 10485760 | 0 | true | `/10.0.0.50:41234` | `/landing/crm/clients.csv._COPYING_` | NULL | `/landing/crm` | 10.0.0.50 |
| 5 A | 10:15:50.000 | dn1 | a1f3c2-17 | alice | write | client | local | 1073741901 | 30000 | 83886080 | 0 | false | `/10.0.0.11:51234` | `/warehouse/tablespace/managed/hive/sales.db/orders/dt=2026-11-05/delta_0000012_0000012/bucket_00000` | 16420 | `/warehouse/tablespace/managed/hive/sales.db/orders` | NULL |
| 6 A | 10:15:50.010 | dn2 | 7be019-40 | alice | write | pipeline | remote | 1073741901 | 30000 | 83886080 | 0 | false | `/10.0.0.11:47710` | `/warehouse/tablespace/managed/hive/sales.db/orders/dt=2026-11-05/delta_0000012_0000012/bucket_00000` | 16420 | `/warehouse/tablespace/managed/hive/sales.db/orders` | 10.0.0.11 |
| 7 A | 10:15:50.020 | dn3 | c04d88-5 | alice | write | pipeline | remote | 1073741901 | 30000 | 83886080 | 0 | false | `/10.0.0.12:38822` | `/warehouse/tablespace/managed/hive/sales.db/orders/dt=2026-11-05/delta_0000012_0000012/bucket_00000` | 16420 | `/warehouse/tablespace/managed/hive/sales.db/orders` | 10.0.0.12 |
| 8 D | 10:15:54.980 | dn2 | 7be019-42 | alice | write | pipeline | remote | 1073742010 | 960 | 2097152 | 0 | true | `/10.0.0.11:47850` | `?` | NULL | `?` | 10.0.0.11 |
| 9 D | 10:15:54.990 | dn1 | a1f3c2-20 | alice | write | pipeline | remote | 1073742010 | 980 | 2097152 | 0 | true | `/10.0.0.13:52004` | `?` | NULL | `?` | 10.0.0.13 |
| 10 D | 10:15:55.000 | dn3 | c04d88-7 | alice | write | client | remote | 1073742010 | 1000 | 2097152 | 0 | true | `/10.0.0.21:60112` | `?` | NULL | `?` | 10.0.0.21 |
| 11 A | 10:16:11.980 | dn3 | c04d88-5 | alice | write | pipeline | remote | 1073741901 | 51960 | 50331648 | 0 | true | `/10.0.0.12:38822` | `/warehouse/tablespace/managed/hive/sales.db/orders/dt=2026-11-05/delta_0000012_0000012/bucket_00000` | 16420 | `/warehouse/tablespace/managed/hive/sales.db/orders` | 10.0.0.12 |
| 12 A | 10:16:11.990 | dn2 | 7be019-40 | alice | write | pipeline | remote | 1073741901 | 51980 | 50331648 | 0 | true | `/10.0.0.11:47710` | `/warehouse/tablespace/managed/hive/sales.db/orders/dt=2026-11-05/delta_0000012_0000012/bucket_00000` | 16420 | `/warehouse/tablespace/managed/hive/sales.db/orders` | 10.0.0.11 |
| 13 A | 10:16:12.000 | dn1 | a1f3c2-17 | alice | write | client | local | 1073741901 | 52000 | 50331648 | 0 | true | `/10.0.0.11:51234` | `/warehouse/tablespace/managed/hive/sales.db/orders/dt=2026-11-05/delta_0000012_0000012/bucket_00000` | 16420 | `/warehouse/tablespace/managed/hive/sales.db/orders` | NULL |
| 14 B | 10:16:30.002 | dn2 | 7be019-43 | bob | scr_fds | client | local | 1073741901 | 2 | 0 | 134217728 | true | `unix:/var/lib/hadoop-hdfs/dn_socket` | `/warehouse/tablespace/managed/hive/sales.db/orders/dt=2026-11-05/delta_0000012_0000012/bucket_00000` | 16420 | `/warehouse/tablespace/managed/hive/sales.db/orders` | NULL |
| 15 F | 10:16:56.980 | dn1 | a1f3c2-21 | radcom | write | pipeline | remote | 1073742100 | 11960 | 67108864 | 0 | true | `/10.0.0.13:52080` | `/warehouse/tablespace/external/hive/radcom.db/cdr/gtpu/dt=2026-11-06/hour=10/000000_0` | 16625 | `/warehouse/tablespace/external/hive/radcom.db/cdr/gtpu` | 10.0.0.13 |
| 16 F | 10:16:56.990 | dn3 | c04d88-9 | radcom | write | pipeline | remote | 1073742100 | 11980 | 67108864 | 0 | true | `/10.0.0.12:38950` | `/warehouse/tablespace/external/hive/radcom.db/cdr/gtpu/dt=2026-11-06/hour=10/000000_0` | 16625 | `/warehouse/tablespace/external/hive/radcom.db/cdr/gtpu` | 10.0.0.12 |
| 17 F | 10:16:57.000 | dn2 | 7be019-44 | radcom | write | client | local | 1073742100 | 12000 | 67108864 | 0 | true | `/10.0.0.12:52310` | `/warehouse/tablespace/external/hive/radcom.db/cdr/gtpu/dt=2026-11-06/hour=10/000000_0` | 16625 | `/warehouse/tablespace/external/hive/radcom.db/cdr/gtpu` | NULL |
| 18 G | 10:17:08.980 | dn2 | 7be019-45 | radcom | write | pipeline | remote | 1073742120 | 3960 | 16777216 | 0 | true | `/10.0.0.13:38990` | `/warehouse/tablespace/external/hive/radcom.db/cdr/s1ap/dt=2026-11-06/hour=10/000000_0` | 16635 | `/warehouse/tablespace/external/hive/radcom.db/cdr/s1ap` | 10.0.0.13 |
| 19 G | 10:17:08.990 | dn3 | c04d88-10 | radcom | write | pipeline | remote | 1073742120 | 3980 | 16777216 | 0 | true | `/10.0.0.11:52120` | `/warehouse/tablespace/external/hive/radcom.db/cdr/s1ap/dt=2026-11-06/hour=10/000000_0` | 16635 | `/warehouse/tablespace/external/hive/radcom.db/cdr/s1ap` | 10.0.0.11 |
| 20 G | 10:17:09.000 | dn1 | a1f3c2-22 | radcom | write | client | remote | 1073742120 | 4000 | 16777216 | 0 | true | `/10.0.0.23:51522` | `/warehouse/tablespace/external/hive/radcom.db/cdr/s1ap/dt=2026-11-06/hour=10/000000_0` | 16635 | `/warehouse/tablespace/external/hive/radcom.db/cdr/s1ap` | 10.0.0.23 |
| 21 E | 10:17:10.000 | dn3 | c04d88-8 | carol | read | client | remote | 1073741901 | 30000 | 100663296 | 0 | false | `/10.0.0.22:43120` | `/warehouse/tablespace/managed/hive/sales.db/orders/dt=2026-11-05/delta_0000012_0000012/bucket_00000` | 16420 | `/warehouse/tablespace/managed/hive/sales.db/orders` | 10.0.0.22 |
| 22 E | 10:17:20.500 | dn3 | c04d88-8 | carol | read | client | remote | 1073741901 | 40500 | 33554432 | 0 | true | `/10.0.0.22:43120` | `/warehouse/tablespace/managed/hive/sales.db/orders/dt=2026-11-05/delta_0000012_0000012/bucket_00000` | 16420 | `/warehouse/tablespace/managed/hive/sales.db/orders` | 10.0.0.22 |

Пояснения:

- Событие 4 (HTTP-плечо WebHDFS) не связано с блоком (`file_block_id = -1`), поэтому
  `inode_id = NULL`, а `path` остаётся из события, с временным именем `._COPYING_`. События
  1–3 той же загрузки получают конечное имя из образа по блоку. `table_dir` у всех четырёх
  одинаковый, потому что имя файла отбрасывается.
- Для `orders` обрезка происходит на `dt=2026-11-05`. Если бы таблица была без партиций,
  обрезка была бы на `delta_0000012_0000012`, и `table_dir` получился бы тот же.
- События 8–10 получили `?`: блока нет в `fs_block_inode`, а в событиях нет `path`. Итоговая
  строка `ResolvePaths` для этого дня: `events=22 unresolved=3`.
- `peer_ip` показывает, с какого адреса пришли данные. У клиентских событий это узел клиента
  (10.0.0.50 у `etl`, 10.0.0.21 и 10.0.0.23 у Hive-задач, 10.0.0.22 у `carol`), у реплик —
  предыдущий DataNode конвейера. У событий 3, 5, 13 и 17 клиент на том же узле, у события 14
  domain socket, поэтому там NULL.
- `cdr/gtpu` и `cdr/s1ap` получают разные `table_dir`: обрезка идёт на первом компоненте
  `key=value` (`dt=2026-11-06`), а `gtpu` и `s1ap` стоят выше него. `TableDir` не обращается к
  Hive metastore, поэтому результат тот же, даже если в metastore это одна таблица `cdr` с
  `LOCATION` `/warehouse/tablespace/external/hive/radcom.db/cdr`, а `gtpu` и `s1ap` — её
  подкаталоги.

## Шаг 5. `traffic_minute` (ResolvePaths)

`event_path` группируется по `minute, user, op, role, locality, table_dir`. `minute` —
начало минуты `ts` в epoch мс; `bytes` и `est_bytes` суммируются; `ops` — число событий с
`last = true`. Колонка «строки» не входит в таблицу: она показывает, из каких событий `#`
собрана группа.

| minute | user | op | role | locality | table_dir | bytes | est_bytes | ops | строки |
|---|---|---|---|---|---|---|---|---|---|
| 1793960100000 (10:15) | alice | write | client | local | `/warehouse/tablespace/managed/hive/sales.db/orders` | 83886080 | 0 | 0 | 5 |
| 1793960100000 (10:15) | alice | write | client | remote | `?` | 2097152 | 0 | 1 | 10 |
| 1793960100000 (10:15) | alice | write | pipeline | remote | `/warehouse/tablespace/managed/hive/sales.db/orders` | 167772160 | 0 | 0 | 6, 7 |
| 1793960100000 (10:15) | alice | write | pipeline | remote | `?` | 4194304 | 0 | 2 | 8, 9 |
| 1793960100000 (10:15) | etl | webhdfs_create | client | remote | `/landing/crm` | 10485760 | 0 | 1 | 4 |
| 1793960100000 (10:15) | etl | write | pipeline | remote | `/landing/crm` | 20971520 | 0 | 2 | 1, 2 |
| 1793960100000 (10:15) | etl | write | webhdfs_proxy | local | `/landing/crm` | 10485760 | 0 | 1 | 3 |
| 1793960160000 (10:16) | alice | write | client | local | `/warehouse/tablespace/managed/hive/sales.db/orders` | 50331648 | 0 | 1 | 13 |
| 1793960160000 (10:16) | alice | write | pipeline | remote | `/warehouse/tablespace/managed/hive/sales.db/orders` | 100663296 | 0 | 2 | 11, 12 |
| 1793960160000 (10:16) | bob | scr_fds | client | local | `/warehouse/tablespace/managed/hive/sales.db/orders` | 0 | 134217728 | 1 | 14 |
| 1793960160000 (10:16) | radcom | write | client | local | `/warehouse/tablespace/external/hive/radcom.db/cdr/gtpu` | 67108864 | 0 | 1 | 17 |
| 1793960160000 (10:16) | radcom | write | pipeline | remote | `/warehouse/tablespace/external/hive/radcom.db/cdr/gtpu` | 134217728 | 0 | 2 | 15, 16 |
| 1793960220000 (10:17) | carol | read | client | remote | `/warehouse/tablespace/managed/hive/sales.db/orders` | 134217728 | 0 | 1 | 21, 22 |
| 1793960220000 (10:17) | radcom | write | client | remote | `/warehouse/tablespace/external/hive/radcom.db/cdr/s1ap` | 16777216 | 0 | 1 | 20 |
| 1793960220000 (10:17) | radcom | write | pipeline | remote | `/warehouse/tablespace/external/hive/radcom.db/cdr/s1ap` | 33554432 | 0 | 2 | 18, 19 |

Пояснения:

- Байты операции A разошлись по двум минутам по времени выдачи событий: 80 МиБ на каждом
  DataNode в 10:15, 48 МиБ в 10:16. Каждая из трёх операций A учтена в `ops` один раз, в
  минуте своего последнего события.
- Оба события чтения E пришлись на 10:17 и попали в одну группу: 128 МиБ, `ops = 1`.
  Блок `orders` прочитан дважды: по сети (`read`, `bytes`) и через short-circuit (`scr_fds`,
  только `est_bytes`).
- Одни и те же 128 МиБ `alice` дают в сумме 384 МиБ по всем ролям: `client` на dn1 и две
  реплики `pipeline`. Загрузка `etl` на 10 МиБ даёт 40 МиБ: HTTP-плечо, `webhdfs_proxy` и две
  реплики.
- Трафик обоих каталогов `cdr` суммируется по префиксу: `table_dir LIKE
  '/warehouse/tablespace/external/hive/radcom.db/cdr/%'`. В 10:16 и 10:17 это группы событий
  15–17 и 18–20.
- `start` и `dur_ms` в `traffic_minute` не переносятся. Скорость отдельной операции
  считается только по `event_path` (см. ниже).

## Запросы к результату

Пропускная способность записи по таблицам за минуту, МиБ/с. Длительность операций здесь не
нужна: делим байты минуты на 60 с. `role = 'client'` оставляет одну копию данных: без реплик
конвейера и без внутреннего плеча WebHDFS:

```sql
SELECT minute, table_dir, SUM(bytes) / 1048576 / 60 AS mib_per_s
FROM hdfs_traffic.traffic_minute
WHERE dt = '2026-11-06'
  AND op IN ('write', 'webhdfs_create', 'webhdfs_append') AND role = 'client'
GROUP BY minute, table_dir
ORDER BY minute, mib_per_s DESC;
```

Результат на данных примера:

| minute | table_dir | mib_per_s | строки |
|---|---|---|---|
| 10:15 | `/warehouse/tablespace/managed/hive/sales.db/orders` | 1.333 | 5 |
| 10:15 | `/landing/crm` | 0.167 | 4 |
| 10:15 | `?` | 0.033 | 10 |
| 10:16 | `/warehouse/tablespace/external/hive/radcom.db/cdr/gtpu` | 1.067 | 17 |
| 10:16 | `/warehouse/tablespace/managed/hive/sales.db/orders` | 0.8 | 13 |
| 10:17 | `/warehouse/tablespace/external/hive/radcom.db/cdr/s1ap` | 0.267 | 20 |

Пропускная способность чтения по таблицам за минуту, МиБ/с. Short-circuit чтения сюда не
попадают: у `scr_fds` байты есть только в `est_bytes`:

```sql
SELECT minute, table_dir, SUM(bytes) / 1048576 / 60 AS mib_per_s
FROM hdfs_traffic.traffic_minute
WHERE dt = '2026-11-06'
  AND op IN ('read', 'webhdfs_read') AND role = 'client'
GROUP BY minute, table_dir
ORDER BY minute, mib_per_s DESC;
```

Результат на данных примера:

| minute | table_dir | mib_per_s | строки |
|---|---|---|---|
| 10:17 | `/warehouse/tablespace/managed/hive/sales.db/orders` | 2.133 | 21, 22 |

Скорость каждой завершённой операции, МиБ/с. Длительность — `MAX(dur_ms)`, потому что
`dur_ms` нарастает внутри операции:

```sql
SELECT dn, op_id, `user`, op, role, table_dir,
       SUM(bytes)                                    AS bytes,
       MAX(dur_ms)                                   AS dur_ms,
       SUM(bytes) / 1048576 / (MAX(dur_ms) / 1000)   AS mib_per_s
FROM hdfs_traffic.event_path
WHERE dt = '2026-11-06'
GROUP BY dn, op_id, `user`, op, role, table_dir
HAVING MAX(CAST(`last` AS INT)) = 1 AND MAX(dur_ms) > 0;
```

Результат на данных примера (18 операций):

| dn | op_id | user | op | role | table_dir | bytes | dur_ms | mib_per_s |
|---|---|---|---|---|---|---|---|---|
| dn3 | c04d88-6 | etl | write | pipeline | `/landing/crm` | 10485760 | 3710 | 2.695 |
| dn2 | 7be019-41 | etl | write | pipeline | `/landing/crm` | 10485760 | 3730 | 2.681 |
| dn1 | a1f3c2-19 | etl | write | webhdfs_proxy | `/landing/crm` | 10485760 | 3750 | 2.667 |
| dn1 | a1f3c2-18 | etl | webhdfs_create | client | `/landing/crm` | 10485760 | 4000 | 2.5 |
| dn1 | a1f3c2-17 | alice | write | client | `/warehouse/tablespace/managed/hive/sales.db/orders` | 134217728 | 52000 | 2.462 |
| dn2 | 7be019-40 | alice | write | pipeline | `/warehouse/tablespace/managed/hive/sales.db/orders` | 134217728 | 51980 | 2.462 |
| dn3 | c04d88-5 | alice | write | pipeline | `/warehouse/tablespace/managed/hive/sales.db/orders` | 134217728 | 51960 | 2.463 |
| dn2 | 7be019-42 | alice | write | pipeline | `?` | 2097152 | 960 | 2.083 |
| dn1 | a1f3c2-20 | alice | write | pipeline | `?` | 2097152 | 980 | 2.041 |
| dn3 | c04d88-7 | alice | write | client | `?` | 2097152 | 1000 | 2.0 |
| dn2 | 7be019-43 | bob | scr_fds | client | `/warehouse/tablespace/managed/hive/sales.db/orders` | 0 | 2 | 0.0 |
| dn1 | a1f3c2-21 | radcom | write | pipeline | `/warehouse/tablespace/external/hive/radcom.db/cdr/gtpu` | 67108864 | 11960 | 5.351 |
| dn3 | c04d88-9 | radcom | write | pipeline | `/warehouse/tablespace/external/hive/radcom.db/cdr/gtpu` | 67108864 | 11980 | 5.342 |
| dn2 | 7be019-44 | radcom | write | client | `/warehouse/tablespace/external/hive/radcom.db/cdr/gtpu` | 67108864 | 12000 | 5.333 |
| dn2 | 7be019-45 | radcom | write | pipeline | `/warehouse/tablespace/external/hive/radcom.db/cdr/s1ap` | 16777216 | 3960 | 4.04 |
| dn3 | c04d88-10 | radcom | write | pipeline | `/warehouse/tablespace/external/hive/radcom.db/cdr/s1ap` | 16777216 | 3980 | 4.02 |
| dn1 | a1f3c2-22 | radcom | write | client | `/warehouse/tablespace/external/hive/radcom.db/cdr/s1ap` | 16777216 | 4000 | 4.0 |
| dn3 | c04d88-8 | carol | read | client | `/warehouse/tablespace/managed/hive/sales.db/orders` | 134217728 | 40500 | 3.16 |

## Известные ограничения расчётов

- `dur_ms` — время по часам с начала операции на DataNode. В него входят простои: медленный
  клиент, ожидание ack от следующего DataNode конвейера, паузы между чтениями. `bytes / dur_ms`
  — эффективная скорость операции, а не скорость сети.
- Для `scr_fds` скорость не считается: `bytes = 0`, а `dur_ms` — время выдачи дескрипторов.
- Операция, которая шла через полночь UTC, разделится между двумя партициями `dt`: закрытый
  файл событий несёт дату своих событий, и `upload-events.sh` кладёт его в партицию этой
  даты. Запрос скорости выше оставит часть с `last = true`, а `MAX(dur_ms)` у неё — полное
  время операции, поэтому скорость будет занижена.
- Сумма `bytes` по всем ролям — физический трафик DataNode, а не объём данных: запись с
  репликацией 3 учитывается трижды, `put` через WebHDFS с репликацией 3 — четыре раза.
