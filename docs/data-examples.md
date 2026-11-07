# hdfs-traffic-agent: пример данных от событий до отчёта

[English version](data-examples.en.md) · [Общее описание](../README.md)

Документ показывает на одном сценарии, какие строки получаются на каждом шаге: JSONL агента,
`events`, таблицы block-mapper, `event_path` и `traffic_minute`. Он для тех, кто пишет
запросы к отчётам и хочет понимать, откуда берётся каждое число. Описание колонок — в
[README](../README.md#отчёты-spark-jobs).

Данные выдуманы и не получены прогоном на стенде, но собраны по коду `EventJson`,
`CompactEvents`, `ResolvePaths`, `PathResolver` и `TableDir`. Значения согласованы между
таблицами: суммы в `traffic_minute` считаются из строк `event_path` ниже.

Обозначения:

| Обозначение | Значение |
|---|---|
| `dn1`, `dn2`, `dn3` | DataNode `dn1.example.com`, `dn2.example.com`, `dn3.example.com` |
| `…/orders` | `/warehouse/tablespace/managed/hive/sales.db/orders` |
| время `10:15:50.000` | UTC, 2026-11-06; в таблицах хранится epoch в мс (`1793960150000`) |
| `<root>` | корень данных отчётов, по умолчанию `/data/hdfs-traffic` |

## Сценарий

День `2026-11-06`, `flushIntervalSec=30`.

| # | Что происходит | Ожидаемый результат |
|---|---|---|
| A | `alice` (Hive-джоба на узле dn1) пишет блок `1073741901` размером 128 МиБ в файл `…/orders/dt=2026-11-05/delta_0000012_0000012/bucket_00000`. Конвейер записи: dn1 → dn2 → dn3. Операция идёт 52 с | по два события на каждом DataNode, `table_dir = …/orders` |
| B | `bob` на узле dn2 читает этот файл через short-circuit | одно событие `scr_fds` с `bytes = 0` |
| C | `etl` выполняет `hdfs dfs -put` через WebHDFS в `/landing/crm/clients.csv` (10 МиБ, блок `1073741955`) | два события на dn1: HTTP-плечо и внутреннее плечо `webhdfs_proxy` |
| D | `alice` пишет 2 МиБ (блок `1073742010`) во временный каталог `/tmp/hive/alice/…`, который удаляется до checkpoint'а NameNode | путь не находится, `path = ?` |

События реплик для C и D опущены, чтобы таблицы были короче.

## Шаг 1. JSONL агента

Агент на каждом DataNode пишет локальный файл, `deploy/upload-events.sh` кладёт его в
`<root>/raw/dt=2026-11-06/dn=<host>/`. Строки операции A на dn1:

```json
{"ts":1793960150000,"start":1793960120000,"dn":"dn1.example.com","op_id":"a1f3c2-17","user":"alice","op":"write","role":"client","locality":"local","bpid":"BP-1","block_id":1073741901,"file_block_id":1073741901,"bytes":83886080,"est_bytes":0,"dur_ms":30000,"last":false,"peer":"/10.0.0.11:51234","path":null}
{"ts":1793960172000,"start":1793960120000,"dn":"dn1.example.com","op_id":"a1f3c2-17","user":"alice","op":"write","role":"client","locality":"local","bpid":"BP-1","block_id":1073741901,"file_block_id":1073741901,"bytes":50331648,"est_bytes":0,"dur_ms":52000,"last":true,"peer":"/10.0.0.11:51234","path":null}
```

Что видно по этим двум строкам:

- `start` и `op_id` у них общие: это одна операция.
- `bytes` — прирост с предыдущего события: 80 МиБ к 10:15:50 и ещё 48 МиБ к 10:16:12, всего
  128 МиБ.
- `dur_ms` — время от начала операции, а не прирост: 30000, затем 52000. Длительность
  операции — `dur_ms` последнего события.
- Первое событие выдано через 30 с после начала, потому что `flushIntervalSec=30`. Без этого
  весь блок пришёл бы одним событием в 10:16:12.

## Шаг 2. `events` (CompactEvents)

`CompactEvents --dt 2026-11-06` читает все `raw/dt=2026-11-06/dn=*/*.jsonl` и пишет их
в типизированный ORC без изменения значений. Колонки `bpid`, `file_block_id` и `peer` ниже
не показаны: `bpid` везде `BP-1`, `file_block_id` равен `block_id` (блоки не erasure-coded).

| ts | start | dur_ms | dn | op_id | user | op | role | locality | block_id | bytes | est_bytes | last | path |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 10:15:39.950 | 10:15:36.200 | 3750 | dn1 | a1f3c2-19 | etl | write | webhdfs_proxy | local | 1073741955 | 10485760 | 0 | true | NULL |
| 10:15:40.000 | 10:15:36.000 | 4000 | dn1 | a1f3c2-18 | etl | webhdfs_create | client | remote | 1073741955 | 10485760 | 0 | true | `/landing/crm/clients.csv._COPYING_` |
| 10:15:50.000 | 10:15:20.000 | 30000 | dn1 | a1f3c2-17 | alice | write | client | local | 1073741901 | 83886080 | 0 | false | NULL |
| 10:15:50.000 | 10:15:20.010 | 29990 | dn2 | 7be019-40 | alice | write | pipeline | remote | 1073741901 | 83886080 | 0 | false | NULL |
| 10:15:50.000 | 10:15:20.020 | 29980 | dn3 | c04d88-5 | alice | write | pipeline | remote | 1073741901 | 83886080 | 0 | false | NULL |
| 10:15:55.000 | 10:15:54.000 | 1000 | dn3 | c04d88-6 | alice | write | client | remote | 1073742010 | 2097152 | 0 | true | NULL |
| 10:16:12.000 | 10:15:20.000 | 52000 | dn1 | a1f3c2-17 | alice | write | client | local | 1073741901 | 50331648 | 0 | true | NULL |
| 10:16:12.000 | 10:15:20.010 | 51990 | dn2 | 7be019-40 | alice | write | pipeline | remote | 1073741901 | 50331648 | 0 | true | NULL |
| 10:16:12.000 | 10:15:20.020 | 51980 | dn3 | c04d88-5 | alice | write | pipeline | remote | 1073741901 | 50331648 | 0 | true | NULL |
| 10:16:30.002 | 10:16:30.000 | 2 | dn2 | 7be019-41 | bob | scr_fds | client | local | 1073741901 | 0 | 134217728 | true | NULL |

Пояснения:

- dn1 видит `alice` как `client`/`local`: клиент работает на том же узле и подключился по
  тому же IP. dn2 и dn3 получают данные от предыдущего DataNode конвейера, поэтому у них
  `pipeline`/`remote`. `bob` подключается через UNIX domain socket, это всегда `local`.
- `op_id` уникален только в пределах DataNode (префикс — случайное число процесса), поэтому
  операцию в кластере определяет пара (`dn`, `op_id`).
- У WebHDFS две записи одной передачи: `webhdfs_create` — HTTP-плечо от клиента к dn1,
  `write`/`webhdfs_proxy` — запись блока встроенным в dn1 DFSClient. Путь есть только у
  HTTP-плеча, и это временное имя `._COPYING_`.
- `scr_fds` — выдача файловых дескрипторов для short-circuit чтения. Читает клиент сам с
  локального диска, поэтому `bytes = 0`, в `est_bytes` — длина блока, а `dur_ms = 2` — время
  выдачи дескрипторов, а не чтения.

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
| 16400 | D | `sales.db` |
| 16410 | D | `orders` |
| 16415 | D | `dt=2026-11-05` |
| 16418 | D | `delta_0000012_0000012` |
| 16420 | F | `bucket_00000` |
| 16500 | D | `landing` |
| 16505 | D | `crm` |
| 16512 | F | `clients.csv` |

`fs_edge` (текущий родитель):

| child_id | parent_id |
|---|---|
| 16386 | 16385 |
| 16390 | 16386 |
| 16391 | 16390 |
| 16392 | 16391 |
| 16400 | 16392 |
| 16410 | 16400 |
| 16415 | 16410 |
| 16418 | 16415 |
| 16420 | 16418 |
| 16500 | 16385 |
| 16505 | 16500 |
| 16512 | 16505 |

`fs_block_inode`:

| block_id | inode_id |
|---|---|
| 1073741901 | 16420 |
| 1073741955 | 16512 |

Блока `1073742010` в образе нет: файл в `/tmp/hive/alice/…` удалили до checkpoint'а.

`fs_meta`:

| txid | image_file | parsed_at |
|---|---|---|
| 4815162 | `fsimage_0000000000004815162` | 1794018720000 |

## Шаг 4. `event_path` (ResolvePaths)

`ResolvePaths --dt 2026-11-06 --image-dt 2026-11-07` делает следующее:

1. Берёт `file_block_id` событий дня и находит по `fs_block_inode` их `inode_id`.
2. Поднимается по `fs_edge` до корня (16385) и склеивает имена из `fs_inode` в путь.
3. Заполняет `path`: путь из события, если он есть (WebHDFS), иначе путь из образа, иначе `?`.
4. Вычисляет `table_dir` из `path`: отбрасывает имя файла и обрезает на первом компоненте
   вида `key=value`, `.x`, `_x`, `base_N`, `delta_N_N` или `delete_delta_N_N`.

Колонки событий переносятся без изменений, в том числе `start` и `dur_ms`. Ниже показаны
только колонки, которые помогают проследить результат:

| ts | dn | user | op | role | dur_ms | bytes | path | inode_id | table_dir |
|---|---|---|---|---|---|---|---|---|---|
| 10:15:39.950 | dn1 | etl | write | webhdfs_proxy | 3750 | 10485760 | `/landing/crm/clients.csv` | 16512 | `/landing/crm` |
| 10:15:40.000 | dn1 | etl | webhdfs_create | client | 4000 | 10485760 | `/landing/crm/clients.csv._COPYING_` | 16512 | `/landing/crm` |
| 10:15:50.000 | dn1 | alice | write | client | 30000 | 83886080 | `…/orders/dt=2026-11-05/delta_0000012_0000012/bucket_00000` | 16420 | `…/orders` |
| 10:15:50.000 | dn2 | alice | write | pipeline | 29990 | 83886080 | то же | 16420 | `…/orders` |
| 10:15:50.000 | dn3 | alice | write | pipeline | 29980 | 83886080 | то же | 16420 | `…/orders` |
| 10:15:55.000 | dn3 | alice | write | client | 1000 | 2097152 | `?` | NULL | `?` |
| 10:16:12.000 | dn1 | alice | write | client | 52000 | 50331648 | то же | 16420 | `…/orders` |
| 10:16:12.000 | dn2 | alice | write | pipeline | 51990 | 50331648 | то же | 16420 | `…/orders` |
| 10:16:12.000 | dn3 | alice | write | pipeline | 51980 | 50331648 | то же | 16420 | `…/orders` |
| 10:16:30.002 | dn2 | bob | scr_fds | client | 2 | 0 | то же | 16420 | `…/orders` |

«то же» — путь `…/orders/dt=2026-11-05/delta_0000012_0000012/bucket_00000`.

Пояснения:

- HTTP-плечо WebHDFS сохраняет путь из события (`._COPYING_`), хотя `inode_id` найден по
  блоку. Внутреннее плечо получает конечное имя из образа. `table_dir` у обоих одинаковый,
  потому что имя файла отбрасывается.
- Для `orders` обрезка происходит на `dt=2026-11-05`. Если бы таблица была без партиций,
  обрезка была бы на `delta_0000012_0000012`, и `table_dir` получился бы тот же.
- Строка D получила `?`: блока нет в `fs_block_inode`, а в событии нет `path`. Итоговая строка
  `ResolvePaths` для этого дня: `events=10 unresolved=1`.

## Шаг 5. `traffic_minute` (ResolvePaths)

`event_path` группируется по `minute, user, op, role, locality, table_dir`. `minute` —
начало минуты `ts` в epoch мс; `bytes` и `est_bytes` суммируются; `ops` — число событий с
`last = true`.

| minute | user | op | role | locality | table_dir | bytes | est_bytes | ops |
|---|---|---|---|---|---|---|---|---|
| 1793960100000 (10:15) | etl | write | webhdfs_proxy | local | `/landing/crm` | 10485760 | 0 | 1 |
| 1793960100000 (10:15) | etl | webhdfs_create | client | remote | `/landing/crm` | 10485760 | 0 | 1 |
| 1793960100000 (10:15) | alice | write | client | local | `…/orders` | 83886080 | 0 | 0 |
| 1793960100000 (10:15) | alice | write | pipeline | remote | `…/orders` | 167772160 | 0 | 0 |
| 1793960100000 (10:15) | alice | write | client | remote | `?` | 2097152 | 0 | 1 |
| 1793960160000 (10:16) | alice | write | client | local | `…/orders` | 50331648 | 0 | 1 |
| 1793960160000 (10:16) | alice | write | pipeline | remote | `…/orders` | 100663296 | 0 | 2 |
| 1793960160000 (10:16) | bob | scr_fds | client | local | `…/orders` | 0 | 134217728 | 1 |

Пояснения:

- Байты операции A разошлись по двум минутам по времени выдачи событий: 80 МиБ в 10:15,
  48 МиБ в 10:16. Операция при этом учтена в `ops` один раз, в минуте последнего события.
- `pipeline` в 10:15 — это 2 × 80 МиБ: события dn2 и dn3 попали в одну группу.
- `start` и `dur_ms` в `traffic_minute` не переносятся. Скорость отдельной операции
  считается только по `event_path` (см. ниже).

## Запросы к результату

Пропускная способность записи по таблицам за минуту, МиБ/с. Длительность операций здесь не
нужна: делим байты минуты на 60 с. `role = 'client'` оставляет одну копию данных без реплик
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

| minute | table_dir | mib_per_s |
|---|---|---|
| 10:15 | `…/orders` | 1.333 |
| 10:15 | `/landing/crm` | 0.167 |
| 10:15 | `?` | 0.033 |
| 10:16 | `…/orders` | 0.8 |

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

Результат на данных примера:

| dn | op_id | user | op | role | table_dir | bytes | dur_ms | mib_per_s |
|---|---|---|---|---|---|---|---|---|
| dn1 | a1f3c2-17 | alice | write | client | `…/orders` | 134217728 | 52000 | 2.462 |
| dn2 | 7be019-40 | alice | write | pipeline | `…/orders` | 134217728 | 51990 | 2.462 |
| dn3 | c04d88-5 | alice | write | pipeline | `…/orders` | 134217728 | 51980 | 2.462 |
| dn1 | a1f3c2-18 | etl | webhdfs_create | client | `/landing/crm` | 10485760 | 4000 | 2.5 |
| dn1 | a1f3c2-19 | etl | write | webhdfs_proxy | `/landing/crm` | 10485760 | 3750 | 2.667 |
| dn3 | c04d88-6 | alice | write | client | `?` | 2097152 | 1000 | 2.0 |
| dn2 | 7be019-41 | bob | scr_fds | client | `…/orders` | 0 | 2 | 0.0 |

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
  репликацией 3 учитывается трижды, `put` через WebHDFS — дважды на первом DataNode.
