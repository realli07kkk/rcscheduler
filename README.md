# rcscheduler

用 Go 实现的单机 rclone 迁移控制器。原生读取目录中的清单，全部校验通过后统一提交，再按全局并发数滚动执行。任务、对象结果和执行历史保存在本地 JSON 中，无数据库依赖。

## 运行条件

- 支持 Linux、macOS。编译需要 Go 1.27.0；使用预编译二进制时不需要 Go 环境。
- 需要独立的 **rclone v1.75.1** 二进制。该版本用于对象统计契约测试，不修改 rclone 源码，也不要求运行机器保存源码。
- 需要明确指定 `rclone.conf`，并已配置源和目标 remote，确保运行账户具有相应的访问权限和网络连接。
- 数据目录使用本地文件系统，由运行服务的用户独占。程序使用文件锁、原子替换及 fsync，不支持多个控制器共享写入或 NFS 数据目录。

官方二进制和校验方法见 [rclone 下载](https://rclone.org/downloads/)。其他版本默认拒绝启动；通过兼容性测试后才使用 `--allow-untested-rclone`。控制器只继承 `RCLONE_CONFIG_*` 凭据配置环境变量，其他 `RCLONE_*` 执行选项被过滤，避免外部过滤器改变迁移清单。

## 编译和启动

```bash
make build
./bin/rcscheduler version

./bin/rcscheduler --data-dir /data/rcscheduler serve \
  --rclone /usr/local/bin/rclone \
  --rclone-config /etc/rclone/rclone.conf
```

默认 HTTP 地址为 `127.0.0.1:8787`。`--listen` 可以修改监听地址。服务会在数据目录生成权限为 0600 的 `client.json`，包含连接地址和 token；相同用户的 CLI 自动读取它。不要将该文件提交到代码仓库。

`--data-dir`、`--json`、`--server`、`--token` 是全局参数，放在子命令之前。可用 `RCSCHEDULER_DATA_DIR` 指定默认数据目录。以下例子假设 `rcscheduler` 已加入 PATH。

每个执行自动启动一个 `rclone copy` 子进程，并创建私有 RC socket。无需另外运行 `rclone rcd`。Unix socket 默认放在 `/tmp/rcs-<uid>-<数据目录摘要>/` 的真实路径下，避免 macOS 的 socket 路径长度限制；可以用 `serve --runtime-dir` 指定短目录。

## 从手动逐份清单迁移切换

如果目前通过替换 `TASK_FILE` 逐份运行 `rclone copy --files-from-raw`，可以将整个目录一次导入，由服务串行接续执行。先按上文启动服务，再配置后续执行参数：

```bash
rcscheduler --data-dir /data/rcscheduler scheduler set \
  --max-running 1 \
  --bwlimit 40M \
  --checkers 64 \
  --transfers 64 \
  --user-agent 'aws-sdk-go-v2/1.41.4' \
  --s3-upload-concurrency 8

rcscheduler --data-dir /data/rcscheduler batch import \
  --id migration-001 \
  --manifest-dir /absolute/path/r-list \
  --source 'source,no_head_object=true:example-bucket' \
  --destination 'destination:example-bucket'

rcscheduler --data-dir /data/rcscheduler batch watch migration-001
rcscheduler --data-dir /data/rcscheduler task list --batch-id migration-001
```

将路径、remote 和桶名替换为自己的配置。清单目录由服务所在机器读取，建议使用绝对路径。服务必须持续运行；Linux 可使用下文的 systemd 部署方式。

每份清单生成一个任务，任务名称保留相对文件名，例如 `1_318_1784720583971.txt`，任务 ID 可从 `task list` 获取。默认读取目录第一层 `*.txt`，整批校验通过后，同批次、同优先级任务按文件名字典序启动。`maxRunning=1` 时全局最多运行一个任务，每个任务限速为完整的 `40M`；`transfers=64`、`checkers=64` 控制该任务内部的对象并发数。

服务自动传入 `--files-from-raw`、`--no-traverse`、`--ignore-existing`、`--disable Copy` 和 `--multi-thread-streams 0`；源地址中的 `no_head_object=true` 原样传给 rclone。每次进程退出后自动补入下一份清单。失败或结果不完整会保留任务状态，后续清单继续执行；可重试错误沿用自动重试策略，等待重试期间也可执行后续清单。

使用 `task watch <taskId>` 查看单任务进度，使用 `task history <taskId>` 查看执行参数、PID 和结果。服务管理子进程，日志按任务和执行次数保存，沿用 JSON 日志与 2 秒采样以支持对象统计和恢复。目录后续新增文件时，使用新的批次 ID 导入，并通过 `--pattern` 或独立目录选择新清单；已提交批次不会重新扫描目录。

## 原生批次导入

先设置任务并发和带宽分配基准：

```bash
rcscheduler --data-dir /data/rcscheduler scheduler set \
  --max-running 4 --bwlimit 20M
```

准备目录中的清单，例如 `task-001.txt`、`task-002.txt`，每行一个对象 key：

```text
images/001.jpg
documents/合同.pdf
archive/file with spaces.zip
```

通过一次命令提交整个目录。以下 `source`、`destination` 和桶名均为占位示例，使用时替换为自己的配置：

```bash
rcscheduler --data-dir /data/rcscheduler batch import \
  --id migration-001 \
  --manifest-dir /data/manifests \
  --source 'source:source-bucket' \
  --destination 'destination:destination-bucket'
```

目录路径由**服务所在机器**解释。CLI 只发送一次请求，扫描、快照、校验和注册任务都由 Go 服务完成。默认读取第一层 `*.txt` 普通文件，按相对文件名排序；`--pattern 'task-*.txt'` 匹配文件名，`--recursive` 读取子目录。不会跟随匹配的符号链接。

CLI 默认等待校验完成，`--no-wait` 可以立即返回导入状态。任意一份清单校验失败，本批次零任务执行，其他已运行批次继续执行：

```bash
rcscheduler --data-dir /data/rcscheduler batch validation migration-001
```

修复源清单后可用同一命令重新导入被拒绝的批次。已经提交的批次不可覆盖；重复请求返回原批次快照，不重新读取目录。新增文件或变更清单应使用新的批次 ID。

校验规则：UTF-8、LF/CRLF、末行可无换行；空行、BOM、NUL、首尾斜杠、超长行及清单内重复对象报错。对象名前后空格、中文、`#` 和 `;` 都按字面保留。不同清单的目标对象互不重叠由清单提供方保证。

单批次最多 10,000 份清单，每份最多 100,000 个对象、128MiB。校验报告保留全部错误总数和逐文件错误数；详细错误最多展示 2,000 条，超过时明确标记 `detailsTruncated`。导入期间文件变化或不可读时拒绝提交。

## 查看与控制

```bash
rcscheduler --data-dir /data/rcscheduler batch show migration-001
rcscheduler --data-dir /data/rcscheduler batch watch migration-001
rcscheduler --data-dir /data/rcscheduler task list --batch-id migration-001
```

批次中的每份清单有独立任务 ID，可从列表获取：

```bash
rcscheduler --data-dir /data/rcscheduler task show <taskId>
rcscheduler --data-dir /data/rcscheduler task watch <taskId>
rcscheduler --data-dir /data/rcscheduler task objects <taskId> --state failed
rcscheduler --data-dir /data/rcscheduler task history <taskId>

rcscheduler --data-dir /data/rcscheduler task pause <taskId>
rcscheduler --data-dir /data/rcscheduler task resume <taskId>
rcscheduler --data-dir /data/rcscheduler task cancel <taskId>
rcscheduler --data-dir /data/rcscheduler task retry <taskId>
rcscheduler --data-dir /data/rcscheduler task priority <taskId> 10
```

优先级越大越先执行，同级按入队顺序执行，不抢占运行任务。停止单任务会等待进程退出后释放名额；超时 30 秒强制终止进程组。暂停全局调度只停止补入新任务：

```bash
rcscheduler --data-dir /data/rcscheduler scheduler pause
rcscheduler --data-dir /data/rcscheduler scheduler resume
```

列表默认每页 100 条，支持 `--offset`、`--limit`（最多 1,000）及 `--state`。全局 `--json` 用于结构化输出。

单清单也可以通过上传提交，文件路径由 CLI 所在机器读取：

```bash
rcscheduler --data-dir /data/rcscheduler task add \
  --id manual-001 \
  --source 'source:source-bucket' \
  --destination 'destination:destination-bucket' \
  --files-from-raw /data/manifests/task-001.txt
```

## 动态参数

默认 `maxRunning=4`、`bandwidthBudget=20M`、`transfers=4`、`checkers=8`。多个导入批次共享全局任务并发数。

```bash
rcscheduler --data-dir /data/rcscheduler scheduler set --bwlimit 40M
rcscheduler --data-dir /data/rcscheduler scheduler show
```

任务进入 `starting` 时，读取最新配置并保存执行快照：

```text
本次限速 = bandwidthBudget / maxRunning
```

20M 调到 40M、并发仍为 4：旧执行保持 5M，新执行使用 10M。重试、暂停恢复和服务重启恢复同样使用最新配置。除法按整数 bytes/s 向下取整，禁止得到 0B/s；`20M` 表示 20MiB/s。

分母始终是配置并发数，即使队列只剩两个任务。降低带宽也不修改旧执行，过渡期间可能超过新配置；增加并发数会补入新任务，降低并发数等待已有任务自然退出。因此带宽配置是**新执行的分配基准**，并非任何时刻的严格总上限。

`--transfers` 和 `--checkers` 也只影响后续执行。运行中的进程不会收到这些配置变更。

`--user-agent` 和 `--s3-upload-concurrency` 同样是全局配置，应用于后续执行，并保存到每次执行的参数快照中，包括重试、暂停恢复和服务重启后的执行：

```bash
rcscheduler --data-dir /data/rcscheduler scheduler set \
  --user-agent 'aws-sdk-go-v2/1.41.4' --s3-upload-concurrency 8

# 清除覆盖值，让 rclone 使用自身配置或默认值。
rcscheduler --data-dir /data/rcscheduler scheduler set \
  --user-agent '' --s3-upload-concurrency 0
```

User-Agent 默认为空，不允许控制字符；上传并发默认 `0`，正整数表示显式覆盖，负数报错。空字符串和 `0` 表示启动时省略对应 rclone 参数。未指定的配置字段保持原值。旧版数据缺少这两个字段时使用上述默认值，JSON 文档版本仍为 `1`。

## 对象统计与恢复

| 字段 | 含义 |
|---|---|
| `totalObjects` | 清单对象总数 |
| `copiedObjects` | 已确认复制成功，跨重试去重 |
| `skippedExistingObjects` | 已存在而跳过，且没有先前的确认复制记录 |
| `failedObjects` | 当前失败对象数，区别于错误事件次数 |
| `activeObjects` | 最近采样的检查/传输中对象 |
| `pendingObjects` | 待执行对象 |
| `unknownObjects` | 执行结束后结果无法确认的对象 |
| `completedObjects` | 确认复制 + 已存在跳过 |
| `errorEvents` | JSON 日志中的错误事件次数，可对同一对象多次累计 |

上述六个互斥对象状态之和等于 `totalObjects`。速度和本次传输字节保存在 `progress`，不把重试流量换算成完成百分比。大小或 ETA 不可靠时不显示总量。

最终复制日志、原生 `--match`/`--error` 报告与 RC 活动快照共同生成台账，不依赖只有最近 100 条记录的 `core/transferred`。对象结果及日志偏移一并提交检查点，恢复时补读，重复日志不重复累计。正常采样间隔为 2 秒，检查点间隔为 10 秒，结束时立即保存。

任务退出成功且清单结果完整时状态为 `succeeded`；退出成功但统计不完整时为 `needs_review`。可重试的退出码 5 最多执行三次，间隔 30 秒、120 秒；其他失败留待人工处理。rclone 整任务重试固定为 `--retries 1`，底层请求重试仍由 rclone 处理。

暂停恢复或服务重启会重新执行原清单，保留 `--ignore-existing` 语义，不提供字节级断点续传，也不追加内容哈希校验。已有对象即使内容不同也会被跳过。崩溃前未留下证据的复制不追认为已确认复制。

正常停止或重启服务会先记录中断意图并回收进程；异常退出后根据私有 RC socket 和继承的执行锁清退旧进程，确认退出才重新派发。无法确认旧执行结束的任务保持 `blocked`，不会仅凭 PID 重启。

## 数据与运维

```text
data/
  scheduler.json              # 配置和版本
  client.json                 # 本地连接信息及 token
  controller.lock
  batches/<batchId>/
    batch.json                # 导入提交标记、清单索引及汇总
    validation.json
  tasks/<taskId>/
    task.json                 # 状态、当前执行和历史
    manifest.txt              # 不可变清单
    objects.json              # 按行号保存对象状态、计数和日志偏移
    progress.json
    attempts/<attemptId>/
      rclone.jsonl
      matched.txt
      failed.txt
      stderr.log
  runtime/                    # 运行描述符及继承锁
  staging/                    # 导入快照，供未提交批次恢复
  logs/controller.jsonl
```

JSON 文档统一使用 `{ "version": 1, "record": ... }`。每个文件通过同目录临时文件、fsync、rename 和目录 fsync 发布。任何持久化失败都会停止派发并回收进程，包括 rename 已成功但目录同步失败的情况。修复存储问题后重启服务。

对象检查点或任务文档损坏会明确报错；运行期间不要手工编辑或删除数据文件。备份时先停止服务，再复制完整数据目录。统计证据默认保留，不自动轮转或删除；应监控可用磁盘空间。已提交批次的 `staging` 快照可以在停服并完成备份后清理，任务目录及尚未提交批次的快照必须保留。

Linux 部署模板见 [deploy/rcscheduler.service](deploy/rcscheduler.service)。准备 `rcscheduler` 系统用户、可读取的 `/etc/rcscheduler/rclone.conf` 和清单目录，将两个二进制放入 `/usr/local/bin`，再安装 unit：

```bash
sudo cp deploy/rcscheduler.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now rcscheduler
sudo systemctl status rcscheduler
```

使用 `sudo -u rcscheduler rcscheduler --data-dir /var/lib/rcscheduler ...` 管理该实例。模板采用 `KillMode=mixed`：先通知控制器，超时后回收整个服务的剩余进程。[systemd 说明](https://github.com/systemd/systemd/blob/main/man/systemd.kill.xml)

## HTTP API

除 `/healthz` 和 `/readyz` 外均要求 `Authorization: Bearer <token>`。

| 方法与路径 | 用途 |
|---|---|
| `POST /v1/batches/import` | JSON 参数：`id, manifestDir, source, destination, pattern, recursive` |
| `GET /v1/batches/{id}` | 批次状态及实时汇总 |
| `GET /v1/batches/{id}/validation` | 校验结果 |
| `GET /v1/tasks?batchId=&state=&offset=&limit=` | 任务分页 |
| `POST /v1/tasks` | multipart：`metadata` JSON 字段和 `manifest` 文件 |
| `GET /v1/tasks/{id}` | 任务详情 |
| `GET /v1/tasks/{id}/objects?state=&offset=&limit=` | 对象结果分页 |
| `GET /v1/tasks/{id}/attempts` | 执行历史 |
| `POST /v1/tasks/{id}/{pause\|resume\|cancel\|retry}` | 保存控制意图 |
| `PATCH /v1/tasks/{id}` | `{ "priority": 10 }` |
| `GET /v1/scheduler` | 配置及下一任务限速 |
| `PATCH /v1/scheduler` | 部分更新配置：`maxRunning, bandwidthBudget, transfers, checkers, userAgent, s3UploadConcurrency, paused`，支持可选 `revision` 冲突检查 |
| `POST /v1/scheduler/{pause\|resume}` | 暂停或恢复派发 |
| `GET /v1/status` | 引擎版本、任务数、加载错误和存储故障 |

导入返回 202；任务控制返回 202 表示意图已保存，不代表进程已停止。输入错误返回 400、鉴权失败 401、资源不存在 404、状态或幂等冲突 409、存储故障或停服 503。错误结构为 `{ "error": { "code": "...", "message": "..." } }`。

## 开发验证

```bash
make test
make vet

# 固定版本二进制，可使用临时路径；会创建本机 HTTP/Unix socket 和临时文件。
RCSCHEDULER_TEST_RCLONE=/path/to/rclone-v1.75.1 make integration

# 生成 Linux amd64 和 arm64 二进制
make linux
```

普通测试不要求 rclone；真实契约测试需要 `RCSCHEDULER_TEST_RCLONE`，不连接业务桶。测试覆盖原子写故障、整批提交门槛、对象去重、配置快照、自动重试、HTTP/CLI，以及本地真实复制和控制器强制退出后的恢复。

实际存储后端联调需使用隔离测试资源和专用配置：先使用小清单验证确认复制/跳过/失败数、暂停恢复和带宽分配，再导入正式清单。本地契约测试不能替代真实后端验收。
