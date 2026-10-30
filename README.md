# GraphMemory：精简参赛候选

本目录提供原在线v2-r2文本记忆服务的精简源码。候选revision为`r2-whitespace-recovery-20261030`，route为`direct-graph-v2-online-add-r2`，检索策略仍为`direct-graph-v2-role-entry-r2`。默认端口8005。

本次整理将运行所需的共享函数从历史实验脚本中分离，没有修改R2检索、模型提示词、身份与来源校验、Add成功/失败处理或有限重试规则。新revision用于清楚区分构建和代码绑定；不能静默覆盖旧实例或将未部署的候选称为线上版本。

## 安装与检查

验证环境为Python3.12。先创建虚拟环境，再安装固定依赖并准备公开分词器缓存：

```sh
python3.12 -m venv .venv
.venv/bin/python -m pip install -r requirements-r2.txt
.venv/bin/python scripts/bootstrap_tokenizer.py
.venv/bin/python -B verify_release.py --runtime
.venv/bin/python -B serve_online_graph_v2_r2.py
```

最后两个命令只校验源码/配置/分词器，不加载密钥、不创建记忆实例、不调用模型。bootstrap只下载公开o200k_base词表并核对冻结SHA-256；Git源码包不附带词表或第三方依赖包。

Windows可使用相应虚拟环境的`Scripts/python.exe`。Windows部署应选择短目录，避免SQLite与原子发布文件超过系统路径长度。Linux安装和目标机容量仍需实际验证。

## 启动接口

复制`online_graph_service_v2_r2.example.json`为项目内的私有配置，填写真实HTTPS模型端点、环境变量名、累计预算和费率，将`mode`改成`live`，为`data_root`指定新的独立实例。示例费率和100次请求上限不能视为Full预算。

服务密钥通过环境变量或根目录的私有`.env`提供。`MEMORY_API_KEY`至少24字符；其他密钥使用配置中指定的环境变量名。不要将真实配置、凭证、数据库或费用回执提交到仓库。

```sh
.venv/bin/python -B serve_online_graph_v2_r2.py \
  --config service.private.json --serve --live --port 8005
```

后台只监听`127.0.0.1:8005`，可通过已有HTTPS反向代理或ngrok接入公网。该源码包不重复附带各操作系统的服务安装与代理模板。

- `POST /v1/add`：Bearer鉴权，同步持久化与处理，返回原`request_id/user_id/session_id`。
- `POST /v1/search`：Bearer鉴权，返回按相关性排序的原文证据`data`，不超过`top_k`。
- `GET /healthz`：无鉴权，成功为2xx。

## 保留的系统行为

Add保存全部原文及text-embedding-v4文档向量；仅user消息进入gpt-4o-mini抽取与身份判断。所有图关系受原始来源约束，不把assistant建议作为用户事实。

Search先做BM25/向量RRF检索并保留100个原始候选。R2按消息合并至多5个种子，排除speaker枢纽，通过一跳实体关系最多补20个用户来源块，再对完整候选池做一次qwen3.7-text-rerank，最终在8192个o200k_base token内打包原文引用。长query的检索分窗和抽取式重排输入规则不变。

不同user_id独立存储与检索；有效Add后动态登记用户，未知用户Search返回空数组且不调用模型。同一user_id下可跨session检索，支持Add/Search交替调用。timestamp可省略。

未完成Add明确报错，保留已完成步骤与全部费用记录。同request_id、同请求体复用回执；冲突内容拒绝。语义校验拒绝个别关系时允许如实披露`partial`，原文/向量保留，不将其误报为所有关系均入图。

默认逻辑模型请求最多3次原生尝试，包含首次；次数、冷却、成功缓存及失败/未知费用预留跨重启保留。永久错误、过滤和无效/截断输出不自动重新生成。每次发包仍受提供方累计请求数/费用预留上限、单次超时和总请求deadline限制。HTTP客户端标识仍为`MemoryR2/1.0`。

当前代码边界仍是HTTP body不超过1MiB、Add不超过256块、query+options不超过131072字符、top_k为1..100，总请求时间最多1740秒。边界值不等于已验证的Full容量或并发性能。

## 文件布局

33个运行模块保留存储、图处理、R2检索与恢复/费用控制。共享函数集中在`evidence_packing.py`、`graph_source.py`、`graph_types.py`、`graph_validation.py`、`graph_query_support.py`。不再依赖历史`evaluate_*`、`compare_*`、离线答案生成、评分与实验调度脚本。

其余文件只提供入口配置、依赖清单、词表准备、源码校验、许可和说明。源码冻结清单见`versions/r2-whitespace-recovery-20261030/manifest.json`。依赖包由pip获取，其信息见`THIRD_PARTY_NOTICES.md`；项目原MIT许可证保留。

## 验证与限制

67项免费契约测试通过，覆盖原R2候选保留、用户隔离、Add幂等/恢复、partial、长期状态、超时、重试与累计预算。隔离环境只加载本目录运行模块和分词器依赖，已验证本地HTTP Add/Search、重复Add和重启回执。

另使用既有自造数据及成功模型回执做只读对照：同条件下三个query的实际第一阶段候选池和图路径一致；对原有完整候选池，原生重排请求、排名和原文打包一致；七个抽取和六个身份判断请求体完全相同。没有新增模型调用，也没有改写线上数据库。

原实现的长query排序存在Python哈希顺序引起的近似同分顺序差异。本次对照固定同一哈希种子；历史回执只在其完整请求体和有序候选池完全相同的情况下复放，没有借用不同池的旧分数，也没有改变原排序规则。

以上验证仅针对源码、免费契约和只读回执对照，没有据此运行官方Smoke或Full。是否已部署或发布，以对应部署/发布记录为准。正式申报前必须先部署并锁定实际源码/配置，再填写对应公开commit；不能用旧服务冒充新构建。


## 身份引用空白格式恢复

在执行原身份判断校验前，对模型引用中仅空白格式不同的情况，按同一message_id在绑定原文中查找唯一片段，并恢复该片段原始字符。大小写、词语、标点均不改；找不到或有多个匹配时不恢复，继续由原校验器拒绝。决策、理由、来源ID、原模型请求和原生回执均保留；不将无效same降级成自动合并，也不自动重新生成。恢复的字符区间和原引文hash记录在图判断的审计字段中。新revision区分此格式处理修复。
