# Telegram 文件暂存与 ID 提取机器人

一个基于 aiogram 框架的 Telegram 机器人，支持文件上传、分组管理和分页提取。

## 功能

### 📤 文件上传
- 支持图片（Photo）、视频（Video）、文档（Document）、音频（Audio）、语音（Voice）
- 自动创建文件组进行管理
- 支持同一文件组内持续上传多个文件

### 📦 文件组管理
- 创建、完成、取消文件组
- 为每个完成的文件组生成唯一 ID
- 记录文件上传顺序

### 📥 文件提取
- 使用文件组 ID 查询并提取文件
- 支持分页显示（每页最多 10 个文件）
- 按上传顺序严格排序

### 💾 数据库兼容
- 支持 MySQL（默认）和 PostgreSQL
- 通用字段类型设计，易于切换

## 项目结构

```
ztpan/
├── config.py              # 配置文件（支持 MySQL/PostgreSQL 切换）
├── db.py                  # 数据库连接与初始化
├── main.py                # 主程序入口
├── requirements.txt       # Python 依赖
├── .env.example           # 环境变量示例
├── handlers/
│   ├── __init__.py
│   ├── upload.py          # 文件上传处理
│   └── extract.py         # 文件提取处理
└── services/
    ├── __init__.py
    ├── file_service.py    # 文件操作服务
    └── group_service.py   # 文件组操作服务
```

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 配置环境变量

复制 `.env.example` 为 `.env`，修改配置：

```bash
# MySQL 配置
DB_TYPE=mysql
DB_HOST=localhost
DB_PORT=3306
DB_USER=root
DB_PASSWORD=your_password
DB_NAME=ztpan

# 或 PostgreSQL 配置
DB_TYPE=postgresql
DB_HOST=localhost
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=your_password
DB_NAME=ztpan

# Telegram 机器人令牌
TELEGRAM_BOT_TOKEN=your_bot_token_here
```

### 3. 初始化数据库

数据库表会在机器人启动时自动创建。

### 4. 启动机器人

```bash
python main.py
```

## 数据库表结构

### photo（图片表）
- `file_unique_id` (PK): 文件唯一标识
- `file_size`: 文件大小
- `width`, `height`: 图片尺寸
- `caption`: 图片描述

### video（视频表）
- `file_unique_id` (PK): 文件唯一标识
- `file_size`: 文件大小
- `duration`: 视频时长
- `width`, `height`: 视频尺寸

### document（文档表）
- `file_unique_id` (PK): 文件唯一标识
- `file_size`: 文件大小
- `file_name`: 文件名
- `mime_type`: MIME 类型

### audio（音频表）
- `file_unique_id` (PK): 文件唯一标识
- `file_size`: 文件大小
- `duration`: 音频时长
- `performer`, `title`: 艺术家和标题

### file_extension（映射表）
- `id` (PK): 自增主键
- `file_type`: 文件类型
- `file_unique_id`: 文件唯一标识
- `file_id`: Telegram file_id
- `bot`: 机器人名称
- `user_id`: 用户 ID

### ztpan（文件组表）
- `id` (PK): 文件组 ID
- `user_id`: 用户 ID
- `content`: 文件组描述
- `upload_status`: 状态（0: 已取消, 1: 收集中, 2: 已完成, 4: 已删除）

### ztpan_detail（文件组详情表）
- `ztpan_id` (FK): 文件组 ID
- `file_unique_id`: 文件唯一标识
- `file_id`: Telegram file_id
- `file_type`: 文件类型
- `position`: 文件在组内的位置

## 使用流程

### 上传文件

1. 用户发送 `/start` 启动机器人
2. 用户发送文件（图片、视频、文档等）
3. 机器人创建文件组并提示操作
4. 用户可以继续上传或完成上传
5. 完成后获得文件组 ID

### 提取文件

1. 用户发送文件组 ID（整数）
2. 机器人返回该文件组的文件
3. 如果文件数量超过 10，提供分页控制
4. 用户通过分页按钮浏览所有文件

## 主要特性

- ✅ 纯 SQL（无 ORM）
- ✅ MySQL 与 PostgreSQL 无缝切换
- ✅ aiogram 3.x FSM 状态管理
- ✅ Telegram media group 自动分页
- ✅ 文件顺序严格保证
- ✅ 完善的错误处理
- ✅ 详细的日志记录

## 配置说明

### MySQL 配置

```python
# config.py
DB_TYPE = "mysql"
DATABASE_URL = "mysql+aiomysql://root:password@localhost:3306/ztpan"
```

### PostgreSQL 配置

```python
# config.py
DB_TYPE = "postgresql"
DATABASE_URL = "postgresql+asyncpg://postgres:password@localhost:5432/ztpan"
```

## 注意事项

1. Telegram media group 限制每次最多 10 个文件
2. 不同文件类型自动拆分发送
3. 文件组创建后未上传任何文件时状态为 collecting
4. 同一用户同时只能有一个 collecting 状态的文件组
5. 确保 `TELEGRAM_BOT_TOKEN` 正确配置

## 开发指南

### 添加新的文件类型

1. 在 `db.py` 的建表 SQL 中补充字段/索引
2. 在 `services/file_service.py` 中添加保存与查询 SQL
3. 在 `handlers/upload.py` 中添加处理器
4. 在 `handlers/extract.py` 中添加提取逻辑

### 自定义数据库配置

修改 `config.py` 中的数据库连接参数或环境变量。

## 许可证

MIT

## 联系方式

如有问题或建议，请提出 Issue 或 Pull Request。
