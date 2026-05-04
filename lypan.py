'''
Telegram 文件暂存与 ID 提取机器人规格

一、项目概述

本项目为一个基于 aiogram 框架开发的 Telegram 机器人。

主要功能为接收用户上传的文件，将文件对应的 file_id 与 file_unique_id 储存至数据库，并为每一组文件生成一个唯一的整数型 id。

用户之后只需发送该 id，即可取回该组文件。

二、文件上传与储存

系统需支持以下类型文件，包括但不限于图片、视频、音频、文档。

每个文件在入库时需要记录以下信息：

table photo/video/document ，记录每个文件的 file_unique_id、file_size、file_name、mime_type、caption、create_time 等元信息。
table file_extension，记录 file_id、file_unique_id、file_type、user_id, bot 等信息。
table ztpan 记录 每一个 collection,
table ztpan_detail 记录 collection 中的每个 file_unique_id 与 file_id 以及 file_type。



三、文件组生成流程

系统使用“文件组（file group）”来管理多个文件。(使用ztpan,ztpan_detail表)

流程说明如下：

当用户发送第一个文件时，系统创建一个新的文件组，状态设为 collecting ( ztpan.upload_status = 1)，同时将该文件写入数据库。

系统随后进入等待状态，持续接收该用户的后续文件。

用户每发送一个新文件，系统将该文件写入同一个 group_id (ztpan.id)。

每次上传后，机器人需提供一个操作选项，完成上传。

如果用户选择继续上传，则用户直接上传文件即可。

如果用户选择完成上传，系统结束该文件组，将状态更新为 completed ( ztpan.upload_status = 2)，并生成唯一 id 返回给用户。

约束条件为，同一用户在同一时间只允许存在一个 collecting 状态的文件组。

四、文件提取流程

当用户发送一个整数 id 时，系统执行以下步骤：

首先校验输入是否为合法数字。

然后查询数据库是否存在该 id。

若存在，则读取该文件组下所有 file_id，并按照 ztpan_detail.ztpan_id 顺序排序。

最后将这些文件返回给用户。

五、分页与发送规则

由于 Telegram 限制单次 media group 最多发送 10 个文件，因此系统必须实现分页机制。

规则如下：

每页最多返回 10 个文件。

用户发送 id 后，默认返回第一页。

当文件总数超过 10 时，需要提供分页操作，例如上一页与下一页。

用户触发分页操作后，系统返回对应页的数据。

所有文件必须严格按照原始上传顺序发送。

需要注意的是，不同 file_type 在 Telegram 中不一定可以组合成同一个 media group，如果无法组合，则需拆分发送。

六、数据库设计要求

数据库需要同时兼容 PostgreSQL 与 MySQL。

设计时应尽量使用通用字段类型，例如 BIGINT、INTEGER、VARCHAR、TEXT、TIMESTAMP。

应避免使用特定数据库专属功能，例如 PostgreSQL 的 JSONB 或 MySQL 的 ENUM。

同时避免依赖特定数据库的 UPSERT 语法或全文索引功能。

七、数据表设计建议

表 ztpan 用于记录文件组信息。

字段包括 id（主键）、user_id、upload_status

表 ztpan_detail 用于记录文件组中的文件明细。

字段包括 ztpan__id、file_id、file_unique_id、file_type。

八、状态定义

ztpan 的 upload_status 字段建议定义如下：

1: collecting 表示文件正在收集
2: completed 表示文件组已完成
0: cancelled 表示用户取消上传或下架 
4: deleted 表示文件组已失效或删除

九、开发注意事项

需要保证文件在组内的顺序一致性。

file_unique_id 可用于判断文件是否重复，但发送文件时必须使用 file_id。

需要处理用户输入非法 id 的情况，例如非数字或不存在的 id。

需要处理用户中途中断上传的情况，可以考虑加入超时或取消机制。

需要严格遵守 Telegram media group 的 10 条限制，实现分页逻辑。

需要处理不同文件类型在发送时的兼容问题。

在并发场景下，需要确保 group_id 不冲突，position 正确递增。

十、简化流程

用户上传文件后，系统创建或取得当前 collecting 状态的文件组，并持续写入文件记录。

用户选择完成上传后，系统关闭文件组并返回 id。

用户发送 id 后，系统查询文件组并分页读取文件，然后返回给用户。


CREATE TABLE `document` (
  `file_unique_id` varchar(100) NOT NULL,
  `file_size` int(12) UNSIGNED NOT NULL,
  `file_name` varchar(100) DEFAULT NULL,
  `mime_type` varchar(100) DEFAULT NULL,
  `caption` mediumtext DEFAULT NULL,
  `create_time` datetime NOT NULL,
  `update_time` datetime DEFAULT NULL,
  `files_drive` varchar(100) DEFAULT NULL,
  `file_password` varchar(150) DEFAULT NULL,
  `kc_id` int(10) UNSIGNED DEFAULT NULL,
  `kc_status` enum('','pending','updated') DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

--
-- 已傾印資料表的索引
--

--
-- 資料表索引 `document`
--
ALTER TABLE `document`
  ADD PRIMARY KEY (`file_unique_id`),
  ADD KEY `file_unique_id` (`file_unique_id`);

  

CREATE TABLE `photo` (
  `file_unique_id` varchar(100) NOT NULL,
  `file_size` int(11) NOT NULL,
  `width` int(11) NOT NULL,
  `height` int(11) NOT NULL,
  `file_name` varchar(100) DEFAULT NULL,
  `caption` mediumtext DEFAULT NULL,
  `root_unique_id` varchar(100) DEFAULT NULL,
  `create_time` datetime NOT NULL,
  `update_time` datetime DEFAULT NULL,
  `files_drive` varchar(100) DEFAULT NULL,
  `hash` varchar(64) DEFAULT NULL,
  `same_fuid` varchar(50) DEFAULT NULL,
  `kc_id` int(11) UNSIGNED DEFAULT NULL,
  `kc_status` varchar(10) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

--
-- 已傾印資料表的索引
--

--
-- 資料表索引 `photo`
--
ALTER TABLE `photo`
  ADD PRIMARY KEY (`file_unique_id`),
  ADD KEY `file_unique_id` (`file_unique_id`);
COMMIT;



CREATE TABLE `video` (
  `file_unique_id` varchar(100) NOT NULL,
  `file_size` int(13) UNSIGNED NOT NULL,
  `duration` int(11) UNSIGNED DEFAULT NULL,
  `width` int(11) UNSIGNED DEFAULT NULL,
  `height` int(11) UNSIGNED DEFAULT NULL,
  `file_name` varchar(100) DEFAULT NULL,
  `mime_type` varchar(100) NOT NULL DEFAULT 'video/mp4',
  `caption` mediumtext DEFAULT NULL,
  `create_time` datetime NOT NULL,
  `update_time` datetime DEFAULT NULL,
  `tag_count` int(11) DEFAULT 0,
  `kind` varchar(2) DEFAULT NULL,
  `credit` int(11) DEFAULT 0,
  `files_drive` varchar(100) DEFAULT NULL,
  `root` varchar(50) DEFAULT NULL,
  `kc_id` int(11) UNSIGNED DEFAULT NULL,
  `kc_status` enum('','pending','updated') DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

--
-- 已傾印資料表的索引
--

--
-- 資料表索引 `video`
--
ALTER TABLE `video`
  ADD PRIMARY KEY (`file_unique_id`),
  ADD KEY `file_size` (`file_size`,`width`,`height`,`mime_type`),
  ADD KEY `file_unique_id` (`file_unique_id`);
COMMIT;



CREATE TABLE `file_extension` (
  `id` bigint(20) UNSIGNED NOT NULL,
  `file_type` varchar(30) DEFAULT NULL,
  `file_unique_id` varchar(100) NOT NULL,
  `file_id` varchar(200) NOT NULL,
  `bot` varchar(50) DEFAULT NULL,
  `user_id` bigint(20) DEFAULT NULL,
  `create_time` datetime DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

--
-- 已傾印資料表的索引
--

--
-- 資料表索引 `file_extension`
--
ALTER TABLE `file_extension`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `file_id` (`file_id`,`bot`),
  ADD KEY `idx_bot` (`bot`),
  ADD KEY `idx_uid_bid` (`file_unique_id`,`file_id`),
  ADD KEY `idx_uid_bot` (`file_unique_id`,`bot`);

--
-- 在傾印的資料表使用自動遞增(AUTO_INCREMENT)
--

--
-- 使用資料表自動遞增(AUTO_INCREMENT) `file_extension`
--
ALTER TABLE `file_extension`
  MODIFY `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT;
COMMIT;


CREATE TABLE `ztpan` (
  `id` int(10) UNSIGNED NOT NULL,
  `content` text DEFAULT NULL,
  `user_id` bigint(20) UNSIGNED DEFAULT NULL,
  `fee` int(10) UNSIGNED DEFAULT NULL,
  `protect` int(10) UNSIGNED DEFAULT NULL,
  `flash` int(10) UNSIGNED DEFAULT NULL,
  `view` int(10) UNSIGNED NOT NULL DEFAULT 0,
  `expire_datetime` int(13) UNSIGNED DEFAULT NULL,
  `enc_str` varchar(30) DEFAULT NULL,
  `source_bot_id` bigint(20) UNSIGNED DEFAULT NULL,
  `upload_status` tinyint(4) NOT NULL DEFAULT 1 COMMENT '0:下架,1:准备中,2:已上架',
  `view_count` int(10) UNSIGNED NOT NULL DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

--
-- 已傾印資料表的索引
--

--
-- 資料表索引 `ztpan`
--
ALTER TABLE `ztpan`
  ADD PRIMARY KEY (`id`);

--
-- 在傾印的資料表使用自動遞增(AUTO_INCREMENT)
--

--
-- 使用資料表自動遞增(AUTO_INCREMENT) `ztpan`
--
ALTER TABLE `ztpan`
  MODIFY `id` int(10) UNSIGNED NOT NULL AUTO_INCREMENT;
COMMIT;


CREATE TABLE `ztpan_detail` (
  `ztpan_id` int(11) NOT NULL,
  `file_unique_id` varchar(50) NOT NULL,
  `file_id` varchar(100) DEFAULT NULL,
  `file_type` varchar(10) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

--
-- 已傾印資料表的索引
--

--
-- 資料表索引 `ztpan_detail`
--
ALTER TABLE `ztpan_detail`
  ADD PRIMARY KEY (`ztpan_id`,`file_unique_id`),
  ADD UNIQUE KEY `ztpan_id` (`ztpan_id`,`file_unique_id`);
COMMIT;

'''

