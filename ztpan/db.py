"""
数据库连接与初始化

- MySQL: 使用现有 lz_mysql.py 的 MySQLPool
- PostgreSQL: 使用现有 lz_pgsql.py 的 PGPool
"""
import logging
import sys
from contextlib import asynccontextmanager
from pathlib import Path

from config import DB_TYPE

logger = logging.getLogger(__name__)

# 让 ztpan/ 能导入到同级 lz_mysql.py / lz_pgsql.py
parent_dir = str(Path(__file__).resolve().parent.parent)
if parent_dir not in sys.path:
	sys.path.insert(0, parent_dir)

if DB_TYPE == "mysql":
	from lz_mysql import MySQLPool
else:
	from lz_pgsql import PGPool


async def _ensure_mysql_schema() -> None:
	"""MySQL 侧建表/补列，确保 ztpan 所需结构存在。"""
	if DB_TYPE != "mysql":
		return

	conn, cur = await MySQLPool.get_conn_cursor()
	try:
		await cur.execute(
			"""
			CREATE TABLE IF NOT EXISTS ztpan (
				id INT UNSIGNED NOT NULL AUTO_INCREMENT,
				content TEXT DEFAULT NULL,
				user_id BIGINT UNSIGNED DEFAULT NULL,
				`view` INT UNSIGNED DEFAULT 0,
				protect INT UNSIGNED DEFAULT 0,
				flash INT UNSIGNED DEFAULT 0,
				expire_datetime DATETIME NULL DEFAULT NULL,
				upload_status TINYINT NOT NULL DEFAULT 1,
				view_count INT UNSIGNED NOT NULL DEFAULT 0,
				create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
				update_time DATETIME NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
				PRIMARY KEY (id),
				KEY idx_user_status (user_id, upload_status)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
			"""
		)

		await cur.execute(
			"""
			CREATE TABLE IF NOT EXISTS ztpan_detail (
				ztpan_id INT NOT NULL,
				file_unique_id VARCHAR(100) NOT NULL,
				file_id VARCHAR(200) DEFAULT NULL,
				file_type VARCHAR(30) DEFAULT NULL,
				position INT NOT NULL DEFAULT 0,
				create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
				PRIMARY KEY (ztpan_id, file_unique_id),
				KEY idx_pan_position (ztpan_id, position)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
			"""
		)

		# 兼容老表，缺列则补齐
		await cur.execute("SHOW COLUMNS FROM ztpan_detail LIKE 'position'")
		if not await cur.fetchone():
			await cur.execute("ALTER TABLE ztpan_detail ADD COLUMN position INT NOT NULL DEFAULT 0")

		await cur.execute("SHOW COLUMNS FROM ztpan_detail LIKE 'create_time'")
		if not await cur.fetchone():
			await cur.execute("ALTER TABLE ztpan_detail ADD COLUMN create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP")

		await cur.execute("SHOW COLUMNS FROM ztpan LIKE 'create_time'")
		if not await cur.fetchone():
			await cur.execute("ALTER TABLE ztpan ADD COLUMN create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP")

		await cur.execute("SHOW COLUMNS FROM ztpan LIKE 'update_time'")
		if not await cur.fetchone():
			await cur.execute("ALTER TABLE ztpan ADD COLUMN update_time DATETIME NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP")

		await cur.execute("SHOW COLUMNS FROM ztpan LIKE 'flash'")
		if not await cur.fetchone():
			await cur.execute("ALTER TABLE ztpan ADD COLUMN flash INT UNSIGNED DEFAULT 0")

		await cur.execute("SHOW COLUMNS FROM ztpan LIKE 'protect'")
		if not await cur.fetchone():
			await cur.execute("ALTER TABLE ztpan ADD COLUMN protect INT UNSIGNED DEFAULT 0")

		await cur.execute("SHOW COLUMNS FROM ztpan LIKE 'view'")
		if not await cur.fetchone():
			await cur.execute("ALTER TABLE ztpan ADD COLUMN `view` INT UNSIGNED DEFAULT 0")

		try:
			await cur.execute("SHOW COLUMNS FROM ztpan LIKE 'expire_datetime'")
			expire_col = await cur.fetchone()
			if not expire_col:
				await cur.execute("ALTER TABLE ztpan ADD COLUMN expire_datetime DATETIME NULL DEFAULT NULL")
			else:
				col_type = str(expire_col.get("Type", "")).lower()
				if "datetime" not in col_type and "timestamp" not in col_type:
					try:
						# 先尝试直接改类型（若存量数据可自动转换，这一步最快）
						await cur.execute("ALTER TABLE ztpan MODIFY COLUMN expire_datetime DATETIME NULL DEFAULT NULL")
					except Exception as exc:
						logger.warning(f"[DB] expire_datetime direct modify failed, trying legacy conversion: {exc}")

						# 兼容旧数据：可能是 unix 时间戳（10/13位）或字符串
						await cur.execute("SHOW COLUMNS FROM ztpan LIKE 'expire_datetime_tmp'")
						if not await cur.fetchone():
							await cur.execute("ALTER TABLE ztpan ADD COLUMN expire_datetime_tmp DATETIME NULL DEFAULT NULL")

						await cur.execute(
							"""
							UPDATE ztpan
							SET expire_datetime_tmp = CASE
								WHEN expire_datetime IS NULL THEN NULL
								WHEN CAST(expire_datetime AS CHAR) = '' THEN NULL
								WHEN CAST(expire_datetime AS CHAR) REGEXP '^[0-9]{13}$' THEN FROM_UNIXTIME(CAST(expire_datetime AS UNSIGNED) / 1000)
								WHEN CAST(expire_datetime AS CHAR) REGEXP '^[0-9]{10}$' THEN FROM_UNIXTIME(CAST(expire_datetime AS UNSIGNED))
								WHEN CAST(expire_datetime AS CHAR) REGEXP '^[0-9]{1,9}$' THEN FROM_UNIXTIME(CAST(expire_datetime AS UNSIGNED))
								ELSE STR_TO_DATE(CAST(expire_datetime AS CHAR), '%Y-%m-%d %H:%i:%s')
							END
							"""
						)

						await cur.execute("ALTER TABLE ztpan DROP COLUMN expire_datetime")
						await cur.execute(
							"ALTER TABLE ztpan CHANGE COLUMN expire_datetime_tmp expire_datetime DATETIME NULL DEFAULT NULL"
						)
		except Exception as exc:
			logger.warning(f"[DB] expire_datetime migration skipped due to unexpected error: {exc}")
	finally:
		await MySQLPool.release(conn, cur)


async def _ensure_postgresql_schema() -> None:
	"""PostgreSQL 侧建表，使用纯 SQL。"""
	conn = await PGPool.acquire()
	try:
		await conn.execute(
			"""
			CREATE TABLE IF NOT EXISTS photo (
				file_unique_id VARCHAR(100) PRIMARY KEY,
				file_size INTEGER NOT NULL,
				width INTEGER,
				height INTEGER,
				file_name VARCHAR(100),
				caption TEXT,
				root_unique_id VARCHAR(100),
				create_time TIMESTAMP NOT NULL DEFAULT NOW(),
				update_time TIMESTAMP NULL,
				files_drive VARCHAR(100),
				hash VARCHAR(64),
				same_fuid VARCHAR(50),
				kc_id INTEGER,
				kc_status VARCHAR(10)
			)
			"""
		)

		await conn.execute(
			"""
			CREATE TABLE IF NOT EXISTS video (
				file_unique_id VARCHAR(100) PRIMARY KEY,
				file_size INTEGER NOT NULL,
				duration INTEGER,
				width INTEGER,
				height INTEGER,
				file_name VARCHAR(100),
				mime_type VARCHAR(100) NOT NULL DEFAULT 'video/mp4',
				caption TEXT,
				create_time TIMESTAMP NOT NULL DEFAULT NOW(),
				update_time TIMESTAMP NULL
			)
			"""
		)

		await conn.execute(
			"""
			CREATE TABLE IF NOT EXISTS document (
				file_unique_id VARCHAR(100) PRIMARY KEY,
				file_size INTEGER NOT NULL,
				file_name VARCHAR(100),
				mime_type VARCHAR(100),
				caption TEXT,
				create_time TIMESTAMP NOT NULL DEFAULT NOW(),
				update_time TIMESTAMP NULL
			)
			"""
		)

		await conn.execute(
			"""
			CREATE TABLE IF NOT EXISTS audio (
				file_unique_id VARCHAR(100) PRIMARY KEY,
				file_size INTEGER NOT NULL,
				duration INTEGER,
				performer VARCHAR(100),
				title VARCHAR(100),
				mime_type VARCHAR(100),
				file_name VARCHAR(100),
				caption TEXT,
				create_time TIMESTAMP NOT NULL DEFAULT NOW(),
				update_time TIMESTAMP NULL
			)
			"""
		)

		await conn.execute(
			"""
			CREATE TABLE IF NOT EXISTS file_extension (
				id BIGSERIAL PRIMARY KEY,
				file_type VARCHAR(30),
				file_unique_id VARCHAR(100) NOT NULL,
				file_id VARCHAR(200) NOT NULL,
				bot VARCHAR(50),
				user_id BIGINT,
				create_time TIMESTAMP DEFAULT NOW(),
				UNIQUE (file_id, bot)
			)
			"""
		)

		await conn.execute(
			"""
			CREATE TABLE IF NOT EXISTS ztpan (
				id SERIAL PRIMARY KEY,
				content TEXT,
				user_id BIGINT,
				"view" INTEGER DEFAULT 0,
				protect INTEGER DEFAULT 0,
				flash INTEGER DEFAULT 0,
				expire_datetime TIMESTAMP NULL,
				upload_status SMALLINT NOT NULL DEFAULT 1,
				view_count INTEGER NOT NULL DEFAULT 0,
				create_time TIMESTAMP NOT NULL DEFAULT NOW(),
				update_time TIMESTAMP NULL
			)
			"""
		)

		await conn.execute("ALTER TABLE ztpan ADD COLUMN IF NOT EXISTS flash INTEGER DEFAULT 0")
		await conn.execute("ALTER TABLE ztpan ADD COLUMN IF NOT EXISTS protect INTEGER DEFAULT 0")
		await conn.execute("ALTER TABLE ztpan ADD COLUMN IF NOT EXISTS \"view\" INTEGER DEFAULT 0")
		await conn.execute("ALTER TABLE ztpan ADD COLUMN IF NOT EXISTS expire_datetime TIMESTAMP NULL")

		await conn.execute(
			"""
			CREATE TABLE IF NOT EXISTS ztpan_detail (
				ztpan_id INTEGER NOT NULL,
				file_unique_id VARCHAR(100) NOT NULL,
				file_id VARCHAR(200),
				file_type VARCHAR(30),
				position INTEGER NOT NULL DEFAULT 0,
				create_time TIMESTAMP NOT NULL DEFAULT NOW(),
				PRIMARY KEY (ztpan_id, file_unique_id)
			)
			"""
		)

		await conn.execute("CREATE INDEX IF NOT EXISTS idx_ztpan_user_status ON ztpan(user_id, upload_status)")
		await conn.execute("CREATE INDEX IF NOT EXISTS idx_ztpan_detail_position ON ztpan_detail(ztpan_id, position)")
	finally:
		await PGPool.release(conn)


async def init_db():
	"""初始化数据库。"""
	if DB_TYPE == "mysql":
		await MySQLPool.init_pool()
		await _ensure_mysql_schema()
		logger.info("[DB] MySQL initialized via lz_mysql.MySQLPool")
		return

	await PGPool.init_pool()
	await _ensure_postgresql_schema()
	logger.info("[DB] PostgreSQL initialized via lz_pgsql.PGPool")


@asynccontextmanager
async def db_session_ctx():
	"""统一 DB 上下文：MySQL 返回 None，PostgreSQL 返回 PG 连接。"""
	if DB_TYPE == "mysql":
		await MySQLPool.ensure_pool()
		yield None
		return

	conn = await PGPool.acquire()
	try:
		yield conn
	finally:
		await PGPool.release(conn)


async def commit_if_needed(session) -> None:
	"""纯 SQL 方式下无需显式 commit，保留接口兼容。"""
	return None


def _to_pg_placeholders(sql: str) -> str:
	"""将 %s 占位符转换为 PostgreSQL 的 $1/$2/...。"""
	parts = sql.split("%s")
	if len(parts) == 1:
		return sql

	buf = [parts[0]]
	for i, part in enumerate(parts[1:], start=1):
		buf.append(f"${i}")
		buf.append(part)
	return "".join(buf)


async def db_fetchrow(sql: str, *params):
	"""统一单行查询（返回 dict 或 None）。"""
	if DB_TYPE == "mysql":
		conn, cur = await MySQLPool.get_conn_cursor()
		try:
			await cur.execute(sql, params)
			return await cur.fetchone()
		finally:
			await MySQLPool.release(conn, cur)

	pg_sql = _to_pg_placeholders(sql)
	return await PGPool.fetchrow(pg_sql, *params)


async def db_fetch(sql: str, *params):
	"""统一多行查询（返回 list[dict]）。"""
	if DB_TYPE == "mysql":
		conn, cur = await MySQLPool.get_conn_cursor()
		try:
			await cur.execute(sql, params)
			rows = await cur.fetchall()
			return rows or []
		finally:
			await MySQLPool.release(conn, cur)

	pg_sql = _to_pg_placeholders(sql)
	return await PGPool.fetch(pg_sql, *params)


async def db_execute(sql: str, *params):
	"""统一执行写操作。返回 {'lastrowid': int|None, 'rowcount': int|None}。"""
	if DB_TYPE == "mysql":
		conn, cur = await MySQLPool.get_conn_cursor()
		try:
			await cur.execute(sql, params)
			return {"lastrowid": getattr(cur, "lastrowid", None), "rowcount": getattr(cur, "rowcount", None)}
		finally:
			await MySQLPool.release(conn, cur)

	pg_sql = _to_pg_placeholders(sql)
	await PGPool.execute(pg_sql, *params)
	return {"lastrowid": None, "rowcount": None}


async def db_insert_get_id(sql: str, *params, id_field: str = "id") -> int | None:
	"""统一插入并返回主键 id。MySQL 用 lastrowid，PG 自动追加 RETURNING。"""
	if DB_TYPE == "mysql":
		ret = await db_execute(sql, *params)
		return ret.get("lastrowid")

	base_sql = sql.strip().rstrip(";")
	if " returning " not in base_sql.lower():
		base_sql = f"{base_sql} RETURNING {id_field}"
	row = await db_fetchrow(base_sql, *params)
	if not row:
		return None
	return row.get(id_field)


async def db_execute_by_backend(mysql_sql: str, pg_sql: str, *params):
	"""按后端执行不同 SQL，避免业务层显式判断数据库类型。"""
	if DB_TYPE == "mysql":
		return await db_execute(mysql_sql, *params)
	return await db_execute(pg_sql, *params)


async def close_db():
	"""关闭数据库连接。"""
	if DB_TYPE == "mysql":
		await MySQLPool.close()
		logger.info("[DB] MySQL pool closed")
		return

	await PGPool.close()
	logger.info("[DB] PostgreSQL pool closed via lz_pgsql.PGPool")
