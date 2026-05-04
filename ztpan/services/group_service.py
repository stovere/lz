"""
文件组服务 - 处理文件组的创建、更新、查询
"""
import logging
from types import SimpleNamespace

from config import GroupStatus
from db import db_fetchrow, db_fetch, db_execute, db_insert_get_id, db_execute_by_backend

logger = logging.getLogger(__name__)


class GroupService:
	"""文件组服务类"""
	
	def __init__(self, session=None):
		# 兼容旧调用，当前纯 SQL 路径不依赖 session
		self.session = session

	@staticmethod
	def _row_to_obj(row):
		if row is None:
			return None
		if isinstance(row, dict):
			return SimpleNamespace(**row)
		return row
	
	async def create_group(self, user_id: int, content: str = None):
		"""创建新的文件组"""
		group_id = await db_insert_get_id(
			"""
			INSERT INTO ztpan (content, user_id, upload_status, view_count, create_time)
			VALUES (%s, %s, %s, 0, NOW())
			""",
			content, user_id, GroupStatus.COLLECTING,
		)
		logger.info(f"[Group] Created group {group_id} for user {user_id}")
		return SimpleNamespace(id=group_id, user_id=user_id, content=content, upload_status=GroupStatus.COLLECTING)
	
	async def get_collecting_group(self, user_id: int):
		"""获取用户当前收集中的文件组"""
		row = await db_fetchrow(
			"""
			SELECT *
			FROM ztpan
			WHERE user_id=%s AND upload_status=%s
			ORDER BY id DESC
			LIMIT 1
			""",
			user_id, GroupStatus.COLLECTING,
		)
		return self._row_to_obj(row)
	
	async def get_group(self, group_id: int):
		"""获取文件组"""
		row = await db_fetchrow("SELECT * FROM ztpan WHERE id=%s LIMIT 1", group_id)
		return self._row_to_obj(row)
	
	async def complete_group(self, group_id: int) -> bool:
		"""完成文件组"""
		await db_execute(
			"UPDATE ztpan SET upload_status=%s, update_time=NOW() WHERE id=%s",
			GroupStatus.COMPLETED, group_id,
		)
		logger.info(f"[Group] Completed group {group_id}")
		return True
	
	async def cancel_group(self, group_id: int) -> bool:
		"""取消文件组"""
		await db_execute(
			"UPDATE ztpan SET upload_status=%s, update_time=NOW() WHERE id=%s",
			GroupStatus.CANCELLED, group_id,
		)
		logger.info(f"[Group] Cancelled group {group_id}")
		return True
	
	async def delete_group(self, group_id: int) -> bool:
		"""删除文件组"""
		await db_execute(
			"UPDATE ztpan SET upload_status=%s, update_time=NOW() WHERE id=%s",
			GroupStatus.DELETED, group_id,
		)
		logger.info(f"[Group] Deleted group {group_id}")
		return True
	
	async def add_file_to_group(self, group_id: int, file_unique_id: str, file_id: str,
							   file_type: str):
		"""向文件组添加文件"""
		pos_row = await db_fetchrow(
			"SELECT COALESCE(MAX(position), -1) AS max_position FROM ztpan_detail WHERE ztpan_id=%s",
			group_id,
		)
		position = int((pos_row or {}).get("max_position", -1)) + 1

		await db_execute_by_backend(
			"""
			INSERT INTO ztpan_detail (ztpan_id, file_unique_id, file_id, file_type, position, create_time)
			VALUES (%s, %s, %s, %s, %s, NOW())
			ON DUPLICATE KEY UPDATE
				file_id=VALUES(file_id),
				file_type=VALUES(file_type)
			""",
			"""
			INSERT INTO ztpan_detail (ztpan_id, file_unique_id, file_id, file_type, position, create_time)
			VALUES (%s, %s, %s, %s, %s, NOW())
			ON CONFLICT (ztpan_id, file_unique_id)
			DO UPDATE SET file_id=EXCLUDED.file_id, file_type=EXCLUDED.file_type
			""",
			group_id, file_unique_id, file_id, file_type, position,
		)

		logger.info(f"[Group] Added file {file_unique_id} to group {group_id} at position {position}")
		return SimpleNamespace(
			ztpan_id=group_id,
			file_unique_id=file_unique_id,
			file_id=file_id,
			file_type=file_type,
			position=position,
		)
	
	async def get_group_files(self, group_id: int, page: int = 1, page_size: int = 10):
		"""分页获取文件组中的文件"""
		offset = (page - 1) * page_size
		rows = await db_fetch(
			"""
			SELECT ztpan_id, file_unique_id, file_id, file_type, position
			FROM ztpan_detail
			WHERE ztpan_id=%s
			ORDER BY position ASC
			LIMIT %s OFFSET %s
			""",
			group_id, page_size, offset,
		)
		return [self._row_to_obj(r) for r in rows]
	
	async def get_group_file_count(self, group_id: int) -> int:
		"""获取文件组中的文件数量"""
		row = await db_fetchrow("SELECT COUNT(1) AS c FROM ztpan_detail WHERE ztpan_id=%s", group_id)
		return int((row or {}).get("c", 0))
	
	async def increment_view_count(self, group_id: int) -> bool:
		"""增加浏览次数"""
		await db_execute("UPDATE ztpan SET view_count=view_count+1, update_time=NOW() WHERE id=%s", group_id)
		return True

	async def set_group_flash(self, group_id: int, flash_seconds: int) -> bool:
		"""设置文件组闪照秒数（0 表示关闭）。"""
		await db_execute(
			"UPDATE ztpan SET flash=%s, update_time=NOW() WHERE id=%s",
			max(0, int(flash_seconds)),
			group_id,
		)
		return True

	async def set_group_protect(self, group_id: int, protect: int) -> bool:
		"""设置文件组限制转发（0 不限制，1 限制）。"""
		await db_execute(
			"UPDATE ztpan SET protect=%s, update_time=NOW() WHERE id=%s",
			1 if int(protect) == 1 else 0,
			group_id,
		)
		return True

	async def set_group_expire_datetime(self, group_id: int, expire_datetime) -> bool:
		"""设置文件组分享到期时间（None 表示无限制）。"""
		await db_execute(
			"UPDATE ztpan SET expire_datetime=%s, update_time=NOW() WHERE id=%s",
			expire_datetime,
			group_id,
		)
		return True

	async def set_group_expire_minutes(self, group_id: int, minutes: int) -> bool:
		"""按分钟设置分享时限。minutes<=0 则清空时限。"""
		m = int(minutes)
		if m <= 0:
			return await self.set_group_expire_datetime(group_id, None)

		await db_execute_by_backend(
			"UPDATE ztpan SET expire_datetime=DATE_ADD(NOW(), INTERVAL %s MINUTE), update_time=NOW() WHERE id=%s",
			"UPDATE ztpan SET expire_datetime=(NOW() + (%s * INTERVAL '1 minute')), update_time=NOW() WHERE id=%s",
			m, group_id,
		)
		return True

	async def set_group_view_limit(self, group_id: int, view_limit: int) -> bool:
		"""设置文件组查看上限（0 表示不限）。"""
		limit_val = max(0, int(view_limit))
		await db_execute_by_backend(
			"UPDATE ztpan SET `view`=%s, update_time=NOW() WHERE id=%s",
			"UPDATE ztpan SET \"view\"=%s, update_time=NOW() WHERE id=%s",
			limit_val, group_id,
		)
		return True
