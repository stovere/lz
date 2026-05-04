"""
提取处理器 - 处理文件提取逻辑
"""
import asyncio
import logging
import math
from datetime import datetime
from aiogram import Router, F, types
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from db import db_session_ctx, commit_if_needed
from services.file_service import FileService
from services.group_service import GroupService
from config import MAX_MEDIA_GROUP_SIZE

logger = logging.getLogger(__name__)
extract_router = Router()


def _format_elapsed_zh(seconds: int) -> str:
	"""将已过去秒数格式化为中文时长。"""
	seconds = max(0, int(seconds))
	if seconds <= 5:
		return "刚刚"
	if seconds < 60:
		return f"{seconds}秒"

	days = seconds // 86400
	remain = seconds % 86400
	hours = remain // 3600
	remain %= 3600
	minutes = remain // 60

	parts = []
	if days > 0:
		parts.append(f"{days}天")
	if hours > 0:
		parts.append(f"{hours}小时")
	if minutes > 0:
		parts.append(f"{minutes}分钟")

	if parts:
		# 最多保留三段：天/小时/分钟
		return "".join(parts[:3])

	# 兜底：>=60 秒且分钟为 0 的情况
	return "1分钟"


def get_expired_elapsed_text(expire_datetime) -> str | None:
	"""若已过期则返回“多久之前到期”文本，否则返回 None。"""
	if not expire_datetime:
		return None
	now = datetime.now(expire_datetime.tzinfo) if getattr(expire_datetime, "tzinfo", None) else datetime.now()
	if now < expire_datetime:
		return None
	elapsed = (now - expire_datetime).total_seconds()
	return _format_elapsed_zh(elapsed)


async def _auto_delete_messages(bot, chat_id: int, message_ids: list[int], delay_seconds: int) -> None:
	"""延迟删除指定消息（忽略删除失败）。"""
	if delay_seconds <= 0 or not message_ids:
		return

	await asyncio.sleep(delay_seconds)
	for mid in message_ids:
		try:
			await bot.delete_message(chat_id=chat_id, message_id=mid)
		except Exception:
			# 可能已被删除/超时/权限限制，忽略即可
			pass


def build_pagination_keyboard(group_id: int, current_page: int, total_pages: int):
	"""构建分页键盘"""
	# 最后一页不显示按钮（全部提取完成后按钮消失）
	if current_page >= total_pages:
		return None

	buttons = [[
		InlineKeyboardButton(text=f"{current_page}/{total_pages}", callback_data="noop"),
		InlineKeyboardButton(text="下一页 ➡️", callback_data=f"page_{group_id}_{current_page + 1}"),
	]]

	return InlineKeyboardMarkup(inline_keyboard=buttons)


@extract_router.message(F.text.isdigit())
async def handle_group_id(message: Message):
	"""处理文件组 ID 提取"""
	group_id = int(message.text)
	
	async with db_session_ctx() as session:
		group_service = GroupService(session)
		group = await group_service.get_group(group_id)
		
		if not group:
			await message.answer(f"❌ 文件组 {group_id} 不存在")
			return
		
		if group.upload_status != 2:  # 检查是否已完成
			await message.answer(f"❌ 文件组 {group_id} 未完成或已取消")
			return

		view_limit = int(getattr(group, "view", 0) or 0)
		view_count = int(getattr(group, "view_count", 0) or 0)
		if view_limit > 0 and view_count >= view_limit:
			await message.answer("❌ 此文件已超过可查看次数，无法索取")
			return

		expired_elapsed = get_expired_elapsed_text(getattr(group, "expire_datetime", None))
		if expired_elapsed is not None:
			await message.answer(f"❌ 此文件已于{expired_elapsed}前到期，无法索取")
			return
		
		# 获取第一页文件
		file_count = await group_service.get_group_file_count(group_id)
		total_pages = math.ceil(file_count / MAX_MEDIA_GROUP_SIZE)
		flash_seconds = int(getattr(group, "flash", 0) or 0)
		protect_content = int(getattr(group, "protect", 0) or 0) == 1
		
		# 发送第一页
		sent_ok = await send_group_page(
			message,
			session,
			group_id,
			page=1,
			total_pages=total_pages,
			flash_seconds=flash_seconds,
			protect_content=protect_content,
		)
		if sent_ok:
			await group_service.increment_view_count(group_id)
			await commit_if_needed(session)
	
	logger.info(f"[Extract] User {message.from_user.id} requested group {group_id}")


async def send_group_page(message: Message, session, group_id: int,
						 page: int, total_pages: int, flash_seconds: int = 0, protect_content: bool = False):
	"""发送文件组的某一页"""
	group_service = GroupService(session)
	files = await group_service.get_group_files(group_id, page=page, page_size=MAX_MEDIA_GROUP_SIZE)
	auto_delete_ids: list[int] = []
	sent_any = False
	
	if not files:
		await message.answer(f"❌ 页面 {page} 无文件")
		return False
	
	# 按文件类型分组，以便兼容 media group
	media_groups = {}
	for file_detail in files:
		file_type = file_detail.file_type
		if file_type not in media_groups:
			media_groups[file_type] = []
		media_groups[file_type].append(file_detail)
	
	# 发送各文件类型的 media group
	for file_type, file_list in media_groups.items():
		if file_type in ("photo", "video"):
			# 照片和视频可以组成 media group
			media_items = []
			for file_detail in file_list:
				if file_type == "photo":
					media_items.append(
						types.InputMediaPhoto(
							media=file_detail.file_id,
							caption=f"📷 Position {file_detail.position + 1}"
						)
					)
				elif file_type == "video":
					media_items.append(
						types.InputMediaVideo(
							media=file_detail.file_id,
							caption=f"🎥 Position {file_detail.position + 1}"
						)
					)
			
			# 分批发送 media group（最多 10 个）
			for i in range(0, len(media_items), MAX_MEDIA_GROUP_SIZE):
				batch = media_items[i:i + MAX_MEDIA_GROUP_SIZE]
				if batch:
					sent_messages = await message.answer_media_group(media=batch, protect_content=protect_content)
					sent_any = True
					auto_delete_ids.extend([m.message_id for m in sent_messages if getattr(m, "message_id", None)])
		else:
			# 文档、音频等单独发送
			for file_detail in file_list:
				if file_type == "document":
					sent = await message.answer_document(
						document=file_detail.file_id,
						caption=f"📄 Position {file_detail.position + 1}",
						protect_content=protect_content,
					)
					sent_any = True
					auto_delete_ids.append(sent.message_id)
				elif file_type == "audio":
					sent = await message.answer_audio(
						audio=file_detail.file_id,
						caption=f"🎵 Position {file_detail.position + 1}",
						protect_content=protect_content,
					)
					sent_any = True
					auto_delete_ids.append(sent.message_id)
				elif file_type == "voice":
					sent = await message.answer_voice(
						voice=file_detail.file_id,
						caption=f"🎙️ Position {file_detail.position + 1}",
						protect_content=protect_content,
					)
					sent_any = True
					auto_delete_ids.append(sent.message_id)
	
	# 发送分页信息
	if total_pages > 1:
		keyboard = build_pagination_keyboard(group_id, page, total_pages)
		if keyboard:
			pagination_text = f"📄 第 {page}/{total_pages} 页"
			await message.answer(
				pagination_text,
				reply_markup=keyboard
			)

	# 启用闪照：按设定秒数自动撤回已发送文件
	if flash_seconds > 0 and auto_delete_ids:
		asyncio.create_task(
			_auto_delete_messages(
				bot=message.bot,
				chat_id=message.chat.id,
				message_ids=auto_delete_ids,
				delay_seconds=flash_seconds,
			)
		)

	return sent_any


@extract_router.callback_query(F.data.startswith("page_"))
async def handle_pagination(callback: CallbackQuery):
	"""处理分页回调"""
	try:
		# 解析数据格式: page_<group_id>_<page_num>
		parts = callback.data.split("_")
		group_id = int(parts[1])
		page = int(parts[2])
		
		async with db_session_ctx() as session:
			group_service = GroupService(session)
			group = await group_service.get_group(group_id)
			
			if not group:
				await callback.answer("❌ 文件组不存在", show_alert=True)
				return

			expired_elapsed = get_expired_elapsed_text(getattr(group, "expire_datetime", None))
			if expired_elapsed is not None:
				await callback.answer(f"❌ 已于{expired_elapsed}前到期", show_alert=True)
				return

			view_limit = int(getattr(group, "view", 0) or 0)
			view_count = int(getattr(group, "view_count", 0) or 0)
			if view_limit > 0 and view_count >= view_limit:
				await callback.answer("❌ 此文件已超过可查看次数，无法索取", show_alert=True)
				return
			
			file_count = await group_service.get_group_file_count(group_id)
			total_pages = math.ceil(file_count / MAX_MEDIA_GROUP_SIZE)
			flash_seconds = int(getattr(group, "flash", 0) or 0)
			protect_content = int(getattr(group, "protect", 0) or 0) == 1
			
			if page < 1 or page > total_pages:
				await callback.answer("❌ 页码超出范围", show_alert=True)
				return
			
			# 删除原消息
			await callback.message.delete()
			
			# 发送新页面
			sent_ok = await send_group_page(
				callback.message,
				session,
				group_id,
				page=page,
				total_pages=total_pages,
				flash_seconds=flash_seconds,
				protect_content=protect_content,
			)
			if sent_ok:
				await group_service.increment_view_count(group_id)
				await commit_if_needed(session)
			await callback.answer("✅ 已加载", show_alert=False)
	
	except (ValueError, IndexError):
		await callback.answer("❌ 分页参数错误", show_alert=True)
		logger.error(f"[Extract] Invalid pagination callback: {callback.data}")


@extract_router.callback_query(F.data == "noop")
async def noop_callback(callback: CallbackQuery):
	"""空操作回调"""
	await callback.answer()
