"""
上传处理器 - 处理文件上传逻辑
"""
import asyncio
import logging
import math
from datetime import datetime
from collections import defaultdict
from aiogram import Router, F, types
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, ForceReply
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import StateFilter, Command, CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from db import db_session_ctx, commit_if_needed
from services.file_service import FileService
from services.group_service import GroupService
from config import TELEGRAM_BOT_NAME, FILE_TYPES

logger = logging.getLogger(__name__)
upload_router = Router()
_upload_menu_locks: dict[int, asyncio.Lock] = defaultdict(asyncio.Lock)


class UploadStates(StatesGroup):
	"""上传流程状态"""
	waiting_for_file = State()  # 等待文件
	confirming_completion = State()  # 等待确认完成
	waiting_for_expire_minutes = State()  # 等待输入分享时限分钟数
	waiting_for_view_limit = State()  # 等待输入查看上限次数


def get_upload_keyboard():
	"""获取上传后的操作键盘"""
	keyboard = InlineKeyboardMarkup(inline_keyboard=[
		[
			InlineKeyboardButton(text="继续上传", callback_data="upload_continue"),
			InlineKeyboardButton(text="完成上传", callback_data="upload_complete"),
		],
		[
			InlineKeyboardButton(text="取消上传", callback_data="upload_cancel"),
		],
	])
	return keyboard


def get_control_menu_keyboard(group_id: int, flash_seconds: int = 0) -> InlineKeyboardMarkup:
	"""控制面板按钮：闪照 + 限制转发 + 分享时限 + 查看上限。"""
	flash_btn_text = "配置闪照" if int(flash_seconds or 0) > 0 else "启用闪照"
	return InlineKeyboardMarkup(inline_keyboard=[
		[
			InlineKeyboardButton(text=flash_btn_text, callback_data=f"flash_enable_{group_id}"),
		],
		[
			InlineKeyboardButton(text="限制转发", callback_data=f"protect_menu_{group_id}"),
		],
		[
			InlineKeyboardButton(text="分享时限", callback_data=f"expire_menu_{group_id}"),
		],
		[
			InlineKeyboardButton(text="查看上限", callback_data=f"view_menu_{group_id}"),
		],
	])


def format_view_limit_status_text(view_limit: int = 0, view_count: int = 0) -> str:
	"""控制面板展示用：格式化查看上限状态。"""
	limit_val = max(0, int(view_limit or 0))
	count_val = max(0, int(view_count or 0))
	if limit_val <= 0:
		return f"无查看上限（已查看{count_val}次）"
	return f"{count_val}/{limit_val} 次"


def _calc_remaining_minutes(expire_datetime) -> int | None:
	"""计算到期剩余分钟：None=无时限，0=已到期。"""
	if not expire_datetime:
		return None

	now = datetime.now(expire_datetime.tzinfo) if getattr(expire_datetime, "tzinfo", None) else datetime.now()
	delta_sec = (expire_datetime - now).total_seconds()
	if delta_sec <= 0:
		return 0
	return max(1, math.ceil(delta_sec / 60.0))


def format_expire_status_text(expire_datetime) -> str:
	"""控制面板展示用：格式化分享时限状态。"""
	remaining_minutes = _calc_remaining_minutes(expire_datetime)
	if remaining_minutes is None:
		return "无分享时限"
	if remaining_minutes <= 0:
		return "已到期"
	if remaining_minutes < 60:
		return f"剩余约{remaining_minutes}分钟"
	hours = remaining_minutes // 60
	mins = remaining_minutes % 60
	if mins == 0:
		return f"剩余约{hours}小时"
	return f"剩余约{hours}小时{mins}分钟"


def get_flash_setting_keyboard(group_id: int) -> InlineKeyboardMarkup:
	"""闪照设置二级按钮。"""
	return InlineKeyboardMarkup(inline_keyboard=[
		[
			InlineKeyboardButton(text="关闭闪照模式", callback_data=f"flash_set_{group_id}_0"),
		],
		[
			InlineKeyboardButton(text="5秒", callback_data=f"flash_set_{group_id}_5"),
			InlineKeyboardButton(text="10秒", callback_data=f"flash_set_{group_id}_10"),
		],
		[
			InlineKeyboardButton(text="30秒", callback_data=f"flash_set_{group_id}_30"),
			InlineKeyboardButton(text="60秒", callback_data=f"flash_set_{group_id}_60"),
		],
		[
			InlineKeyboardButton(text="取消", callback_data=f"flash_cancel_{group_id}"),
		],
	])


def get_protect_setting_keyboard(group_id: int, protect: int = 0) -> InlineKeyboardMarkup:
	"""限制转发二级按钮。"""
	protect = 1 if int(protect or 0) == 1 else 0
	limit_text = "✅ 限制转发" if protect == 1 else "限制转发"
	unlimit_text = "✅ 不限制转发" if protect == 0 else "不限制转发"
	return InlineKeyboardMarkup(inline_keyboard=[
		[
			InlineKeyboardButton(text=limit_text, callback_data=f"protect_set_{group_id}_1"),
		],
		[
			InlineKeyboardButton(text=unlimit_text, callback_data=f"protect_set_{group_id}_0"),
		],
		[
			InlineKeyboardButton(text="回上一页", callback_data=f"protect_back_{group_id}"),
		],
	])


def get_expire_setting_keyboard(group_id: int, expire_datetime=None) -> InlineKeyboardMarkup:
	"""分享时限二级按钮。"""
	presets = [10, 30, 60, 360, 720, 1440]
	remaining_minutes = _calc_remaining_minutes(expire_datetime)

	selected = None
	if remaining_minutes is None:
		selected = 0
	elif remaining_minutes > 0:
		for p in presets:
			if abs(remaining_minutes - p) <= 1:
				selected = p
				break

	def label(text: str, value: int) -> str:
		return f"✅ {text}" if selected == value else text

	custom_text = "自定义"
	if remaining_minutes not in (None, 0) and selected is None:
		custom_text = f"✅ 自定义（约{remaining_minutes}分钟）"

	return InlineKeyboardMarkup(inline_keyboard=[
		[
			InlineKeyboardButton(text=label("无分享时限", 0), callback_data=f"expire_set_{group_id}_0"),
		],
		[
			InlineKeyboardButton(text=label("10分钟后", 10), callback_data=f"expire_set_{group_id}_10"),
			InlineKeyboardButton(text=label("30分钟后", 30), callback_data=f"expire_set_{group_id}_30"),
			InlineKeyboardButton(text=label("60分钟后", 60), callback_data=f"expire_set_{group_id}_60"),
		],
		[
			InlineKeyboardButton(text=label("6小时后", 360), callback_data=f"expire_set_{group_id}_360"),
			InlineKeyboardButton(text=label("12小时后", 720), callback_data=f"expire_set_{group_id}_720"),
			InlineKeyboardButton(text=label("24小时后", 1440), callback_data=f"expire_set_{group_id}_1440"),
		],
		[
			InlineKeyboardButton(text=custom_text, callback_data=f"expire_custom_{group_id}"),
		],
		[
			InlineKeyboardButton(text="回上一页", callback_data=f"expire_back_{group_id}"),
		],
	])


def get_view_setting_keyboard(group_id: int, view_limit: int = 0) -> InlineKeyboardMarkup:
	"""查看上限二级按钮。"""
	limit_val = max(0, int(view_limit or 0))

	def label(text: str, value: int) -> str:
		return f"✅ {text}" if limit_val == value else text

	custom_text = "自定义"
	if limit_val not in (0, 30, 50, 100):
		custom_text = f"✅ 自定义（{limit_val}次）"

	return InlineKeyboardMarkup(inline_keyboard=[
		[
			InlineKeyboardButton(text=label("无查看上限", 0), callback_data=f"view_set_{group_id}_0"),
		],
		[
			InlineKeyboardButton(text=label("30次", 30), callback_data=f"view_set_{group_id}_30"),
			InlineKeyboardButton(text=label("50次", 50), callback_data=f"view_set_{group_id}_50"),
			InlineKeyboardButton(text=label("100次", 100), callback_data=f"view_set_{group_id}_100"),
		],
		[
			InlineKeyboardButton(text=custom_text, callback_data=f"view_custom_{group_id}"),
		],
		[
			InlineKeyboardButton(text="回上页", callback_data=f"view_back_{group_id}"),
		],
	])


def build_control_menu_text(
	group_id: int,
	flash_seconds: int = 0,
	protect: int = 0,
	expire_datetime=None,
	view_limit: int = 0,
	view_count: int = 0,
) -> str:
	"""控制菜单文案。"""
	flash_text = "关闭" if int(flash_seconds or 0) <= 0 else f"{int(flash_seconds)}秒"
	protect_text = "限制转发" if int(protect or 0) == 1 else "不限制转发"
	expire_text = format_expire_status_text(expire_datetime)
	view_text = format_view_limit_status_text(view_limit, view_count)
	return (
		f"📦 文件组控制面板\n\n"
		f"🔑 文件组 ID: <code>{group_id}</code>\n"
		f"⚡ 当前闪照：<b>{flash_text}</b>\n"
		f"🛡️ 当前转发：<b>{protect_text}</b>\n"
		f"⏳ 分享时限：<b>{expire_text}</b>\n"
		f"👀 查看上限：<b>{view_text}</b>\n\n"
		f"可选：启用闪照后，提取时可按设定秒数处理。"
	)


async def send_or_update_upload_menu(
	message: Message,
	state: FSMContext,
	group_id: int,
	file_count: int,
	file_label: str,
) -> None:
	"""同一用户只保留一个上传菜单消息，多次上传时更新原消息。"""
	user_id = message.from_user.id
	text = (
		f"✅ {file_label}已上传\n\n"
		f"📊 当前文件组 ID: {group_id}\n"
		f"📈 已上传文件数: {file_count}"
	)

	async with _upload_menu_locks[user_id]:
		data = await state.get_data()
		menu_chat_id = data.get("upload_menu_chat_id")
		menu_message_id = data.get("upload_menu_message_id")
		menu_sent = False

		if menu_chat_id and menu_message_id:
			try:
				await message.bot.edit_message_text(
					chat_id=menu_chat_id,
					message_id=menu_message_id,
					text=text,
					reply_markup=get_upload_keyboard(),
				)
				menu_sent = True
			except TelegramBadRequest as exc:
				# 文案/按钮完全相同会报：message is not modified；这不应触发新菜单
				if "message is not modified" in str(exc).lower():
					menu_sent = True
				else:
					menu_sent = False
			except Exception:
				menu_sent = False

		if not menu_sent:
			menu_message = await message.answer(text, reply_markup=get_upload_keyboard())
			await state.update_data(
				upload_menu_chat_id=menu_message.chat.id,
				upload_menu_message_id=menu_message.message_id,
			)
		else:
			await state.update_data(
				upload_menu_chat_id=menu_chat_id,
				upload_menu_message_id=menu_message_id,
			)


@upload_router.message(F.text == "/start")
async def cmd_start(message: Message, state: FSMContext):
	"""开始命令"""
	await message.answer(
		f"欢迎使用 {TELEGRAM_BOT_NAME}！\n\n"
		"📤 发送文件开始上传\n"
		"📥 发送文件组 ID 提取文件\n\n"
		"支持的文件类型：photo, video, document, audio, voice"
	)


@upload_router.message(Command("edit"))
async def cmd_edit_group(message: Message, command: CommandObject):
	"""/edit [id] 调出文件组控制菜单。"""
	arg = (command.args or "").strip() if command else ""
	if not arg or not arg.isdigit():
		await message.answer("用法：/edit [id]\n示例：/edit 12345")
		return

	group_id = int(arg)
	user_id = message.from_user.id

	async with db_session_ctx() as session:
		group_service = GroupService(session)
		group = await group_service.get_group(group_id)
		if not group:
			await message.answer("❌ 文件组不存在")
			return

		if getattr(group, "user_id", None) not in (None, user_id):
			await message.answer("❌ 仅文件组拥有者可编辑")
			return

		flash_seconds = int(getattr(group, "flash", 0) or 0)
		protect = int(getattr(group, "protect", 0) or 0)
		expire_datetime = getattr(group, "expire_datetime", None)
		view_limit = int(getattr(group, "view", 0) or 0)
		view_count = int(getattr(group, "view_count", 0) or 0)

	await message.answer(
		build_control_menu_text(group_id, flash_seconds, protect, expire_datetime, view_limit, view_count),
		parse_mode="HTML",
		reply_markup=get_control_menu_keyboard(group_id, flash_seconds),
	)


@upload_router.message(F.photo)
async def handle_photo(message: Message, state: FSMContext):
	"""处理图片上传"""
	async with db_session_ctx() as session:
		user_id = message.from_user.id
		photo = message.photo[-1]  # 取最大尺寸
		
		# 获取或创建文件组
		group_service = GroupService(session)
		group = await group_service.get_collecting_group(user_id)
		
		if not group:
			group = await group_service.create_group(user_id, content=f"Photos collection from {message.from_user.username or user_id}")
		
		# 保存图片
		file_service = FileService(session)
		await file_service.save_photo(
			file_unique_id=photo.file_unique_id,
			file_size=photo.file_size,
			width=photo.width,
			height=photo.height,
		)
		
		# 保存 file_id 映射
		await file_service.save_file_extension(
			file_type="photo",
			file_unique_id=photo.file_unique_id,
			file_id=photo.file_id,
			bot=TELEGRAM_BOT_NAME,
			user_id=user_id,
		)
		
		# 添加到文件组
		await group_service.add_file_to_group(
			group_id=group.id,
			file_unique_id=photo.file_unique_id,
			file_id=photo.file_id,
			file_type="photo",
		)
		
		# 提交事务
		await commit_if_needed(session)
		
		# 获取文件数量
		file_count = await group_service.get_group_file_count(group.id)

		await state.set_state(UploadStates.waiting_for_file)
		await state.update_data(group_id=group.id)
		await send_or_update_upload_menu(message, state, group.id, file_count, "图片")
	
	logger.info(f"[Upload] Photo from user {user_id}: {photo.file_unique_id}")


@upload_router.message(F.video)
async def handle_video(message: Message, state: FSMContext):
	"""处理视频上传"""
	async with db_session_ctx() as session:
		user_id = message.from_user.id
		video = message.video
		
		# 获取或创建文件组
		group_service = GroupService(session)
		group = await group_service.get_collecting_group(user_id)
		
		if not group:
			group = await group_service.create_group(user_id, content=f"Videos collection from {message.from_user.username or user_id}")
		
		# 保存视频
		file_service = FileService(session)
		await file_service.save_video(
			file_unique_id=video.file_unique_id,
			file_size=video.file_size,
			duration=video.duration,
			width=video.width,
			height=video.height,
			file_name=video.file_name,
			mime_type=video.mime_type,
		)
		
		# 保存 file_id 映射
		await file_service.save_file_extension(
			file_type="video",
			file_unique_id=video.file_unique_id,
			file_id=video.file_id,
			bot=TELEGRAM_BOT_NAME,
			user_id=user_id,
		)
		
		# 添加到文件组
		await group_service.add_file_to_group(
			group_id=group.id,
			file_unique_id=video.file_unique_id,
			file_id=video.file_id,
			file_type="video",
		)
		
		# 提交事务
		await commit_if_needed(session)
		
		# 获取文件数量
		file_count = await group_service.get_group_file_count(group.id)

		await state.set_state(UploadStates.waiting_for_file)
		await state.update_data(group_id=group.id)
		await send_or_update_upload_menu(message, state, group.id, file_count, "视频")
	
	logger.info(f"[Upload] Video from user {user_id}: {video.file_unique_id}")


@upload_router.message(F.document)
async def handle_document(message: Message, state: FSMContext):
	"""处理文档上传"""
	async with db_session_ctx() as session:
		user_id = message.from_user.id
		document = message.document
		
		# 获取或创建文件组
		group_service = GroupService(session)
		group = await group_service.get_collecting_group(user_id)
		
		if not group:
			group = await group_service.create_group(user_id, content=f"Documents collection from {message.from_user.username or user_id}")
		
		# 保存文档
		file_service = FileService(session)
		await file_service.save_document(
			file_unique_id=document.file_unique_id,
			file_size=document.file_size,
			file_name=document.file_name,
			mime_type=document.mime_type,
		)
		
		# 保存 file_id 映射
		await file_service.save_file_extension(
			file_type="document",
			file_unique_id=document.file_unique_id,
			file_id=document.file_id,
			bot=TELEGRAM_BOT_NAME,
			user_id=user_id,
		)
		
		# 添加到文件组
		await group_service.add_file_to_group(
			group_id=group.id,
			file_unique_id=document.file_unique_id,
			file_id=document.file_id,
			file_type="document",
		)
		
		# 提交事务
		await commit_if_needed(session)
		
		# 获取文件数量
		file_count = await group_service.get_group_file_count(group.id)

		await state.set_state(UploadStates.waiting_for_file)
		await state.update_data(group_id=group.id)
		await send_or_update_upload_menu(message, state, group.id, file_count, "文档")
	
	logger.info(f"[Upload] Document from user {user_id}: {document.file_unique_id}")


@upload_router.callback_query(F.data == "upload_continue")
async def callback_continue_upload(callback: CallbackQuery, state: FSMContext):
	"""继续上传回调"""
	await callback.answer("请继续发送文件")
	await callback.message.delete()
	await state.update_data(upload_menu_chat_id=None, upload_menu_message_id=None)
	await state.set_state(UploadStates.waiting_for_file)


@upload_router.callback_query(F.data == "upload_complete")
async def callback_complete_upload(callback: CallbackQuery, state: FSMContext):
	"""完成上传回调"""
	data = await state.get_data()
	group_id = data.get("group_id")
	
	if not group_id:
		await callback.answer("❌ 文件组不存在")
		return
	
	async with db_session_ctx() as session:
		group_service = GroupService(session)
		await group_service.complete_group(group_id)
		await commit_if_needed(session)
		group = await group_service.get_group(group_id)
		flash_seconds = int(getattr(group, "flash", 0) or 0) if group else 0
		protect = int(getattr(group, "protect", 0) or 0) if group else 0
		expire_datetime = getattr(group, "expire_datetime", None) if group else None
		view_limit = int(getattr(group, "view", 0) or 0) if group else 0
		view_count = int(getattr(group, "view_count", 0) or 0) if group else 0
	
	await callback.answer("✅ 上传已完成")
	await callback.message.edit_text(
		build_control_menu_text(group_id, flash_seconds, protect, expire_datetime, view_limit, view_count),
		parse_mode="HTML",
		reply_markup=get_control_menu_keyboard(group_id, flash_seconds),
	)
	await state.clear()


@upload_router.callback_query(F.data == "upload_cancel")
async def callback_cancel_upload(callback: CallbackQuery, state: FSMContext):
	"""取消上传回调"""
	data = await state.get_data()
	group_id = data.get("group_id")
	
	if group_id:
		async with db_session_ctx() as session:
			group_service = GroupService(session)
			await group_service.cancel_group(group_id)
			await commit_if_needed(session)
	
	await callback.answer("❌ 上传已取消")
	await callback.message.delete()
	await state.clear()


@upload_router.callback_query(F.data.startswith("flash_enable_"))
async def callback_flash_enable(callback: CallbackQuery):
	"""点击“启用闪照”后，切换到闪照设置按钮组。"""
	try:
		group_id = int(callback.data.split("_")[-1])
	except (ValueError, IndexError):
		await callback.answer("❌ 参数错误", show_alert=True)
		return

	async with db_session_ctx() as session:
		group_service = GroupService(session)
		group = await group_service.get_group(group_id)
		if not group:
			await callback.answer("❌ 文件组不存在", show_alert=True)
			return
		if getattr(group, "user_id", None) not in (None, callback.from_user.id):
			await callback.answer("❌ 仅文件组拥有者可编辑", show_alert=True)
			return

	await callback.message.edit_reply_markup(reply_markup=get_flash_setting_keyboard(group_id))
	await callback.answer("请选择闪照模式")


@upload_router.callback_query(F.data.startswith("flash_set_"))
async def callback_flash_set(callback: CallbackQuery):
	"""设置闪照秒数并写入 ztpan.flash。"""
	try:
		# flash_set_<group_id>_<seconds>
		parts = callback.data.split("_")
		group_id = int(parts[2])
		seconds = int(parts[3])
	except (ValueError, IndexError):
		await callback.answer("❌ 参数错误", show_alert=True)
		return

	async with db_session_ctx() as session:
		group_service = GroupService(session)
		group = await group_service.get_group(group_id)
		if not group:
			await callback.answer("❌ 文件组不存在", show_alert=True)
			return
		if getattr(group, "user_id", None) not in (None, callback.from_user.id):
			await callback.answer("❌ 仅文件组拥有者可编辑", show_alert=True)
			return
		await group_service.set_group_flash(group_id, seconds)
		await commit_if_needed(session)
		group = await group_service.get_group(group_id)
		flash_seconds = int(getattr(group, "flash", 0) or 0) if group else seconds
		protect = int(getattr(group, "protect", 0) or 0) if group else 0
		expire_datetime = getattr(group, "expire_datetime", None) if group else None
		view_limit = int(getattr(group, "view", 0) or 0) if group else 0
		view_count = int(getattr(group, "view_count", 0) or 0) if group else 0

	await callback.message.edit_text(
		build_control_menu_text(group_id, flash_seconds, protect, expire_datetime, view_limit, view_count),
		parse_mode="HTML",
		reply_markup=get_control_menu_keyboard(group_id, flash_seconds),
	)
	await callback.answer("✅ 已更新闪照设置")


@upload_router.callback_query(F.data.startswith("flash_cancel_"))
async def callback_flash_cancel(callback: CallbackQuery):
	"""取消闪照设置，移除按钮。"""
	try:
		group_id = int(callback.data.split("_")[-1])
	except (ValueError, IndexError):
		await callback.answer("❌ 参数错误", show_alert=True)
		return

	async with db_session_ctx() as session:
		group_service = GroupService(session)
		group = await group_service.get_group(group_id)
		if not group:
			await callback.answer("❌ 文件组不存在", show_alert=True)
			return
		if getattr(group, "user_id", None) not in (None, callback.from_user.id):
			await callback.answer("❌ 仅文件组拥有者可编辑", show_alert=True)
			return
		flash_seconds = int(getattr(group, "flash", 0) or 0)
		protect = int(getattr(group, "protect", 0) or 0)
		expire_datetime = getattr(group, "expire_datetime", None)
		view_limit = int(getattr(group, "view", 0) or 0)
		view_count = int(getattr(group, "view_count", 0) or 0)

	await callback.message.edit_text(
		build_control_menu_text(group_id, flash_seconds, protect, expire_datetime, view_limit, view_count),
		parse_mode="HTML",
		reply_markup=get_control_menu_keyboard(group_id, flash_seconds),
	)
	await callback.answer("已取消")


@upload_router.callback_query(F.data.startswith("protect_menu_"))
async def callback_protect_menu(callback: CallbackQuery):
	"""进入“限制转发”设置子菜单。"""
	try:
		group_id = int(callback.data.split("_")[-1])
	except (ValueError, IndexError):
		await callback.answer("❌ 参数错误", show_alert=True)
		return

	async with db_session_ctx() as session:
		group_service = GroupService(session)
		group = await group_service.get_group(group_id)
		if not group:
			await callback.answer("❌ 文件组不存在", show_alert=True)
			return
		if getattr(group, "user_id", None) not in (None, callback.from_user.id):
			await callback.answer("❌ 仅文件组拥有者可编辑", show_alert=True)
			return
		protect = int(getattr(group, "protect", 0) or 0)

	await callback.message.edit_reply_markup(reply_markup=get_protect_setting_keyboard(group_id, protect))
	await callback.answer("请选择转发限制模式")


@upload_router.callback_query(F.data.startswith("protect_set_"))
async def callback_protect_set(callback: CallbackQuery):
	"""设置 ztpan.protect 并回到控制面板。"""
	try:
		# protect_set_<group_id>_<protect>
		parts = callback.data.split("_")
		group_id = int(parts[2])
		protect = int(parts[3])
	except (ValueError, IndexError):
		await callback.answer("❌ 参数错误", show_alert=True)
		return

	async with db_session_ctx() as session:
		group_service = GroupService(session)
		group = await group_service.get_group(group_id)
		if not group:
			await callback.answer("❌ 文件组不存在", show_alert=True)
			return
		if getattr(group, "user_id", None) not in (None, callback.from_user.id):
			await callback.answer("❌ 仅文件组拥有者可编辑", show_alert=True)
			return

		await group_service.set_group_protect(group_id, protect)
		await commit_if_needed(session)

		group = await group_service.get_group(group_id)
		flash_seconds = int(getattr(group, "flash", 0) or 0) if group else 0
		protect = int(getattr(group, "protect", 0) or 0) if group else protect
		expire_datetime = getattr(group, "expire_datetime", None) if group else None
		view_limit = int(getattr(group, "view", 0) or 0) if group else 0
		view_count = int(getattr(group, "view_count", 0) or 0) if group else 0

	await callback.message.edit_text(
		build_control_menu_text(group_id, flash_seconds, protect, expire_datetime, view_limit, view_count),
		parse_mode="HTML",
		reply_markup=get_control_menu_keyboard(group_id, flash_seconds),
	)
	await callback.answer("✅ 已更新限制转发设置")


@upload_router.callback_query(F.data.startswith("protect_back_"))
async def callback_protect_back(callback: CallbackQuery):
	"""从限制转发子菜单返回控制面板。"""
	try:
		group_id = int(callback.data.split("_")[-1])
	except (ValueError, IndexError):
		await callback.answer("❌ 参数错误", show_alert=True)
		return

	async with db_session_ctx() as session:
		group_service = GroupService(session)
		group = await group_service.get_group(group_id)
		if not group:
			await callback.answer("❌ 文件组不存在", show_alert=True)
			return
		if getattr(group, "user_id", None) not in (None, callback.from_user.id):
			await callback.answer("❌ 仅文件组拥有者可编辑", show_alert=True)
			return

		flash_seconds = int(getattr(group, "flash", 0) or 0)
		protect = int(getattr(group, "protect", 0) or 0)
		expire_datetime = getattr(group, "expire_datetime", None)
		view_limit = int(getattr(group, "view", 0) or 0)
		view_count = int(getattr(group, "view_count", 0) or 0)

	await callback.message.edit_text(
		build_control_menu_text(group_id, flash_seconds, protect, expire_datetime, view_limit, view_count),
		parse_mode="HTML",
		reply_markup=get_control_menu_keyboard(group_id, flash_seconds),
	)
	await callback.answer("已返回控制面板")


@upload_router.callback_query(F.data.startswith("expire_menu_"))
async def callback_expire_menu(callback: CallbackQuery):
	"""进入“分享时限”设置子菜单。"""
	try:
		group_id = int(callback.data.split("_")[-1])
	except (ValueError, IndexError):
		await callback.answer("❌ 参数错误", show_alert=True)
		return

	async with db_session_ctx() as session:
		group_service = GroupService(session)
		group = await group_service.get_group(group_id)
		if not group:
			await callback.answer("❌ 文件组不存在", show_alert=True)
			return
		if getattr(group, "user_id", None) not in (None, callback.from_user.id):
			await callback.answer("❌ 仅文件组拥有者可编辑", show_alert=True)
			return
		expire_datetime = getattr(group, "expire_datetime", None)

	await callback.message.edit_reply_markup(reply_markup=get_expire_setting_keyboard(group_id, expire_datetime))
	await callback.answer("请选择分享时限")


@upload_router.callback_query(F.data.startswith("expire_set_"))
async def callback_expire_set(callback: CallbackQuery):
	"""设置分享时限：0 清空；其余分钟数设置到期时间。"""
	try:
		# expire_set_<group_id>_<minutes>
		parts = callback.data.split("_")
		group_id = int(parts[2])
		minutes = int(parts[3])
	except (ValueError, IndexError):
		await callback.answer("❌ 参数错误", show_alert=True)
		return

	async with db_session_ctx() as session:
		group_service = GroupService(session)
		group = await group_service.get_group(group_id)
		if not group:
			await callback.answer("❌ 文件组不存在", show_alert=True)
			return
		if getattr(group, "user_id", None) not in (None, callback.from_user.id):
			await callback.answer("❌ 仅文件组拥有者可编辑", show_alert=True)
			return

		if minutes <= 0:
			await group_service.set_group_expire_datetime(group_id, None)
		else:
			await group_service.set_group_expire_minutes(group_id, minutes)
		await commit_if_needed(session)

		group = await group_service.get_group(group_id)
		flash_seconds = int(getattr(group, "flash", 0) or 0) if group else 0
		protect = int(getattr(group, "protect", 0) or 0) if group else 0
		expire_datetime = getattr(group, "expire_datetime", None) if group else None
		view_limit = int(getattr(group, "view", 0) or 0) if group else 0
		view_count = int(getattr(group, "view_count", 0) or 0) if group else 0

	await callback.message.edit_text(
		build_control_menu_text(group_id, flash_seconds, protect, expire_datetime, view_limit, view_count),
		parse_mode="HTML",
		reply_markup=get_control_menu_keyboard(group_id, flash_seconds),
	)
	await callback.answer("✅ 已更新分享时限")


@upload_router.callback_query(F.data.startswith("expire_custom_"))
async def callback_expire_custom(callback: CallbackQuery, state: FSMContext):
	"""进入自定义分钟输入（强制回复）。"""
	try:
		group_id = int(callback.data.split("_")[-1])
	except (ValueError, IndexError):
		await callback.answer("❌ 参数错误", show_alert=True)
		return

	async with db_session_ctx() as session:
		group_service = GroupService(session)
		group = await group_service.get_group(group_id)
		if not group:
			await callback.answer("❌ 文件组不存在", show_alert=True)
			return
		if getattr(group, "user_id", None) not in (None, callback.from_user.id):
			await callback.answer("❌ 仅文件组拥有者可编辑", show_alert=True)
			return

	await state.set_state(UploadStates.waiting_for_expire_minutes)
	await state.update_data(expire_group_id=group_id, expire_owner_id=callback.from_user.id)
	await callback.message.answer(
		"请输入分享时限（分钟，正整数）。\n例如：45",
		reply_markup=ForceReply(selective=True),
	)
	await callback.answer("请回复分钟数")


@upload_router.message(StateFilter(UploadStates.waiting_for_expire_minutes), F.text)
async def handle_custom_expire_minutes(message: Message, state: FSMContext):
	"""处理自定义分享时限分钟输入。"""
	data = await state.get_data()
	group_id = data.get("expire_group_id")
	owner_id = data.get("expire_owner_id")

	if not group_id or (owner_id and owner_id != message.from_user.id):
		await state.clear()
		await message.answer("❌ 会话已失效，请重新打开控制面板")
		return

	text = (message.text or "").strip()
	if not text.isdigit() or int(text) <= 0:
		await message.answer("❌ 请输入正整数分钟数，例如 45")
		return

	minutes = int(text)
	if minutes > 525600:
		await message.answer("❌ 分钟数过大，请输入不超过 525600（约 1 年）")
		return

	async with db_session_ctx() as session:
		group_service = GroupService(session)
		group = await group_service.get_group(group_id)
		if not group:
			await state.clear()
			await message.answer("❌ 文件组不存在")
			return
		if getattr(group, "user_id", None) not in (None, message.from_user.id):
			await state.clear()
			await message.answer("❌ 仅文件组拥有者可编辑")
			return

		await group_service.set_group_expire_minutes(group_id, minutes)
		await commit_if_needed(session)

		group = await group_service.get_group(group_id)
		flash_seconds = int(getattr(group, "flash", 0) or 0) if group else 0
		protect = int(getattr(group, "protect", 0) or 0) if group else 0
		expire_datetime = getattr(group, "expire_datetime", None) if group else None
		view_limit = int(getattr(group, "view", 0) or 0) if group else 0
		view_count = int(getattr(group, "view_count", 0) or 0) if group else 0

	await state.clear()
	await message.answer(
		build_control_menu_text(group_id, flash_seconds, protect, expire_datetime, view_limit, view_count),
		parse_mode="HTML",
		reply_markup=get_control_menu_keyboard(group_id, flash_seconds),
	)


@upload_router.callback_query(F.data.startswith("expire_back_"))
async def callback_expire_back(callback: CallbackQuery):
	"""从分享时限子菜单返回控制面板。"""
	try:
		group_id = int(callback.data.split("_")[-1])
	except (ValueError, IndexError):
		await callback.answer("❌ 参数错误", show_alert=True)
		return

	async with db_session_ctx() as session:
		group_service = GroupService(session)
		group = await group_service.get_group(group_id)
		if not group:
			await callback.answer("❌ 文件组不存在", show_alert=True)
			return
		if getattr(group, "user_id", None) not in (None, callback.from_user.id):
			await callback.answer("❌ 仅文件组拥有者可编辑", show_alert=True)
			return

		flash_seconds = int(getattr(group, "flash", 0) or 0)
		protect = int(getattr(group, "protect", 0) or 0)
		expire_datetime = getattr(group, "expire_datetime", None)
		view_limit = int(getattr(group, "view", 0) or 0)
		view_count = int(getattr(group, "view_count", 0) or 0)

	await callback.message.edit_text(
		build_control_menu_text(group_id, flash_seconds, protect, expire_datetime, view_limit, view_count),
		parse_mode="HTML",
		reply_markup=get_control_menu_keyboard(group_id, flash_seconds),
	)
	await callback.answer("已返回控制面板")


@upload_router.callback_query(F.data.startswith("view_menu_"))
async def callback_view_menu(callback: CallbackQuery):
	"""进入“查看上限”设置子菜单。"""
	try:
		group_id = int(callback.data.split("_")[-1])
	except (ValueError, IndexError):
		await callback.answer("❌ 参数错误", show_alert=True)
		return

	async with db_session_ctx() as session:
		group_service = GroupService(session)
		group = await group_service.get_group(group_id)
		if not group:
			await callback.answer("❌ 文件组不存在", show_alert=True)
			return
		if getattr(group, "user_id", None) not in (None, callback.from_user.id):
			await callback.answer("❌ 仅文件组拥有者可编辑", show_alert=True)
			return
		view_limit = int(getattr(group, "view", 0) or 0)

	await callback.message.edit_reply_markup(reply_markup=get_view_setting_keyboard(group_id, view_limit))
	await callback.answer("请选择查看上限")


@upload_router.callback_query(F.data.startswith("view_set_"))
async def callback_view_set(callback: CallbackQuery):
	"""设置查看上限：0 不限；其余为固定次数。"""
	try:
		# view_set_<group_id>_<count>
		parts = callback.data.split("_")
		group_id = int(parts[2])
		count = int(parts[3])
	except (ValueError, IndexError):
		await callback.answer("❌ 参数错误", show_alert=True)
		return

	async with db_session_ctx() as session:
		group_service = GroupService(session)
		group = await group_service.get_group(group_id)
		if not group:
			await callback.answer("❌ 文件组不存在", show_alert=True)
			return
		if getattr(group, "user_id", None) not in (None, callback.from_user.id):
			await callback.answer("❌ 仅文件组拥有者可编辑", show_alert=True)
			return

		await group_service.set_group_view_limit(group_id, max(0, count))
		await commit_if_needed(session)

		group = await group_service.get_group(group_id)
		flash_seconds = int(getattr(group, "flash", 0) or 0) if group else 0
		protect = int(getattr(group, "protect", 0) or 0) if group else 0
		expire_datetime = getattr(group, "expire_datetime", None) if group else None
		view_limit = int(getattr(group, "view", 0) or 0) if group else 0
		view_count = int(getattr(group, "view_count", 0) or 0) if group else 0

	await callback.message.edit_text(
		build_control_menu_text(group_id, flash_seconds, protect, expire_datetime, view_limit, view_count),
		parse_mode="HTML",
		reply_markup=get_control_menu_keyboard(group_id, flash_seconds),
	)
	await callback.answer("✅ 已更新查看上限")


@upload_router.callback_query(F.data.startswith("view_custom_"))
async def callback_view_custom(callback: CallbackQuery, state: FSMContext):
	"""进入自定义查看上限输入（强制回复）。"""
	try:
		group_id = int(callback.data.split("_")[-1])
	except (ValueError, IndexError):
		await callback.answer("❌ 参数错误", show_alert=True)
		return

	async with db_session_ctx() as session:
		group_service = GroupService(session)
		group = await group_service.get_group(group_id)
		if not group:
			await callback.answer("❌ 文件组不存在", show_alert=True)
			return
		if getattr(group, "user_id", None) not in (None, callback.from_user.id):
			await callback.answer("❌ 仅文件组拥有者可编辑", show_alert=True)
			return

	await state.set_state(UploadStates.waiting_for_view_limit)
	await state.update_data(view_group_id=group_id, view_owner_id=callback.from_user.id)
	await callback.message.answer(
		"请输入查看上限次数（正整数）。\n例如：200",
		reply_markup=ForceReply(selective=True),
	)
	await callback.answer("请回复次数")


@upload_router.message(StateFilter(UploadStates.waiting_for_view_limit), F.text)
async def handle_custom_view_limit(message: Message, state: FSMContext):
	"""处理自定义查看上限次数输入。"""
	data = await state.get_data()
	group_id = data.get("view_group_id")
	owner_id = data.get("view_owner_id")

	if not group_id or (owner_id and owner_id != message.from_user.id):
		await state.clear()
		await message.answer("❌ 会话已失效，请重新打开控制面板")
		return

	text = (message.text or "").strip()
	if not text.isdigit() or int(text) <= 0:
		await message.answer("❌ 请输入正整数次数，例如 200")
		return

	view_limit = int(text)
	if view_limit > 100000000:
		await message.answer("❌ 次数过大，请输入不超过 100000000")
		return

	async with db_session_ctx() as session:
		group_service = GroupService(session)
		group = await group_service.get_group(group_id)
		if not group:
			await state.clear()
			await message.answer("❌ 文件组不存在")
			return
		if getattr(group, "user_id", None) not in (None, message.from_user.id):
			await state.clear()
			await message.answer("❌ 仅文件组拥有者可编辑")
			return

		await group_service.set_group_view_limit(group_id, view_limit)
		await commit_if_needed(session)

		group = await group_service.get_group(group_id)
		flash_seconds = int(getattr(group, "flash", 0) or 0) if group else 0
		protect = int(getattr(group, "protect", 0) or 0) if group else 0
		expire_datetime = getattr(group, "expire_datetime", None) if group else None
		view_limit = int(getattr(group, "view", 0) or 0) if group else 0
		view_count = int(getattr(group, "view_count", 0) or 0) if group else 0

	await state.clear()
	await message.answer(
		build_control_menu_text(group_id, flash_seconds, protect, expire_datetime, view_limit, view_count),
		parse_mode="HTML",
		reply_markup=get_control_menu_keyboard(group_id, flash_seconds),
	)


@upload_router.callback_query(F.data.startswith("view_back_"))
async def callback_view_back(callback: CallbackQuery):
	"""从查看上限子菜单返回控制面板。"""
	try:
		group_id = int(callback.data.split("_")[-1])
	except (ValueError, IndexError):
		await callback.answer("❌ 参数错误", show_alert=True)
		return

	async with db_session_ctx() as session:
		group_service = GroupService(session)
		group = await group_service.get_group(group_id)
		if not group:
			await callback.answer("❌ 文件组不存在", show_alert=True)
			return
		if getattr(group, "user_id", None) not in (None, callback.from_user.id):
			await callback.answer("❌ 仅文件组拥有者可编辑", show_alert=True)
			return

		flash_seconds = int(getattr(group, "flash", 0) or 0)
		protect = int(getattr(group, "protect", 0) or 0)
		expire_datetime = getattr(group, "expire_datetime", None)
		view_limit = int(getattr(group, "view", 0) or 0)
		view_count = int(getattr(group, "view_count", 0) or 0)

	await callback.message.edit_text(
		build_control_menu_text(group_id, flash_seconds, protect, expire_datetime, view_limit, view_count),
		parse_mode="HTML",
		reply_markup=get_control_menu_keyboard(group_id, flash_seconds),
	)
	await callback.answer("已返回控制面板")
