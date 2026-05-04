"""
主程序入口 - 机器人启动和事件循环
"""
import asyncio
import logging
from aiogram import Dispatcher, Bot
from aiogram.types import BotCommand, BotCommandScopeDefault
from db import init_db, close_db
from config import TELEGRAM_BOT_TOKEN, LOG_LEVEL
from handlers.upload import upload_router
from handlers.extract import extract_router

# 配置日志
logging.basicConfig(
	level=LOG_LEVEL,
	format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

# 初始化机器人和调度器
bot = Bot(token=TELEGRAM_BOT_TOKEN)
dp = Dispatcher()

# 注册处理器
dp.include_router(upload_router)
dp.include_router(extract_router)


async def set_default_commands(bot: Bot):
	"""设置默认命令菜单"""
	commands = [
		BotCommand(command="start", description="开始使用机器人"),
		BotCommand(command="edit", description="编辑文件组控制菜单"),
		BotCommand(command="help", description="获取帮助"),
		BotCommand(command="cancel", description="取消当前操作"),
	]
	await bot.set_my_commands(commands, BotCommandScopeDefault())
	logger.info("[Bot] Default commands set")


async def main():
	"""主函数"""
	try:
		# 初始化数据库
		logger.info("[Bot] Initializing database...")
		await init_db()
		
		# 设置命令菜单
		await set_default_commands(bot)
		
		# 删除 webhook（如果有）
		await bot.delete_webhook(drop_pending_updates=True)
		
		# 启动长轮询
		logger.info("[Bot] Starting polling...")
		await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
	
	except Exception as e:
		logger.error(f"[Bot] Fatal error: {e}")
	
	finally:
		# 关闭机器人和数据库连接
		logger.info("[Bot] Shutting down...")
		await close_db()
		await bot.session.close()


if __name__ == "__main__":
	asyncio.run(main())
