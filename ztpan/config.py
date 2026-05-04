"""
配置文件 - 支持 MySQL 和 PostgreSQL 切换
"""
import os
from pathlib import Path

from dotenv import load_dotenv


ENV_FILE = Path(__file__).with_name(".ztpan.env")
load_dotenv(dotenv_path=ENV_FILE)

# Bot 配置
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
TELEGRAM_BOT_NAME = "ztpan_bot"

# 数据库配置
DB_TYPE = os.getenv("DB_TYPE", "mysql").lower()  # mysql 或 postgresql

if DB_TYPE == "postgresql":
	DB_HOST = os.getenv("DB_HOST", "localhost")
	DB_PORT = int(os.getenv("DB_PORT", 5432))
	DB_USER = os.getenv("DB_USER", "postgres")
	DB_PASSWORD = os.getenv("DB_PASSWORD", "")
	DB_NAME = os.getenv("DB_NAME", "ztpan")
	# asyncpg 连接字符串
	DATABASE_URL = f"postgresql+asyncpg://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
else:
	# MySQL 配置
	DB_HOST = os.getenv("DB_HOST", "localhost")
	DB_PORT = int(os.getenv("DB_PORT", 3306))
	DB_USER = os.getenv("DB_USER", "root")
	DB_PASSWORD = os.getenv("DB_PASSWORD", "")
	DB_NAME = os.getenv("DB_NAME", "ztpan")
	# aiomysql 连接字符串
	DATABASE_URL = f"mysql+aiomysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# 文件组状态常量
class GroupStatus:
	COLLECTING = 1  # 收集中
	COMPLETED = 2   # 已完成
	CANCELLED = 0   # 已取消
	DELETED = 4     # 已删除

# 文件类型常量
FILE_TYPES = {
	"photo": "photo",
	"video": "video",
	"document": "document",
	"audio": "audio",
	"voice": "voice",
}

# Telegram media group 限制
MAX_MEDIA_GROUP_SIZE = 10

# 文件组超时（秒）
GROUP_TIMEOUT_SECONDS = 3600  # 1 小时

# 日志配置
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# 项目根目录
PROJECT_ROOT = Path(__file__).parent.parent

# 数据库连接池配置
DB_POOL_SIZE = int(os.getenv("DB_POOL_SIZE", 10))
DB_MAX_OVERFLOW = int(os.getenv("DB_MAX_OVERFLOW", 20))

print(f"[Config] Using {DB_TYPE.upper()} database: {DB_HOST}:{DB_PORT}/{DB_NAME}")
