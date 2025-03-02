import os
from dotenv import load_dotenv

# Загрузка переменных окружения из .env файла
load_dotenv()

# Токены и ключи API
VK_TOKEN = os.getenv('VK_TOKEN')
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHANNEL_ID = os.getenv('TELEGRAM_CHANNEL_ID')

# Список групп ВКонтакте для парсинга (указывать без '@' и 'public')
# Например: 'group_name' или числовой ID группы
VK_GROUPS = [
    'recepticys',     # Рецепты
    'box_tea',        # Чай
    219956325,        # Числовой ID группы
    'retsepty_video', # Видео-рецепты
]

# Настройки времени публикации (по МСК)
POSTING_TIMES = ['10:00', '13:00', '16:00', '19:00']

# Дата, с которой начинать поиск постов
START_DATE = '2025-01-01'

# Директория для временного хранения видео
TEMP_DIR = 'temp_videos'

# Максимальное количество постов для кэширования
MAX_CACHED_POSTS = 100 