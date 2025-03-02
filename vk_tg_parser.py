import os
import random
import time
import json
import logging
import datetime
import pytz
import schedule
import subprocess
import shutil
import vk_api
import requests
import argparse
import threading
from telegram import Bot
from telegram.error import TelegramError
from moviepy.editor import VideoFileClip
import yt_dlp
from config import (
    VK_TOKEN, TELEGRAM_BOT_TOKEN, TELEGRAM_CHANNEL_ID,
    VK_GROUPS, POSTING_TIMES, START_DATE, TEMP_DIR, MAX_CACHED_POSTS
)

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("parser.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Создаем директорию для временных файлов, если она не существует
os.makedirs(TEMP_DIR, exist_ok=True)

# Путь к файлу с кэшем опубликованных постов
PUBLISHED_POSTS_FILE = 'published_posts.json'

# Максимальный размер видео для Telegram (в МБ)
MAX_VIDEO_SIZE_MB = 45  # Оставляем запас от лимита в 50 МБ

# Инициализация API ВКонтакте
vk_session = vk_api.VkApi(token=VK_TOKEN)
vk = vk_session.get_api()

# Инициализация бота Telegram
bot = Bot(token=TELEGRAM_BOT_TOKEN)

# Московский часовой пояс
moscow_tz = pytz.timezone('Europe/Moscow')

# URL вашего сервиса на Render (заполните после деплоя)
SERVICE_URL = "https://vk-tg-parser.onrender.com"  # Замените на ваш URL

def keep_alive():
    """Функция для предотвращения засыпания сервиса на Render.com"""
    while True:
        try:
            requests.get(SERVICE_URL)
            logger.info("Keeping service alive...")
        except Exception as e:
            logger.error(f"Error in keep_alive: {e}")
        time.sleep(600)  # Запрос каждые 10 минут

# Запускаем функцию в отдельном потоке
if __name__ == "__main__" and SERVICE_URL != "https://vk-tg-parser.onrender.com":
    keep_alive_thread = threading.Thread(target=keep_alive)
    keep_alive_thread.daemon = True
    keep_alive_thread.start()
    logger.info("Keep-alive thread started")

def load_published_posts():
    """Загрузка списка уже опубликованных постов"""
    if os.path.exists(PUBLISHED_POSTS_FILE):
        try:
            with open(PUBLISHED_POSTS_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except json.JSONDecodeError:
            logger.warning(f"Ошибка чтения файла {PUBLISHED_POSTS_FILE}. Создаем новый.")
            return []
    return []


def save_published_posts(posts):
    """Сохранение списка опубликованных постов"""
    # Ограничиваем размер кэша
    if len(posts) > MAX_CACHED_POSTS:
        posts = posts[-MAX_CACHED_POSTS:]
    
    try:
        with open(PUBLISHED_POSTS_FILE, 'w', encoding='utf-8') as f:
            json.dump(posts, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Ошибка при сохранении списка опубликованных постов: {e}")


def get_vk_posts_with_videos(group_id, count=100):
    """Получение постов с видео из группы ВКонтакте"""
    try:
        # Преобразуем дату начала в timestamp
        start_date = int(datetime.datetime.strptime(START_DATE, '%Y-%m-%d').timestamp())
        
        # Получаем посты из группы
        response = vk.wall.get(owner_id=-group_id if isinstance(group_id, int) else None,
                              domain=None if isinstance(group_id, int) else group_id,
                              count=count,
                              filter='owner')
        
        posts_with_videos = []
        
        for post in response['items']:
            # Проверяем, что пост после указанной даты
            if post['date'] < start_date:
                continue
                
            # Ищем видео в посте
            has_video = False
            video_urls = []
            
            # Проверяем вложения
            if 'attachments' in post:
                for attachment in post['attachments']:
                    if attachment['type'] == 'video':
                        has_video = True
                        video = attachment['video']
                        
                        # Формируем URL видео
                        owner_id = video['owner_id']
                        video_id = video['id']
                        access_key = video.get('access_key', '')
                        
                        video_url = f"https://vk.com/video{owner_id}_{video_id}"
                        if access_key:
                            video_url += f"_{access_key}"
                            
                        video_urls.append(video_url)
            
            # Если в посте есть видео и текст, добавляем его в список
            if has_video and 'text' in post and post['text'].strip():
                posts_with_videos.append({
                    'id': f"{group_id}_{post['id']}",
                    'text': post['text'],
                    'video_urls': video_urls,
                    'date': post['date'],
                    'group': group_id
                })
        
        logger.info(f"Получено {len(posts_with_videos)} постов с видео из группы {group_id}")
        return posts_with_videos
    
    except Exception as e:
        logger.error(f"Ошибка при получении постов из ВК (группа {group_id}): {e}")
        return []


def download_video(video_url):
    """Скачивание видео с помощью yt-dlp как Python-модуля"""
    try:
        # Создаем временную директорию для видео, если она не существует
        os.makedirs(TEMP_DIR, exist_ok=True)
        
        # Формируем имя для временного файла
        timestamp = int(time.time())
        output_path = os.path.join(TEMP_DIR, f'video_{timestamp}.mp4')
        
        logger.info(f"Начинаем скачивание видео: {video_url}")
        
        # Опции для yt-dlp
        ydl_opts = {
            'format': 'best[ext=mp4]/best',
            'merge_output_format': 'mp4',
            'outtmpl': output_path,
            'quiet': False,
            'no_warnings': False,
            'ignoreerrors': True,
        }
        
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([video_url])
            
            if os.path.exists(output_path):
                logger.info(f"Видео успешно скачано: {output_path}")
                return output_path
            else:
                # Проверяем, был ли файл скачан с другим расширением
                for file in os.listdir(TEMP_DIR):
                    file_path = os.path.join(TEMP_DIR, file)
                    if os.path.isfile(file_path) and file.startswith(f'video_{timestamp}'):
                        logger.info(f"Видео скачано в формате, отличном от mp4: {file_path}")
                        # Конвертируем в mp4
                        return convert_to_mp4(file_path)
                
                logger.error(f"Не удалось скачать видео: {video_url}")
                return None
        except Exception as e:
            logger.error(f"Ошибка при скачивании видео с yt-dlp: {e}")
            return None
    
    except Exception as e:
        logger.error(f"Ошибка при скачивании видео: {e}")
        return None


def convert_to_mp4(video_path):
    """Конвертирует видео в формат MP4 с помощью moviepy"""
    try:
        if not os.path.exists(video_path):
            logger.error(f"Файл не существует: {video_path}")
            return None
        
        # Проверяем, является ли файл уже MP4
        if video_path.lower().endswith('.mp4'):
            logger.info("Файл уже в формате MP4")
            return video_path
        
        # Путь для конвертированного видео
        mp4_path = os.path.splitext(video_path)[0] + ".mp4"
        
        try:
            # Открываем видео с помощью moviepy
            clip = VideoFileClip(video_path)
            
            # Конвертируем в MP4
            clip.write_videofile(
                mp4_path,
                codec='libx264',
                audio_codec='aac',
                preset='medium',
                threads=2
            )
            
            # Закрываем клип для освобождения ресурсов
            clip.close()
            
            logger.info(f"Видео успешно конвертировано в MP4: {mp4_path}")
            
            # Удаляем исходный файл
            try:
                os.remove(video_path)
            except Exception as e:
                logger.warning(f"Не удалось удалить исходный файл после конвертации: {e}")
            
            return mp4_path
        except Exception as e:
            logger.error(f"Ошибка при конвертации видео в MP4 с moviepy: {e}")
            return video_path
    except Exception as e:
        logger.error(f"Ошибка при конвертации видео в MP4: {e}")
        return video_path


def compress_video(video_path):
    """Сжимает видео с помощью moviepy для уменьшения размера"""
    try:
        if not os.path.exists(video_path):
            logger.error(f"Файл не существует: {video_path}")
            return None
        
        # Проверяем, нужно ли конвертировать видео в MP4
        if not video_path.lower().endswith('.mp4'):
            video_path = convert_to_mp4(video_path)
            if not video_path:
                logger.error("Не удалось конвертировать видео в MP4")
                return None
        
        # Получаем размер файла в МБ
        file_size = os.path.getsize(video_path) / (1024 * 1024)
        logger.info(f"Размер исходного файла: {file_size:.2f} МБ")
        
        # Если размер меньше 50 МБ, сжатие не требуется
        if file_size < MAX_VIDEO_SIZE_MB:
            logger.info("Размер файла меньше максимального, сжатие не требуется")
            return video_path
        
        # Путь для сжатого видео
        compressed_path = os.path.join(os.path.dirname(video_path), "compressed_video.mp4")
        
        try:
            # Открываем видео с помощью moviepy
            clip = VideoFileClip(video_path)
            
            # Определяем битрейт в зависимости от размера файла
            if file_size > 200:
                target_bitrate = "1000k"
            elif file_size > 100:
                target_bitrate = "1500k"
            else:
                target_bitrate = "2000k"
            
            # Сжимаем видео
            clip.write_videofile(
                compressed_path,
                codec='libx264',
                bitrate=target_bitrate,
                audio_codec='aac',
                audio_bitrate='128k',
                preset='medium',
                threads=2
            )
            
            # Закрываем клип для освобождения ресурсов
            clip.close()
            
            # Проверяем размер сжатого файла
            compressed_size = os.path.getsize(compressed_path) / (1024 * 1024)
            logger.info(f"Размер сжатого файла: {compressed_size:.2f} МБ")
            
            # Если сжатие не помогло, возвращаем исходный файл
            if compressed_size >= file_size:
                logger.warning("Сжатие не уменьшило размер файла, возвращаем исходный")
                os.remove(compressed_path)
                return video_path
            
            # Если размер все еще больше максимального, пробуем сжать сильнее
            if compressed_size > MAX_VIDEO_SIZE_MB:
                logger.warning(f"Файл все еще слишком большой ({compressed_size:.2f} МБ), пробуем сжать сильнее")
                os.remove(compressed_path)
                
                # Открываем видео снова
                clip = VideoFileClip(video_path)
                
                # Уменьшаем разрешение на 50%
                clip_resized = clip.resize(0.5)
                
                # Сжимаем с более низким битрейтом
                clip_resized.write_videofile(
                    compressed_path,
                    codec='libx264',
                    bitrate="800k",
                    audio_codec='aac',
                    audio_bitrate='96k',
                    preset='medium',
                    threads=2
                )
                
                # Закрываем клипы
                clip.close()
                clip_resized.close()
                
                compressed_size = os.path.getsize(compressed_path) / (1024 * 1024)
                logger.info(f"Размер файла после сильного сжатия: {compressed_size:.2f} МБ")
            
            return compressed_path
        except Exception as e:
            logger.error(f"Ошибка при сжатии видео с moviepy: {e}")
            return video_path
    except Exception as e:
        logger.error(f"Ошибка при сжатии видео: {e}")
        return video_path


def post_to_telegram(text, video_path):
    """Публикация поста в Телеграм-канал"""
    try:
        # Проверяем размер файла
        file_size_mb = os.path.getsize(video_path) / (1024 * 1024)
        
        # Если размер больше максимального, сжимаем видео
        if file_size_mb > MAX_VIDEO_SIZE_MB:
            logger.info(f"Видео слишком большое ({file_size_mb:.2f} МБ), пробуем сжать")
            compressed_path = compress_video(video_path)
            
            # Если получили другой файл (сжатие успешно), используем его
            if compressed_path != video_path:
                video_path = compressed_path
                file_size_mb = os.path.getsize(video_path) / (1024 * 1024)
        
        # Проверяем формат файла и конвертируем в mp4 при необходимости
        if not video_path.lower().endswith('.mp4'):
            logger.info(f"Видео не в формате MP4, конвертируем")
            mp4_path = convert_to_mp4(video_path)
            if mp4_path and mp4_path != video_path:
                video_path = mp4_path
                file_size_mb = os.path.getsize(video_path) / (1024 * 1024)
        
        # Проверяем размер еще раз после сжатия
        if file_size_mb > 50:  # Telegram ограничивает размер до 50 МБ
            logger.warning(f"Видео всё еще слишком большое ({file_size_mb:.2f} МБ), Telegram ограничивает размер до 50 МБ")
            return False
        
        # Отправляем видео с текстом в Телеграм
        with open(video_path, 'rb') as video_file:
            bot.send_video(
                chat_id=TELEGRAM_CHANNEL_ID,
                video=video_file,
                caption=text[:1024],  # Ограничение Telegram на длину подписи
                parse_mode='HTML'
            )
        logger.info(f"Пост успешно опубликован в канале {TELEGRAM_CHANNEL_ID}")
        return True
    
    except TelegramError as e:
        logger.error(f"Ошибка при публикации в Телеграм: {e}")
        return False
    
    except Exception as e:
        logger.error(f"Непредвиденная ошибка при публикации: {e}")
        return False


def clean_temp_directory():
    """Очистка временной директории"""
    try:
        for file in os.listdir(TEMP_DIR):
            file_path = os.path.join(TEMP_DIR, file)
            if os.path.isfile(file_path):
                os.remove(file_path)
        logger.info("Временная директория очищена")
    except Exception as e:
        logger.error(f"Ошибка при очистке временной директории: {e}")


def publish_random_post():
    """Публикация случайного поста из ВК в Телеграм"""
    logger.info("Начинаем публикацию случайного поста")
    
    # Загружаем список уже опубликованных постов
    published_posts = load_published_posts()
    
    # Получаем все посты с видео из всех групп
    all_posts = []
    for group in VK_GROUPS:
        posts = get_vk_posts_with_videos(group)
        all_posts.extend(posts)
    
    if not all_posts:
        logger.warning("Не удалось получить посты с видео из указанных групп")
        return
    
    # Фильтруем посты, которые еще не были опубликованы
    unpublished_posts = [post for post in all_posts if post['id'] not in published_posts]
    
    if not unpublished_posts:
        logger.warning("Нет неопубликованных постов с видео")
        return
    
    # Выбираем случайный пост
    random_post = random.choice(unpublished_posts)
    
    # Выбираем первое видео из поста (если их несколько)
    video_url = random_post['video_urls'][0]
    
    # Скачиваем видео
    video_path = download_video(video_url)
    
    if not video_path:
        logger.error(f"Не удалось скачать видео для поста {random_post['id']}")
        return
    
    # Публикуем пост в Телеграм
    success = post_to_telegram(random_post['text'], video_path)
    
    # Очищаем временную директорию
    clean_temp_directory()
    
    if success:
        # Добавляем ID поста в список опубликованных
        published_posts.append(random_post['id'])
        save_published_posts(published_posts)
        logger.info(f"Пост {random_post['id']} из группы {random_post['group']} успешно опубликован")


def test_parser():
    """Тестовый запуск парсера (публикация одного поста)"""
    logger.info("Запуск тестовой публикации")
    publish_random_post()
    logger.info("Тестовая публикация завершена")


def schedule_posts():
    """Настройка расписания публикаций"""
    for time_str in POSTING_TIMES:
        schedule.every().day.at(time_str).do(publish_random_post)
        logger.info(f"Запланирована публикация на {time_str}")


def main():
    """Основная функция"""
    parser = argparse.ArgumentParser(description='Парсер ВК -> Телеграм')
    parser.add_argument('--test', action='store_true', help='Тестовый запуск (публикация одного поста)')
    args = parser.parse_args()
    
    logger.info("Запуск парсера ВК -> Телеграм")
    
    # Проверяем наличие токенов
    if not VK_TOKEN or not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHANNEL_ID:
        logger.error("Отсутствуют необходимые токены. Проверьте файл .env")
        return
    
    # Проверяем наличие групп для парсинга
    if not VK_GROUPS:
        logger.error("Не указаны группы ВК для парсинга. Проверьте config.py")
        return
    
    # Проверяем наличие yt-dlp
    try:
        subprocess.run(['yt-dlp', '--version'], capture_output=True, text=True)
    except FileNotFoundError:
        logger.error("yt-dlp не установлен. Установите его с помощью 'pip install yt-dlp'")
        return
    
    # Если указан тестовый режим, публикуем один пост и выходим
    if args.test:
        test_parser()
        return
    
    # Настраиваем расписание публикаций
    schedule_posts()
    
    logger.info("Парсер запущен и ожидает времени публикации")
    
    try:
        # Запускаем бесконечный цикл для выполнения запланированных задач
        while True:
            schedule.run_pending()
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("Парсер остановлен пользователем")
    except Exception as e:
        logger.error(f"Непредвиденная ошибка: {e}")


if __name__ == "__main__":
    main() 