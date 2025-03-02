@echo off
echo Запуск парсера ВК -> Телеграм в тестовом режиме
cd /d %~dp0
python vk_tg_parser.py --test
pause 