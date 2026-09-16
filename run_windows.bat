@echo off
python -m pip install -r requirements.txt
if not exist .env copy .env.example .env
python bot.py
pause
