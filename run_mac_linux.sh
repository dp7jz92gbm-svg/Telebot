#!/bin/bash
python3 -m pip install -r requirements.txt
[ -f .env ] || cp .env.example .env
python3 bot.py
