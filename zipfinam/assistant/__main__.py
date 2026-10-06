"""Allows `python -m zipfinam.assistant` to launch the CLI."""
import pathlib

# Загружаем .env из папки, где запущен ассистент (GRPC_TOKEN, OPENROUTER_API_KEY и т.д.)
try:
    from dotenv import load_dotenv
    _env = pathlib.Path.cwd() / ".env"
    if _env.exists():
        load_dotenv(_env)
except ImportError:
    pass

from .cli import main

main()
