"""Allows `python -m ai_assistant` to launch the CLI."""
import pathlib

# Загружаем .env из корня проекта (GRPC_TOKEN, GRPC_SERVER_URL и т.д.)
try:
    from dotenv import load_dotenv
    _env = pathlib.Path(__file__).parent.parent / ".env"
    if _env.exists():
        load_dotenv(_env)
except ImportError:
    pass

from .cli import main

main()
