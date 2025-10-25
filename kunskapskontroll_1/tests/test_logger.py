import importlib
import logging
from pathlib import Path

def test_logger_singleton():
    from src.logger import get_logger
    a = get_logger()
    b = get_logger()
    assert a is b

def test_logger_creates_folder_file_and_no_dup_handlers(monkeypatch, tmp_path):
    # Så att loggning går till tillfällig mapp under test och inte vanliga loggmappen
    monkeypatch.setenv("LOG_DIR", str(tmp_path / "logs"))
    monkeypatch.setenv("LOG_FILE", "unit.log")

    # Laddar moduler så att dem plockar upp env vars i import.
    import src.logger as logmod
    importlib.reload(logmod)

    logger = logmod.get_logger()

    assert logger is logmod.get_logger()

    # Basinställning
    assert logger.propagate is False
    assert logger.level == logging.INFO

    # Förvänta exakt två "Handlers": file (RotatingFileHandler) + console (StreamHandler)
    handler_types = {type(h).__name__ for h in logger.handlers}
    assert "RotatingFileHandler" in handler_types
    assert "StreamHandler" in handler_types
    assert len(logger.handlers) == 2

    # För att verifiera att en fil som är skriven existerar och ej tom
    logger.info("hello from test")
    log_path = Path(tmp_path, "logs", "unit.log")
    assert log_path.exists()
    assert log_path.stat().st_size > 0

    # För att kolla att inga duplikat av "handlers" blir tillagda
    _ = logmod.get_logger()
    assert len(logger.handlers) == 2
