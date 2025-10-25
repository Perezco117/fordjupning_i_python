import importlib
import logging
from pathlib import Path
import src.logger as logmod


def test_get_logger_config_and_idempotency():
    """
    Testar den slutliga loggern i KK2.

    Vi verifierar:
    - Loggern skapas med rätt nivå och utan propagate.
    - Loggern har exakt två handlers: RotatingFileHandler + StreamHandler.
    - Loggern loggar faktiskt till fil.
    - Upprepad get_logger() återanvänder samma logger utan fler handlers.
    """

    # Säkerställ att vi börjar i ett rent state
    importlib.reload(logmod)

    # 1. Hämta loggern
    logger = logmod.get_logger()

    # -- Grundegenskaper --
    assert isinstance(logger, logging.Logger)
    assert logger.level == logging.INFO
    assert logger.propagate is False

    # -- Handler-typer --
    handler_types = {type(h).__name__ for h in logger.handlers}
    assert "RotatingFileHandler" in handler_types
    assert "StreamHandler" in handler_types
    assert len(logger.handlers) == 2

    # 2. Logga något
    logger.info("hello from kk2 logger test")

    # 3. Hitta filhandlern, flush:a och verifiera att filen finns och inte är tom
    file_handler = None
    for h in logger.handlers:
        if isinstance(h, logging.handlers.RotatingFileHandler):
            file_handler = h
            break

    assert file_handler is not None, "RotatingFileHandler saknas trots att vi förväntar oss den"

    # flush:a till disk
    file_handler.flush()

    log_file_path = Path(file_handler.baseFilename)
    assert log_file_path.exists(), "Loggfilen ska ha skapats"
    assert log_file_path.stat().st_size > 0, "Loggfilen ska inte vara tom"

    # 4. Kalla get_logger() igen -> ska inte lägga till nya handlers
    logger2 = logmod.get_logger()
    assert logger2 is logger
    assert len(logger2.handlers) == 2
