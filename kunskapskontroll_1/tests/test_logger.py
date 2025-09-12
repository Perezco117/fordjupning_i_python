from src.logger import get_logger

def test_logger_singleton():
    a = get_logger()
    b = get_logger()
    assert a is b
