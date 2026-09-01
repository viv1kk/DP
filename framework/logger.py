import logging
import os

def get_logger(name)->logging.Logger:

    logger = logging.getLogger(name)

    if not logger.handlers:

        logger.setLevel(logging.INFO)

        handler = logging.StreamHandler()

        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
        )

        handler.setFormatter(formatter)

        logger.addHandler(handler)

        # This logger owns its handler, so a record must not also bubble up to
        # the root logger. Without this, anything that configures root (uvicorn
        # with a custom log_config, logging.basicConfig, a file handler added
        # later) makes every pipeline line appear twice.
        logger.propagate = False

    return logger


def format_error(exc: BaseException, max_causes: int = 4, max_length: int = 500) -> str:
    """Render an exception and its cause chain on a single line.

    Stands in for a raw traceback dump: keeps the exception types, their
    messages and the file:line the innermost error actually came from, which is
    the diagnostic part. The full traceback stays available at DEBUG level.
    """
    chain = []
    seen = set()
    current = exc
    while current is not None and len(chain) < max_causes and id(current) not in seen:
        seen.add(id(current))
        chain.append(current)
        current = current.__cause__ or current.__context__

    parts = []
    for err in chain:
        label = type(err).__name__
        message = str(err).strip()
        # Handlers re-raise as RecoverableError(f"...{e}"), so each message embeds
        # the one below it. Once the text is already on the line, keep only the type.
        if parts and message and message in " ".join(parts):
            parts.append(label)
        else:
            parts.append(f"{label}: {message}" if message else label)

    origin = ""
    tb = chain[-1].__traceback__ if chain else None
    if tb is not None:
        while tb.tb_next is not None:
            tb = tb.tb_next
        origin = f" [{os.path.basename(tb.tb_frame.f_code.co_filename)}:{tb.tb_lineno}]"

    line = " <- ".join(parts)
    if len(line) > max_length:
        line = line[:max_length - 3] + "..."
    return line + origin


def log_exception(logger: logging.Logger, prefix: str, exc: BaseException):
    """Log a handled exception as one readable line, traceback kept at DEBUG."""
    logger.error(f"{prefix} : {format_error(exc)}")
    logger.debug(f"{prefix} traceback", exc_info=exc)
