"""
Plastic-Craft Logging Module
Shared logging system for all optimizer scripts.
Provides console + file logging with color coding,
structured JSON event logs, and run summaries.
"""

import logging
import json
import os
import sys
from datetime import datetime
from pathlib import Path

# ============================================================
# COLOR CODES FOR CONSOLE OUTPUT
# ============================================================

class Colors:
    RESET   = '\033[0m'
    BOLD    = '\033[1m'
    RED     = '\033[91m'
    GREEN   = '\033[92m'
    YELLOW  = '\033[93m'
    BLUE    = '\033[94m'
    MAGENTA = '\033[95m'
    CYAN    = '\033[96m'
    WHITE   = '\033[97m'
    GRAY    = '\033[90m'

# ============================================================
# COLORED FORMATTER FOR CONSOLE
# ============================================================

class ColoredFormatter(logging.Formatter):
    LEVEL_COLORS = {
        logging.DEBUG:    Colors.GRAY,
        logging.INFO:     Colors.WHITE,
        logging.WARNING:  Colors.YELLOW,
        logging.ERROR:    Colors.RED,
        logging.CRITICAL: Colors.RED + Colors.BOLD,
    }

    def format(self, record):
        color = self.LEVEL_COLORS.get(record.levelno, Colors.WHITE)
        ts = datetime.now().strftime('%H:%M:%S')
        level = record.levelname[:4].ljust(4)
        msg = record.getMessage()

        # Special prefixes for structured messages
        if msg.startswith('✓'):
            color = Colors.GREEN
        elif msg.startswith('✗'):
            color = Colors.RED
        elif msg.startswith('⚠'):
            color = Colors.YELLOW
        elif msg.startswith('↻'):
            color = Colors.CYAN
        elif msg.startswith('💾'):
            color = Colors.MAGENTA
        elif msg.startswith('🚛'):
            color = Colors.BLUE
        elif msg.startswith('📋'):
            color = Colors.CYAN

        return f"{Colors.GRAY}{ts}{Colors.RESET} {color}{level}{Colors.RESET} {color}{msg}{Colors.RESET}"

# ============================================================
# PLAIN FORMATTER FOR LOG FILE
# ============================================================

class PlainFormatter(logging.Formatter):
    def format(self, record):
        ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        return f"{ts} [{record.levelname}] {record.getMessage()}"

# ============================================================
# JSON EVENT LOGGER
# Writes structured events to a .jsonl file
# One JSON object per line for easy parsing/filtering
# ============================================================

class JSONEventLogger:
    def __init__(self, filepath):
        self.filepath = filepath
        self.session_id = datetime.now().strftime('%Y%m%d_%H%M%S')

    def log(self, event_type, data):
        event = {
            'ts': datetime.now().isoformat(),
            'session': self.session_id,
            'event': event_type,
            **data
        }
        try:
            with open(self.filepath, 'a', encoding='utf-8') as f:
                f.write(json.dumps(event) + '\n')
        except Exception as e:
            pass  # Never let logging crash the main script

# ============================================================
# MAIN LOGGER FACTORY
# ============================================================

def setup_logger(name, log_dir='/home/claude', level=logging.DEBUG):
    """
    Create a logger with:
    - Colored console output
    - Plain text rotating log file
    - JSON structured event log
    Returns: (logger, json_event_logger)
    """
    Path(log_dir).mkdir(parents=True, exist_ok=True)

    run_id = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = os.path.join(log_dir, f'{name}_{run_id}.log')
    json_file = os.path.join(log_dir, f'{name}_{run_id}.jsonl')

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.handlers.clear()

    # Console handler
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.DEBUG)
    console.setFormatter(ColoredFormatter())
    logger.addHandler(console)

    # File handler
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(PlainFormatter())
    logger.addHandler(file_handler)

    json_logger = JSONEventLogger(json_file)

    logger.info(f"📋 Log file: {log_file}")
    logger.info(f"📋 JSON event log: {json_file}")

    return logger, json_logger, run_id

# ============================================================
# RUN SUMMARY WRITER
# ============================================================

def write_run_summary(log_dir, run_id, stats):
    """
    Write a human-readable run summary at end of processing.
    stats dict should include:
      total, successful, errors, warnings,
      freight_flagged, video_flagged,
      l3_fixes, l4_retries, duration_seconds
    """
    summary_file = os.path.join(log_dir, f'pc_run_summary_{run_id}.txt')

    duration = stats.get('duration_seconds', 0)
    mins = int(duration // 60)
    secs = int(duration % 60)

    lines = [
        '=' * 60,
        'PLASTIC-CRAFT AMAZON LISTING OPTIMIZER',
        f'Run Summary — {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}',
        f'Run ID: {run_id}',
        '=' * 60,
        '',
        'PROCESSING RESULTS',
        f'  Total listings processed : {stats.get("total", 0):,}',
        f'  Successful (ready)       : {stats.get("successful", 0):,}',
        f'  Ready with warnings      : {stats.get("warnings", 0):,}',
        f'  Errors (partial data)    : {stats.get("errors", 0):,}',
        f'  Duration                 : {mins}m {secs}s',
        '',
        'SELF-CORRECTION ACTIVITY',
        f'  L1 input issues flagged  : {stats.get("l1_flagged", 0):,}',
        f'  L2 validation catches    : {stats.get("l2_caught", 0):,}',
        f'  L3 auto-corrections      : {stats.get("l3_fixes", 0):,}',
        f'  L4 targeted retries      : {stats.get("l4_retries", 0):,}',
        '',
        'CONTENT FLAGS',
        f'  Freight notice applied   : {stats.get("freight_flagged", 0):,}',
        f'  Video recommended        : {stats.get("video_flagged", 0):,}',
        f'  Compliance flags added   : {stats.get("compliance_flagged", 0):,}',
        '',
        'OUTPUT FILES',
        f'  Main feed file           : {stats.get("output_file", "N/A")}',
        f'  Error log                : {stats.get("error_log", "N/A")}',
        f'  Full log                 : {stats.get("log_file", "N/A")}',
        f'  JSON event log           : {stats.get("json_log", "N/A")}',
        '',
        '=' * 60,
    ]

    if stats.get('top_errors'):
        lines.append('TOP ERROR TYPES')
        for err, count in stats['top_errors'].items():
            lines.append(f'  {err}: {count}')
        lines.append('')
        lines.append('=' * 60)

    try:
        with open(summary_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(lines))
        return summary_file
    except Exception as e:
        return None
