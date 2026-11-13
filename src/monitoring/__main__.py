"""Allow src.monitoring to be run as a module: python -m src.monitoring"""
import sys
import os

# Ensure we can import from the project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

try:
    from src.monitoring.health_check import evaluate_and_alert
    
    if __name__ == "__main__":
        try:
            evaluate_and_alert()
        except Exception:
            raise SystemExit(1) from None
except ImportError as e:
    print(f"FATAL: Cannot import health_check: {e}", file=sys.stderr)
    sys.exit(1)

