import sys
import os

# Make pipeline code importable without a ClickHouse server running
PIPELINE_DIR = os.path.join(os.path.dirname(__file__), "..", "pipeline")
sys.path.insert(0, os.path.abspath(PIPELINE_DIR))
