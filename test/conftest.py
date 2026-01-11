"""
Pytest configuration file that sets up the test environment.
"""
import sys
from pathlib import Path

# Add src directory to Python path so tests can import sttp modules
src_path = Path(__file__).parent.parent / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))
