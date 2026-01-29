"""
Quick test script for DataPipeline

Fast testing of the complete pipeline.

Usage:
    python scripts/test_pipeline.py
"""

import os
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))

from src.data_pipeline import DataPipeline


def main():
    """Quick test of pipeline functionality"""
    print("=" * 80)
    print("DataPipeline Quick Test")
    print("=" * 80)

    # Initialize pipeline
    pipeline = DataPipeline()

    # Test with small number of reviews
    print("\nRunning pipeline with 10 reviews...")
    print("-" * 80)
    success = pipeline.run(total_reviews=10, fetch_app_info=True)

    if success:
        print("\n" + "=" * 80)
        print("Pipeline test completed successfully!")
        print("=" * 80)
    else:
        print("\n" + "=" * 80)
        print("Pipeline test failed!")
        print("=" * 80)


if __name__ == "__main__":
    main()
