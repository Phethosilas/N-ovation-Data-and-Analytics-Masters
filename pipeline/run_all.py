#!/usr/bin/env python3
"""Main entry point - runs complete pipeline (all stages)"""

import sys
import os
import traceback
import yaml

from .common import get_logger, stop_spark_session
from .ingest import run_ingest
from .transform import run_transform
from .provision import run_provision

logger = get_logger(__name__)


def load_config(config_path="/data/config/pipeline_config.yaml"):
    """Load configuration from YAML file"""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def detect_stage(config: dict) -> int:
    """Auto-detect which stage we're running"""
    # Check for streaming directory (Stage 3)
    stream_dir = config.get('paths', {}).get('stream', '/data/stream/')
    if os.path.exists(stream_dir) and os.listdir(stream_dir):
        return 3
    
    # Check for dq_rules.yaml (Stage 2+)
    if os.path.exists('/data/config/dq_rules.yaml'):
        return 2
    
    # Default to Stage 1
    return 1


def run_batch_pipeline(config: dict):
    """Run batch pipeline (all stages)"""
    logger.info("Running batch pipeline: Bronze → Silver → Gold")
    
    run_ingest(config)
    run_transform(config)
    run_provision(config)


def run_streaming_pipeline(config: dict):
    """Run streaming pipeline (Stage 3)"""
    try:
        from .stream_ingest import run_streaming_pipeline as stream_runner
        logger.info("Starting streaming pipeline...")
        stream_runner(config, poll_interval=10, max_iterations=20)
    except ImportError:
        logger.warning("stream_ingest module not available, skipping streaming")


def main():
    """Run complete pipeline"""
    logger.info("=" * 50)
    logger.info("Nedbank DE Challenge - Multi-Stage Pipeline")
    logger.info("=" * 50)
    
    try:
        config = load_config()
        stage = detect_stage(config)
        
        logger.info(f"Detected Stage: {stage}")
        
        # Always run batch pipeline
        run_batch_pipeline(config)
        
        # Run streaming if Stage 3
        if stage == 3:
            run_streaming_pipeline(config)
        
        logger.info("=" * 50)
        logger.info("Pipeline completed successfully")
        logger.info("=" * 50)
        
        return 0
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        logger.error(traceback.format_exc())
        return 1
    finally:
        stop_spark_session()


if __name__ == "__main__":
    sys.exit(main())
