#!/usr/bin/env python3
"""Main entry point - runs complete Stage 1 pipeline"""

import sys
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


def main():
    """Run complete pipeline"""
    logger.info("=" * 50)
    logger.info("Nedbank DE Challenge - Stage 1 Pipeline")
    logger.info("=" * 50)
    
    try:
        config = load_config()
        
        run_ingest(config)
        run_transform(config)
        run_provision(config)
        
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