from datetime import datetime
from shutil import copy2
from time import sleep
from uuid import uuid4
from pathlib import Path
from typing import Any, Dict, List, Tuple

from ifdo.models import iFDO, ImageSetHeader, ImageData

from marimba.core.pipeline import BasePipeline


class TestPipeline(BasePipeline):
    """
    Test pipeline. No-op.
    """
    
    @staticmethod
    def get_pipeline_config_schema() -> dict:
        return {
            "my_str_key": "my_str_value",
            "my_int_key": 1,
            "my_float_key": 1.0,
            "my_bool_key": True,
        }
    
    @staticmethod
    def get_deployment_config_schema() -> dict:
        return {
            "year": 2020,
            "month": 1,
            "day": 1,
        }
    
    def _import(self, data_dir: Path, source_dir: Path, config: Dict[str, Any], **kwargs: dict):
        self.logger.info(f"Importing data from {source_dir} to {data_dir}")
        for source_path in source_dir.iterdir():
            copy2(source_path, data_dir)
            self.logger.info(f"Copied {source_path} -> {data_dir}")

    def _process(self, data_dir: Path, config: Dict[str, Any], **kwargs: dict):
        n = kwargs.get("n", 3)
        try:
            n = int(n)
        except ValueError:
            n = 3
        
        for idx in range(n):
            self.logger.info(f"Processing {idx+1}/{n}...")
            sleep(1)
    
    def _compose(self, data_dirs: List[Path], configs: List[Dict[str, Any]], **kwargs: dict) -> Tuple[iFDO, Dict[Path, Path]]:
        # Find all .png, .jpg, .jpeg files in data_dirs and create a mapping from input file path to output file path
        path_mapping = {}
        for data_dir, config in zip(data_dirs, configs):
            year = config.get("year")
            month = config.get("month")
            day = config.get("day")
            output_dir = Path(year) / month / day
            
            image_file_paths = []
            image_file_paths.extend(data_dir.glob("**/*.png"))
            image_file_paths.extend(data_dir.glob("**/*.jpg"))
            image_file_paths.extend(data_dir.glob("**/*.jpeg"))
            
            for image_file_path in image_file_paths:
                output_name = f"{image_file_path.stem}-{uuid4()}{image_file_path.suffix}"
                output_file_path = output_dir / output_name
                path_mapping[image_file_path] = output_file_path
        
        self.logger.debug(f"{path_mapping=}")
        
        # Create the iFDO
        image_set_items = {}
        for image_file_path in path_mapping:
            file_created_datetime = datetime.fromtimestamp(image_file_path.stat().st_ctime)
            image_set_items[str(path_mapping[image_file_path])] = ImageData(
                image_datetime=file_created_datetime
            )
        
        ifdo = iFDO(
            image_set_header=ImageSetHeader(
                image_set_name=f"Test Pipeline Dataset",
                image_set_uuid=str(uuid4()),
                image_set_handle="test_pipeline",
            ),
            image_set_items=image_set_items,
        )
        
        return ifdo, path_mapping
