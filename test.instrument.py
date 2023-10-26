from datetime import datetime
from time import sleep
from uuid import uuid4
from pathlib import Path
from typing import Any, Dict, List, Tuple

from ifdo.models import iFDO, ImageSetHeader, ImageData

from marimba.core.base_instrument import BaseInstrument


class TestInstrument(BaseInstrument):
    """
    Test instrument. No-op.
    """
    
    @staticmethod
    def get_instrument_config_schema() -> dict:
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

    def _process(self, data_dir: Path, config: Dict[str, Any], **kwargs: dict):
        n = config.get("n", 3)
        try:
            n = int(n)
        except ValueError:
            n = 3
        
        for idx in range(n):
            self.logger.info(f"Processing {idx+1}/{3}...")
            sleep(1)
    
    def _compose(self, data_dirs: List[Path], configs: List[Dict[str, Any]], **kwargs: dict) -> Tuple[Any, Dict[Path, Path]]:
        # Find all .png, .jpg, .jpeg files in data_dirs
        image_file_paths: List[Path] = []
        for data_dir in data_dirs:
            image_file_paths.extend(data_dir.glob("**/*.png"))
            image_file_paths.extend(data_dir.glob("**/*.jpg"))
            image_file_paths.extend(data_dir.glob("**/*.jpeg"))
        
        # Define the path mapping (use old filename stem + random UUID)
        path_mapping = {
            image_file_path: Path(f"{image_file_path.stem}-{uuid4()}.{image_file_path.suffix}")
            for image_file_path in image_file_paths
        }
        
        # Create the iFDO
        image_set_items = {}
        for image_file_path in image_file_paths:
            file_created_datetime = datetime.fromtimestamp(image_file_path.stat().st_ctime)
            image_set_items[str(path_mapping[image_file_path])] = ImageData(
                image_datetime=file_created_datetime
            )
        
        ifdo = iFDO(
            image_set_header=ImageSetHeader(
                image_set_name=f"Test Instrument Dataset",
                image_set_uuid=str(uuid4()),
                image_set_handle="test_instrument",
            ),
            image_set_items=image_set_items,
        )
        
        return ifdo, path_mapping
