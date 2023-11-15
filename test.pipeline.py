from datetime import datetime
from shutil import copy2
from uuid import uuid4
from pathlib import Path
from typing import Any, Dict, List, Tuple

from ifdo.models import ImageData

from marimba.core.pipeline import BasePipeline
from marimba.lib import exif, image, gps


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
    def get_collection_config_schema() -> dict:
        return {
            "year": 2020,
            "month": 1,
            "day": 1,
        }
    
    def _import(self, data_dir: Path, source_paths: List[Path], config: Dict[str, Any], **kwargs: dict):
        self.logger.info(f"Importing data from {source_paths=} to {data_dir}")
        for source_path in source_paths:
            if not source_path.is_dir():
                continue
            
            in_dir = data_dir / "in"
            in_dir.mkdir(exist_ok=True)
            
            for source_file in source_path.glob("**/*"):
                if source_file.is_file() and source_file.suffix.lower() in [".png", ".jpg", ".jpeg"]:
                    if not self.dry_run:
                        copy2(source_file, in_dir)
                    self.logger.debug(f"Copied {source_file.resolve().absolute()} -> {data_dir}")

    def _process(self, data_dir: Path, config: Dict[str, Any], **kwargs: dict):
        # Find image files from the in dir
        in_dir = data_dir / "in"
        image_file_paths = []
        image_file_paths.extend(in_dir.glob("**/*.png"))
        image_file_paths.extend(in_dir.glob("**/*.jpg"))
        image_file_paths.extend(in_dir.glob("**/*.jpeg"))
        
        # Process images
        if not self.dry_run:
            # Create the output directory
            out_dir = data_dir / "out"
            out_dir.mkdir(exist_ok=True)
            
            # Convert to JPG
            jpg_paths = []
            for image_path in image_file_paths:
                jpg_path = out_dir / f"{image_path.stem}.jpg"
                jpg_paths.append(jpg_path)
                if jpg_path.exists():
                    self.logger.info(f"Skipping {image_path}, {jpg_path} already exists")
                    continue
                
                jpg_path = image.convert_to_jpeg(image_path, destination=jpg_path)
                self.logger.info(f"Converted {image_path} to JPEG")

                # Resize fit in place
                prior_width, prior_height = image.get_width_height(jpg_path)
                image.resize_fit(jpg_path, max_width=640, max_height=640)
                width, height = image.get_width_height(jpg_path)
                self.logger.info(f"Resized {jpg_path} from {prior_width}x{prior_height} to {width}x{height}")
                
                # Copy EXIF
                if exif.copy_exif(image_path, jpg_path):
                    self.logger.info(f"Copied EXIF from {image_path} to {jpg_path}")
        
            # Create a summary grid image
            grid_path = out_dir / "grid.jpg"
            image.create_grid_image(jpg_paths, destination=grid_path)
            self.logger.info(f"Created grid image {grid_path}")
    
    def _compose(self, data_dirs: List[Path], configs: List[Dict[str, Any]], **kwargs: dict) -> Dict[Path, Tuple[Path, List[ImageData]]]:
        # Find all .jpg files in the out dirs in data_dirs and create a mapping from input file path to output file path
        data_mapping = {}
        for data_dir, config in zip(data_dirs, configs):
            out_dir = data_dir / "out"
            
            if not out_dir.exists():
                self.logger.warning(f"Data not processed for {data_dir.name}, skipping")
            
            image_file_paths = []
            image_file_paths.extend(out_dir.glob("**/*.jpg"))
            
            for image_file_path in image_file_paths:
                exif_dt = exif.get_datetime(path=image_file_path)

                output_dir = Path("unknown")
                if exif_dt is not None:
                    output_dir = Path(f"{exif_dt.year:0>4}") / f"{exif_dt.month:0>2}" / f"{exif_dt.day:0>2}"
                
                output_name = f"{image_file_path.stem}-{uuid4()}{image_file_path.suffix}"
                output_file_path = output_dir / output_name
                
                file_created_datetime = datetime.fromtimestamp(image_file_path.stat().st_ctime)
                latitude, longitude = gps.read_exif_location(image_file_path)
                image_data_list = [  # in iFDO, the image data list for an image is a list containing single ImageData
                    ImageData(
                        image_datetime=file_created_datetime,
                        image_latitude=latitude,
                        image_longitude=longitude,
                    )
                ]
                
                data_mapping[image_file_path] = output_file_path, image_data_list
        
        return data_mapping
