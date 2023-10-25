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

    def run_rename(self, data_dir, config, **kwargs):
        print(data_dir, config, kwargs)
