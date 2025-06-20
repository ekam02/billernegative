import os
from pathlib import Path
import tomllib

from dotenv import load_dotenv
from oracledb import init_oracle_client
from sqlmodel import create_engine

from .logger_config import setup_logger


class AppSettings:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(AppSettings, cls).__new__(cls)
            cls._instance._load_settings()
        return cls._instance

    def _load_settings(self):
        init_oracle_client()
        load_dotenv()

        self.cfg_dir = Path(__file__).parent   # Config Directory
        self.repository = self.cfg_dir.parent  # Repository Directory
        self.cfg_file = self.cfg_dir / "config.toml"

        if not self.cfg_file.exists():
            raise FileNotFoundError(f"No config file found at {self.cfg_file}")

        with self.cfg_file.open("rb") as f:
            cfg = tomllib.load(f)

        self.log_console_level = cfg.get("log_console_level", 20)
        self.log_file_level = cfg.get("log_file_level", 30)
        engine_pool_size = cfg.get("engine_pool_size", 5)
        engine_max_overflow = cfg.get("engine_max_overflow", 10)

        logger = setup_logger(__name__, console_level=self.log_console_level, file_level=self.log_file_level)
        logger.debug("Loading config settings")

        biller_user = os.getenv("BILLER_USER")
        biller_pass = os.getenv("BILLER_PASS")
        biller_host = os.getenv("BILLER_HOST")
        biller_port = os.getenv("BILLER_PORT")
        biller_name = os.getenv("BILLER_NAME")
        biller_sche = os.getenv("BILLER_SCHE")

        if not all([biller_user, biller_pass, biller_host, biller_port, biller_name, biller_sche]):
            raise ValueError("Missing required environment variables")

        self.biller_engine = create_engine(
            f"postgresql://{biller_user}:{biller_pass}@{biller_host}:{biller_port}/{biller_name}",
            pool_size=engine_pool_size, max_overflow=engine_max_overflow,
            connect_args={"options": f"-csearch_path={biller_sche}"}
        )

        jano_user = os.getenv("JANO_USER")
        jano_pass = os.getenv("JANO_PASS")
        jano_host = os.getenv("JANO_HOST")
        jano_port = os.getenv("JANO_PORT")
        jano_name = os.getenv("JANO_NAME")

        if not all([jano_user, jano_pass, jano_host, jano_port, jano_name]):
            raise ValueError("Missing required environment variables")

        self.jano_engine = create_engine(
            f"oracle+oracledb://{jano_user}:{jano_pass}@{jano_host}:{jano_port}/{jano_name}",
            pool_size=engine_pool_size, max_overflow=engine_max_overflow
        )


settings = AppSettings()
