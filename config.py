import os
from pathlib import Path


class Config:
    LOGGING_LEVEL = os.getenv("LOGGING_LEVEL", "INFO")
    ENVIRONMENT = os.getenv("ENVIRONMENT", "PROD")

    # DB Config
    POSTGRES_USER = os.getenv("POSTGRES_USER")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
    POSTGRES_DB = os.getenv("POSTGRES_DB")
    POSTGRES_HOST = os.getenv("POSTGRES_HOST")
    POSTGRES_PORT = os.getenv("POSTGRES_PORT")
    SQLALCHEMY_DATABASE_URI = (
        f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    )
    RUN_MODE = os.getenv("RUN_MODE", "CLOUD")
    SAMPLE_LOCATION = os.getenv(
        "SAMPLE_LOCATION", "TEST-SAMPLE-FILES"
    )  # Change this to your desired sample files location

    READINESS_FILE_PATH = Path(os.getenv('READINESS_FILE_PATH', 'autoprocessor-ready'))



def get_config() -> Config:
    if Config.ENVIRONMENT == "DEV":
        return DevelopmentConfig()
    elif Config.ENVIRONMENT == "TEST":
        return UnitTestConfig()
    return Config()


class DevelopmentConfig(Config):
    DEBUG = False
    PORT = int(os.getenv("PORT", "9095"))
    HOST = os.getenv("HOST", "localhost")
    LOGGING_LEVEL = os.getenv("LOGGING_LEVEL", "DEBUG")

    # DB Config
    POSTGRES_USER = os.getenv("POSTGRES_USER", "appuser")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
    POSTGRES_DB = os.getenv("POSTGRES_DB", "rm")
    POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
    POSTGRES_PORT = os.getenv("POSTGRES_PORT", "6432")
    SQLALCHEMY_DATABASE_URI = (
        f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    )
    RUN_MODE = os.getenv("RUN_MODE", "LOCAL")
    # RUN_MODE = os.getenv("RUN_MODE", "CLOUD")
    SAMPLE_LOCATION = os.getenv("SAMPLE_LOCATION", "./sample_files")
    # SAMPLE_LOCATION = os.getenv("SAMPLE_LOCATION", "ssdc-rm-dev-sample-file-transfer-bucket")


class UnitTestConfig(DevelopmentConfig):
    DEBUG = True

    # DB config
    POSTGRES_USER = "dummy"
    POSTGRES_DB = "dummy_rm"
    POSTGRES_HOST = "dummy"
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
    POSTGRES_PORT = os.getenv("POSTGRES_PORT", "6432")
    SQLALCHEMY_DATABASE_URI = (
        f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    )


config = get_config()
