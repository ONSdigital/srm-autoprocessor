import os
from pathlib import Path
from urllib.parse import quote

from srm_autoprocessor.common.strtobool import strtobool


class Config:
    LOGGING_LEVEL = os.getenv("LOGGING_LEVEL", "INFO")
    ENVIRONMENT = os.getenv("ENVIRONMENT", "PROD")

    # DB Config
    POSTGRES_USER = os.getenv("POSTGRES_USER")
    POSTGRES_PASSWORD = quote(os.getenv("POSTGRES_PASSWORD", "<PASSWORD>"))
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

    READINESS_FILE_PATH = Path(os.getenv("READINESS_FILE_PATH", "autoprocessor-ready"))
    DELETE_TEMP_FILE = strtobool(os.getenv("DELETE_TEMP_FILE", "False"))


def get_config() -> Config:
    if Config.ENVIRONMENT == "DEV":
        return DevelopmentConfig()
    elif Config.ENVIRONMENT == "TEST":
        return UnitTestConfig()
    elif Config.ENVIRONMENT == "INTEGRATION_TESTS":
        return IntegrationTestConfig()
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
    SAMPLE_LOCATION = os.getenv("SAMPLE_LOCATION", "./sample_files")


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
    SAMPLE_LOCATION = os.getenv("SAMPLE_LOCATION", str(Path(__file__).parent.joinpath("tests/resources")))
    DELETE_TEMP_FILE = strtobool(os.getenv("DELETE_TEMP_FILE", "True"))


class IntegrationTestConfig(DevelopmentConfig):
    DEBUG = True
    POSTGRES_PORT = os.getenv("POSTGRES_PORT", "16432")
    POSTGRES_USER = os.getenv("POSTGRES_USER", "appuser")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
    POSTGRES_DB = os.getenv("POSTGRES_DB", "rm")
    POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
    SQLALCHEMY_DATABASE_URI = (
        f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    )
    SAMPLE_LOCATION = os.getenv("SAMPLE_LOCATION", str(Path(__file__).parent.joinpath("tests/resources")))
    DELETE_TEMP_FILE = strtobool(os.getenv("DELETE_TEMP_FILE", "True"))


config = get_config()
