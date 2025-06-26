import uuid
from typing import TYPE_CHECKING

from sqlalchemy import UUID, Enum, ForeignKey, Integer
from sqlalchemy.dialects.postgresql import BYTEA, JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from srm_autoprocessor.models.base import SCHEMA_METADATA, Base

if TYPE_CHECKING:
    from .job import Job


class JobRow(Base):
    __tablename__ = "job_row"
    metadata = SCHEMA_METADATA

    id = mapped_column(UUID, primary_key=True, nullable=False, default=uuid.uuid4)
    job_row_status = mapped_column(
        Enum(
            "STAGED",
            "VALIDATED_OK",
            "VALIDATED_ERROR",
            "PROCESSED",
            name="job_row_status_enum",
        ),
        nullable=False,
    )
    original_row_data = mapped_column(BYTEA, nullable=False)
    original_row_line_number = mapped_column(Integer, nullable=False)
    row_data = mapped_column(JSONB, nullable=True)
    validation_error_descriptions = mapped_column(BYTEA, nullable=True)
    job_id = mapped_column(ForeignKey("job.id"), nullable=False)

    job: Mapped["Job"] = relationship("Job", back_populates="job_rows")
