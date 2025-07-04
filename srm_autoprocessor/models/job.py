import uuid
from typing import TYPE_CHECKING

from sqlalchemy import UUID, DateTime, Enum, ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from srm_autoprocessor.models.base import SCHEMA_METADATA, Base

if TYPE_CHECKING:
    from .collection_exercise import CollectionExercise  # pragma: no cover
    from .job_row import JobRow  # pragma: no cover


class Job(Base):
    __tablename__ = "job"
    metadata = SCHEMA_METADATA

    # Columns

    id = mapped_column(UUID, primary_key=True, default=uuid.uuid4)
    created_at = mapped_column(DateTime(timezone=True), nullable=False)
    created_by = mapped_column(String, nullable=False)
    last_updated_at = mapped_column(DateTime(timezone=True))
    file_name = mapped_column(String, nullable=False)
    file_id = mapped_column(UUID, nullable=False)
    file_row_count = mapped_column(Integer, nullable=False)
    error_row_count = mapped_column(Integer, nullable=False)
    staging_row_number = mapped_column(Integer, nullable=False)
    validating_row_number = mapped_column(Integer, nullable=False)
    processing_row_number = mapped_column(Integer, nullable=False)
    job_status = mapped_column(
        Enum(
            "FILE_UPLOADED",
            "STAGING_IN_PROGRESS",
            "VALIDATION_IN_PROGRESS",
            "VALIDATED_OK",
            "VALIDATED_WITH_ERRORS",
            "VALIDATED_TOTAL_FAILURE",
            "PROCESSING_IN_PROGRESS",
            "PROCESSED",
            "CANCELLED",
            name="job_status_enum",
        ),
        nullable=False,
    )
    job_type = mapped_column(
        Enum(
            "SAMPLE",
            "BULK_REFUSAL",
            "BULK_UPDATE_SAMPLE_SENSITIVE",
            "BULK_INVALID",
            "BULK_UPDATE_SAMPLE",
            name="job_type_enum",
        ),
        nullable=False,
    )
    processed_by = mapped_column(String)
    processed_at = mapped_column(DateTime(timezone=True))
    cancelled_by = mapped_column(String)
    cancelled_at = mapped_column(DateTime(timezone=True))
    fatal_error_description = mapped_column(String)
    collection_exercise_id = mapped_column(ForeignKey("collection_exercise.id"), nullable=False)

    collection_exercise: Mapped["CollectionExercise"] = relationship("CollectionExercise")
    job_rows: Mapped[list["JobRow"]] = relationship("JobRow", back_populates="job")

    def as_dict(self) -> dict[str, str | int | None]:
        """Returns a dictionary representation of the Job object, in types which are compatible with JSON."""
        return {
            "id": str(self.id),
            "created_at": self.created_at.isoformat(),
            "last_updated_at": self.last_updated_at.isoformat(),
            "file_name": self.file_name,
            "file_id": str(self.file_id),
            "file_row_count": self.file_row_count,
            "error_row_count": self.error_row_count,
            "staging_row_number": self.staging_row_number,
            "validating_row_number": self.validating_row_number,
            "processing_row_number": self.processing_row_number,
            "job_status": self.job_status,
            "job_type": self.job_type,
            "processed_by": self.processed_by,
            "processed_at": self.processed_at.isoformat() if self.processed_at else None,
            "cancelled_by": self.cancelled_by,
            "cancelled_at": self.cancelled_at.isoformat() if self.cancelled_at else None,
            "fatal_error_description": self.fatal_error_description,
            "collection_exercise_id": str(self.collection_exercise_id),
        }
