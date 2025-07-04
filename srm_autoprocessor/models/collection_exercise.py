import uuid
from datetime import datetime
from typing import TYPE_CHECKING, Any

import pytz
from sqlalchemy import UUID, DateTime, ForeignKey, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from srm_autoprocessor.models.base import SCHEMA_METADATA, Base

if TYPE_CHECKING:
    from .action_rule import ActionRule  # pragma: no cover
    from .survey import Survey  # pragma: no cover


class CollectionExercise(Base):
    __tablename__ = "collection_exercise"
    metadata = SCHEMA_METADATA

    # Columns
    id = mapped_column(UUID, primary_key=True, default=uuid.uuid4)
    name = mapped_column(String, nullable=False)
    collection_instrument_selection_rules = mapped_column(JSONB, nullable=False)
    reference = mapped_column(String, nullable=False)
    start_date = mapped_column(DateTime(timezone=True), nullable=False)
    end_date = mapped_column(DateTime(timezone=True), nullable=False)
    survey_id = mapped_column(ForeignKey("survey.id"), nullable=False)

    # "metadata" is a SQL Alchemy keyword, use a different name for the column
    collection_exercise_metadata = mapped_column(JSONB, name="metadata", nullable=True)

    survey: Mapped["Survey"] = relationship("Survey", back_populates="collection_exercise")

    action_rule: Mapped[list["ActionRule"]] = relationship("ActionRule", back_populates="collection_exercise")

    def as_dict(self) -> dict[str, str | dict[str, Any]]:
        """Returns a dictionary representation of the CollectionExercise object, in types which are
        compatible with JSON.
        """
        return {
            "id": str(self.id),
            "name": self.name,
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat(),
            "collection_instrument_selection_rules": self.collection_instrument_selection_rules,
            "reference": self.reference,
            "metadata": self.collection_exercise_metadata,
            "survey_name": self.survey.name,
        }

    @classmethod
    def from_dict(cls, collex_dict: dict[str, Any]) -> "CollectionExercise":
        """Creates a new CollectionExercise object from a dictionary."""
        new_collex = CollectionExercise(
            name=collex_dict["name"],
            collection_instrument_selection_rules=collex_dict["collection_instrument_selection_rules"],
            reference=collex_dict["reference"],
            start_date=datetime.strptime(collex_dict["start_date"], "%Y-%m-%dT%H:%M:%S%z").astimezone(pytz.utc),
            end_date=datetime.strptime(collex_dict["end_date"], "%Y-%m-%dT%H:%M:%S%z").astimezone(pytz.utc),
            collection_exercise_metadata=collex_dict.get("metadata"),
        )
        return new_collex
