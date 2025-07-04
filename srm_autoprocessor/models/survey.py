import uuid
from typing import TYPE_CHECKING

from sqlalchemy import UUID, Boolean, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from srm_autoprocessor.models.base import SCHEMA_METADATA, Base

if TYPE_CHECKING:
    from .collection_exercise import CollectionExercise  # pragma: no cover
    from .email_templates import ActionRuleSurveyEmailTemplate  # pragma: no cover


class Survey(Base):
    __tablename__ = "survey"
    metadata = SCHEMA_METADATA

    # Columns
    id = mapped_column(UUID, primary_key=True, default=uuid.uuid4)
    name = mapped_column(String, nullable=False)
    sample_validation_rules = mapped_column(JSONB, nullable=False)
    sample_definition_url = mapped_column(String, nullable=False)
    sample_with_header_row = mapped_column(Boolean, nullable=False)
    sample_separator = mapped_column(String(length=1), nullable=False)

    # "metadata" is a SQL Alchemy keyword, use a different name for the column
    survey_metadata = mapped_column(JSONB, name="metadata", nullable=True)

    collection_exercise: Mapped[list["CollectionExercise"]] = relationship(
        "CollectionExercise", back_populates="survey"
    )

    action_rule_survey_email_template: Mapped[list["ActionRuleSurveyEmailTemplate"]] = relationship(
        "ActionRuleSurveyEmailTemplate", back_populates="survey"
    )

    def as_dict(self) -> dict[str, str | bool | dict]:
        """Returns a dictionary representation of the Survey object, in types which are compatible with JSON."""
        return {
            "id": str(self.id),
            "name": self.name,
            "sample_validation_rules": self.sample_validation_rules,
            "metadata": self.survey_metadata,
            "sample_definition_url": self.sample_definition_url,
            "sample_with_header_row": self.sample_with_header_row,
            "sample_separator": self.sample_separator,
        }

    @classmethod
    def from_dict(cls, survey_dict: dict[str, str | bool | dict]) -> "Survey":
        """Class method to create a Survey object based on values from a dictionary.

        :raises: :class:`KeyError`: When any of the non-nullable columns aren't present in the dictionary.
        """
        new_survey = Survey(
            name=survey_dict["name"],
            sample_validation_rules=survey_dict["sample_validation_rules"],
            sample_definition_url=survey_dict["sample_definition_url"],
            sample_with_header_row=survey_dict["sample_with_header_row"],
            sample_separator=survey_dict["sample_separator"],
            survey_metadata=survey_dict.get("metadata"),
        )

        return new_survey
