import uuid
from datetime import datetime
from typing import Any

import pytz
from sqlalchemy import UUID, Boolean, DateTime, Enum, ForeignKey, Integer, String
from sqlalchemy.dialects.postgresql import BYTEA, JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from srm_autoprocessor.models.base import SCHEMA_METADATA, Base
from srm_autoprocessor.models.collection_exercise import CollectionExercise
from srm_autoprocessor.models.email_templates import ActionRuleSurveyEmailTemplate


class ActionRule(Base):
    __tablename__ = "action_rule"
    metadata = SCHEMA_METADATA

    # Columns
    id = mapped_column(UUID, primary_key=True, nullable=False, default=uuid.uuid4)
    action_rule_status = mapped_column(
        Enum(
            "SCHEDULED", "SELECTING_CASES", "PROCESSING_CASES", "COMPLETED", "ERRORED", name="action_rule_status_enum"
        ),
        nullable=False,
        default="SCHEDULED",
    )
    classifiers = mapped_column(BYTEA, nullable=True)
    created_by = mapped_column(String, nullable=False)
    description = mapped_column(String, nullable=True)
    email_column = mapped_column(String, nullable=True)
    has_triggered = mapped_column(Boolean, default=False, nullable=False)
    phone_number_column = mapped_column(String, nullable=True)
    selected_case_count = mapped_column(Integer, nullable=True)
    trigger_date_time = mapped_column(DateTime(timezone=True), nullable=False)
    type = mapped_column(
        Enum("EXPORT_FILE", "OUTBOUND_TELEPHONE", "FACE_TO_FACE", "DEACTIVATE_UAC", "SMS", "EMAIL", "EQ_FLUSH"),
        nullable=False,
    )
    uac_metadata = mapped_column(JSONB, nullable=True)
    collection_exercise_id = mapped_column(ForeignKey("collection_exercise.id"), nullable=False)
    email_template_pack_code = mapped_column(
        ForeignKey("action_rule_survey_email_template.email_template_pack_code"), nullable=True
    )
    export_file_template_pack_code = mapped_column(String, nullable=True)
    sms_template_pack_code = mapped_column(String, nullable=True)

    collection_exercise: Mapped["CollectionExercise"] = relationship("CollectionExercise", back_populates="action_rule")
    action_rule_survey_email_template: Mapped["ActionRuleSurveyEmailTemplate"] = relationship(
        "ActionRuleSurveyEmailTemplate", back_populates="action_rule"
    )

    def as_dict(self) -> dict[str, Any]:
        """Returns a dictionary representation of the ActionRule object, in types which are compatible with JSON."""
        return {
            "id": str(self.id),
            "status": self.action_rule_status,
            "classifiers": self.classifiers.decode("utf-8") if self.classifiers else None,
            "created_by": self.created_by,
            "description": self.description,
            "email_column": self.email_column,
            "has_triggered": self.has_triggered,
            "phone_number_column": self.phone_number_column,
            "selected_case_count": self.selected_case_count,
            "trigger_date_time": self.trigger_date_time.isoformat(),
            "type": self.type,
            "uac_metadata": self.uac_metadata,
            "collection_exercise_id": str(self.collection_exercise_id),
            "email_template_pack_code": self.email_template_pack_code,
            "export_file_template_pack_code": self.export_file_template_pack_code,
            "sms_template_pack_code": self.sms_template_pack_code,
        }

    @classmethod
    def from_dict(cls, action_rule_dict: dict[str, Any]) -> "ActionRule":
        """Class method to create an ActionRule object based on values from a dictionary.

        :raises: :class:`KeyError`: When any of the non-nullable columns aren't present in the dictionary.
        """
        new_action_rule = ActionRule(
            created_by="dummy_user",
            description=action_rule_dict["description"],
            trigger_date_time=datetime.strptime(
                action_rule_dict["trigger_date_time"], "%Y-%m-%dT%H:%M:%S%z"
            ).astimezone(pytz.utc),
            type=action_rule_dict["type"],
        )

        return new_action_rule
