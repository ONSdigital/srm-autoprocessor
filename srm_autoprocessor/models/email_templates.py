from typing import TYPE_CHECKING

from sqlalchemy import UUID, ForeignKey, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from srm_autoprocessor.models.base import SCHEMA_METADATA, Base
from srm_autoprocessor.models.survey import Survey

if TYPE_CHECKING:
    from .action_rule import ActionRule  # pragma: no cover


class EmailTemplate(Base):
    __tablename__ = "email_template"
    metadata = SCHEMA_METADATA

    pack_code = mapped_column(String, primary_key=True, nullable=False)
    template = mapped_column(JSONB, nullable=False)
    notify_template_id = mapped_column(UUID, nullable=False)
    description = mapped_column(String, nullable=False)
    notify_service_ref = mapped_column(String, nullable=False)

    # "metadata" is a SQL Alchemy keyword, use a different name for the column
    email_template_metadata = mapped_column(JSONB, name="metadata", nullable=True)

    action_rule_survey_email_template: Mapped[list["ActionRuleSurveyEmailTemplate"]] = relationship(
        "ActionRuleSurveyEmailTemplate", back_populates="email_template"
    )


class ActionRuleSurveyEmailTemplate(Base):
    __tablename__ = "action_rule_survey_email_template"
    metadata = SCHEMA_METADATA

    id = mapped_column(UUID, primary_key=True, nullable=False)
    survey_id = mapped_column(ForeignKey("survey.id"), nullable=False)
    email_template_pack_code = mapped_column(ForeignKey("email_template.pack_code"), nullable=False)

    email_template: Mapped["EmailTemplate"] = relationship(
        "EmailTemplate", back_populates="action_rule_survey_email_template"
    )
    survey: Mapped["Survey"] = relationship("Survey", back_populates="action_rule_survey_email_template")

    action_rule: Mapped[list["ActionRule"]] = relationship(
        "ActionRule", back_populates="action_rule_survey_email_template"
    )
