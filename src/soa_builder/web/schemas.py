import re
from typing import List, Optional

from pydantic import BaseModel, field_validator, model_validator

# ISO 8601 duration pattern supporting both standard (-P2D) and USDM (P-2D) conventions
_ISO8601_DURATION_RE = re.compile(
    r"^-?P-?(?:(\d+)Y)?(?:(\d+)M)?(?:(\d+)W)?(?:(\d+)D)?(?:T(?=\d)(\d+H)?(\d+M)?(\d+S)?)?$"
)


def _validate_iso8601_duration(v: Optional[str]) -> Optional[str]:
    if v is None:
        return v
    v = v.strip()
    if not v:
        return None
    m = _ISO8601_DURATION_RE.match(v)
    if not m or not any(m.group(i) is not None for i in range(1, 8)):
        raise ValueError(
            f"'{v}' is not a valid ISO 8601 duration (e.g. P1D, P2W, PT8H, -P2D)"
        )
    return v


def _validate_window_all_or_none(
    window_lower: Optional[str],
    window_upper: Optional[str],
    window_label: Optional[str],
) -> None:
    """Enforce that window_lower, window_upper, and window_label are all provided or all absent."""

    def _present(v: Optional[str]) -> bool:
        return v is not None and v.strip() != ""

    provided = [_present(window_lower), _present(window_upper), _present(window_label)]
    if any(provided) and not all(provided):
        missing = []
        if not _present(window_lower):
            missing.append("window_lower")
        if not _present(window_upper):
            missing.append("window_upper")
        if not _present(window_label):
            missing.append("window_label")
        raise ValueError(
            f"Window fields are all-or-nothing: missing {', '.join(missing)}"
        )


class InstanceUpdate(BaseModel):
    name: Optional[str] = None
    label: Optional[str] = None
    description: Optional[str] = None
    default_condition_uid: Optional[str] = None
    epoch_uid: Optional[str] = None
    timeline_id: Optional[str] = None
    timeline_exit_id: Optional[str] = None
    encounter_uid: Optional[str] = None
    member_of_timeline: Optional[str] = None


class InstanceCreate(BaseModel):
    name: str
    label: Optional[str] = None
    description: Optional[str] = None
    default_condition_uid: Optional[str] = None
    epoch_uid: Optional[str] = None
    timeline_id: Optional[str] = None
    timeline_exit_id: Optional[str] = None
    encounter_uid: Optional[str] = None
    member_of_timeline: Optional[str] = None


class TimingCreate(BaseModel):
    name: str
    label: Optional[str] = None
    description: Optional[str] = None
    type: Optional[str] = None
    value: Optional[str] = None
    value_label: Optional[str] = None
    relative_to_from: Optional[str] = None
    relative_from_schedule_instance: Optional[str] = None
    relative_to_schedule_instance: Optional[str] = None
    window_label: Optional[str] = None
    window_upper: Optional[str] = None
    window_lower: Optional[str] = None
    member_of_timeline: Optional[str] = None

    @field_validator("value", "window_lower", "window_upper", mode="before")
    @classmethod
    def check_iso8601_duration(cls, v: Optional[str]) -> Optional[str]:
        return _validate_iso8601_duration(v)

    @model_validator(mode="after")
    def check_window_all_or_none(self) -> "TimingCreate":
        _validate_window_all_or_none(
            self.window_lower, self.window_upper, self.window_label
        )
        return self


class TimingUpdate(BaseModel):
    name: str
    label: Optional[str] = None
    description: Optional[str] = None
    type: Optional[str] = None
    value: Optional[str] = None
    value_label: Optional[str] = None
    relative_to_from: Optional[str] = None
    relative_from_schedule_instance: Optional[str] = None
    relative_to_schedule_instance: Optional[str] = None
    window_label: Optional[str] = None
    window_upper: Optional[str] = None
    window_lower: Optional[str] = None
    member_of_timeline: Optional[str] = None

    @field_validator("value", "window_lower", "window_upper", mode="before")
    @classmethod
    def check_iso8601_duration(cls, v: Optional[str]) -> Optional[str]:
        return _validate_iso8601_duration(v)

    @model_validator(mode="after")
    def check_window_all_or_none(self) -> "TimingUpdate":
        _validate_window_all_or_none(
            self.window_lower, self.window_upper, self.window_label
        )
        return self


class ScheduleTimelineCreate(BaseModel):
    name: str
    label: Optional[str] = None
    description: Optional[str] = None
    main_timeline: Optional[bool] = None
    entry_condition: Optional[str] = None
    entry_id: Optional[str] = None
    exit_id: Optional[str] = None


class ScheduleTimelineUpdate(BaseModel):
    name: str
    label: Optional[str] = None
    description: Optional[str] = None
    main_timeline: Optional[bool] = None
    entry_condition: Optional[str] = None
    entry_id: Optional[str] = None
    exit_id: Optional[str] = None


class ActivityCreate(BaseModel):
    name: str
    label: Optional[str] = None
    description: Optional[str] = None


class ActivityUpdate(BaseModel):
    name: Optional[str] = None
    label: Optional[str] = None
    description: Optional[str] = None


class BulkActivities(BaseModel):
    names: List[str]


class ElementCreate(BaseModel):
    name: str
    label: Optional[str] = None
    description: Optional[str] = None
    testrl: Optional[str] = None
    teenrl: Optional[str] = None


class ElementUpdate(BaseModel):
    name: Optional[str] = None
    label: Optional[str] = None
    description: Optional[str] = None
    testrl: Optional[str] = None
    teenrl: Optional[str] = None


class EpochCreate(BaseModel):
    name: str
    epoch_label: Optional[str] = None
    epoch_description: Optional[str] = None
    type: Optional[str] = None


class EpochUpdate(BaseModel):
    name: Optional[str] = None
    epoch_label: Optional[str] = None
    epoch_description: Optional[str] = None
    type: Optional[str] = None


class VisitCreate(BaseModel):
    name: str
    label: Optional[str] = None
    epoch_id: Optional[int] = None
    description: Optional[str] = None
    type: Optional[str] = None
    transitionStartRule: Optional[str] = None
    transitionEndRule: Optional[str] = None
    scheduledAtId: Optional[str] = None
    environmentalSettings: Optional[str] = None
    contactModes: Optional[str] = None


class VisitUpdate(BaseModel):
    name: Optional[str] = None
    label: Optional[str] = None
    epoch_id: Optional[int] = None
    description: Optional[str] = None
    type: Optional[str] = None
    transitionStartRule: Optional[str] = None
    transitionEndRule: Optional[str] = None
    scheduledAtId: Optional[str] = None
    environmentalSettings: Optional[str] = None
    contactModes: Optional[str] = None


class ArmCreate(BaseModel):
    name: str
    label: Optional[str] = None
    description: Optional[str] = None
    type: Optional[str] = None
    data_origin_type: Optional[str] = None


class ArmUpdate(BaseModel):
    name: Optional[str] = None
    label: Optional[str] = None
    description: Optional[str] = None
    type: Optional[str] = None
    data_origin_type: Optional[str] = None


class RuleCreate(BaseModel):
    name: str
    label: Optional[str] = None
    description: Optional[str] = None
    text: Optional[str] = None


class RuleUpdate(BaseModel):
    name: Optional[str] = None
    label: Optional[str] = None
    description: Optional[str] = None
    text: Optional[str] = None


class SOACreate(BaseModel):
    name: str
    study_id: Optional[str] = None
    study_label: Optional[str] = None
    study_description: Optional[str] = None


class SOAMetadataUpdate(BaseModel):
    study_id: Optional[str] = None
    study_label: Optional[str] = None
    study_description: Optional[str] = None


# moved from app.py
class ConceptsUpdate(BaseModel):
    concept_codes: List[str]


class FreezeCreate(BaseModel):
    version_label: Optional[str] = None


class CellCreate(BaseModel):
    visit_id: int
    activity_id: int
    status: str


class MatrixInstance(BaseModel):
    name: str
    label: Optional[str] = None


class MatrixActivity(BaseModel):
    name: str
    statuses: List[str]


class MatrixImport(BaseModel):
    instances: List[MatrixInstance]
    activities: List[MatrixActivity]
    reset: bool = True


class StudyCellCreate(BaseModel):
    arm_uid: Optional[str] = None
    epoch_uid: Optional[str] = None
    element_uid: Optional[str] = None


class StudyCellUpdate(BaseModel):
    arm_uid: Optional[str] = None
    epoch_uid: Optional[str] = None
    element_uid: Optional[str] = None


class DecisionInstanceCreate(BaseModel):
    name: str
    label: Optional[str] = None
    description: Optional[str] = None
    default_condition_uid: Optional[str] = None
    epoch_uid: Optional[str] = None
    member_of_timeline: Optional[str] = None


class DecisionInstanceUpdate(BaseModel):
    name: Optional[str] = None
    label: Optional[str] = None
    description: Optional[str] = None
    default_condition_uid: Optional[str] = None
    epoch_uid: Optional[str] = None
    member_of_timeline: Optional[str] = None


class ConditionAssignmentCreate(BaseModel):
    name: str
    label: Optional[str] = None
    description: Optional[str] = None
    condition: Optional[str] = None
    decision_instance_uid: Optional[str] = None
    condition_target_uid: Optional[str] = None


class ConditionAssignmentUpdate(BaseModel):
    name: Optional[str] = None
    label: Optional[str] = None
    description: Optional[str] = None
    condition: Optional[str] = None
    decision_instance_uid: Optional[str] = None
    condition_target_uid: Optional[str] = None


class BCSurrogateCreate(BaseModel):
    name: str
    label: Optional[str] = None
    description: Optional[str] = None
    reference: Optional[str] = None


class BCSurrogateUpdate(BaseModel):
    name: Optional[str] = None
    label: Optional[str] = None
    description: Optional[str] = None
    reference: Optional[str] = None


class FootnoteCreate(BaseModel):
    name: str
    label: Optional[str] = None
    description: Optional[str] = None
    text: Optional[str] = None
    dictionary_uid: Optional[str] = None


class FootnoteUpdate(BaseModel):
    name: Optional[str] = None
    label: Optional[str] = None
    description: Optional[str] = None
    text: Optional[str] = None
    dictionary_uid: Optional[str] = None
