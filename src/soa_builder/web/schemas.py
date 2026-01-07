from typing import List, Optional

from pydantic import BaseModel


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


class EpochUpdate(BaseModel):
    name: Optional[str] = None
    epoch_label: Optional[str] = None
    epoch_description: Optional[str] = None


class VisitCreate(BaseModel):
    name: str
    label: Optional[str] = None
    epoch_id: Optional[int] = None
    description: Optional[str] = None


class VisitUpdate(BaseModel):
    name: Optional[str] = None
    label: Optional[str] = None
    epoch_id: Optional[int] = None
    description: Optional[str] = None


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


class MatrixVisit(BaseModel):
    name: str
    label: Optional[str] = None


class MatrixActivity(BaseModel):
    name: str
    statuses: List[str]


class MatrixImport(BaseModel):
    visits: List[MatrixVisit]
    activities: List[MatrixActivity]
    reset: bool = True
