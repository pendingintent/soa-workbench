"""Ordered registry of database migrations.

Order is the historical boot order from ``app.py`` and must not change.
Append new migrations at the end (see ``migrations/README.md``).

Most migrations are one-shot schema changes: the runner records them in
``schema_migrations`` and skips them on later boots. Those in
``RECURRING`` act on data that current code (or rollback of older
snapshots) can still produce, so they keep running on every boot as
they always have. Each is a candidate for fixing at its write path.
"""

import os

from usdm.generate_biomedical_concept_properties import (
    populate_biomedical_concept_properties_for_all_soas,
)

from .legacy_amendments import (
    _migrate_add_amendment_geographic_scope_audit_table,
    _migrate_add_amendment_geographic_scope_table,
    _migrate_add_amendment_governance_date_audit_table,
    _migrate_add_amendment_governance_date_table,
    _migrate_add_amendment_subject_enrollment_audit_table,
    _migrate_add_amendment_subject_enrollment_table,
    _migrate_add_document_content_reference_audit_table,
    _migrate_add_document_content_reference_table,
    _migrate_add_governance_date_geographic_scope_table,
    _migrate_add_location_code_uid_to_geo_scope,
    _migrate_add_study_amendment_audit_table,
    _migrate_add_study_amendment_impact_audit_table,
    _migrate_add_study_amendment_impact_table,
    _migrate_add_study_amendment_reason_audit_table,
    _migrate_add_study_amendment_reason_table,
    _migrate_add_study_amendment_table,
    _migrate_add_study_change_audit_table,
    _migrate_add_study_change_table,
)
from .legacy_concepts import (
    _migrate_activity_concept_add_bc_category_name,
    _migrate_activity_concept_add_concept_group_uid,
    _migrate_activity_concept_add_dss,
    _migrate_activity_concept_add_href,
    _migrate_activity_concept_dss_add_display,
    _migrate_activity_concept_dss_add_extension_attribute_uid,
    _migrate_activity_surrogate_add_concept_group_uid,
    _migrate_add_activity_concept_crf_table,
    _migrate_add_activity_concept_dss_table,
    _migrate_add_activity_grouping_extension_table,
    _migrate_add_activity_surrogate_table,
    _migrate_add_bc_surrogate_audit_table,
    _migrate_add_bc_surrogate_table,
    _migrate_add_bcp_response_code_table,
    _migrate_add_concept_group_table,
    _migrate_add_decode_to_code_association,
    _migrate_backfill_biomedical_concept_codes,
    _migrate_backfill_code_association_decode,
    _migrate_backfill_crf_href_latest_version,
    _migrate_biomedical_concept_audit,
    _migrate_concept_group_add_cdisc_source_columns,
    _migrate_remap_code_association_codes,
    _migrate_repair_broken_bc_code_chains,
    _migrate_repoint_stale_bc_code_chains,
    _migrate_surrogate_add_concept_group_uid,
)
from .legacy_core import (
    _drop_unused_override_table,
    _migrate_activity_add_superscript,
    _migrate_activity_add_uid,
    _migrate_add_arm_uid,
    _migrate_add_epoch_id_to_visit,
    _migrate_add_epoch_label_desc,
    _migrate_add_epoch_seq,
    _migrate_add_epoch_type,
    _migrate_add_epoch_uid,
    _migrate_add_soa_id_indexes,
    _migrate_add_study_fields,
    _migrate_add_uid_counter_table,
    _migrate_arm_add_type_fields,
    _migrate_copy_cell_data,
    _migrate_drop_arm_element_link,
    _migrate_element_audit_columns,
    _migrate_element_id,
    _migrate_element_table,
    _migrate_epoch_add_superscript,
    _migrate_index_all_soa_id_tables,
    _migrate_instances_add_member_of_timeline,
    _migrate_matrix_cells_add_instance_id,
    _migrate_matrix_cells_add_superscript,
    _migrate_matrix_cells_backfill_instance_id,
    _migrate_rename_cell_table,
    _migrate_rollback_add_cells_skipped,
    _migrate_rollback_add_elements_restored,
    _migrate_rollback_add_encounters_instances,
    _migrate_soa_add_tool_extension_uids,
    _migrate_study_cell_add_order_index,
    _migrate_timing_add_member_of_timeline,
    _migrate_visit_add_label_desc,
    _migrate_visit_add_superscript,
    _migrate_visit_columns,
)
from .legacy_people_orgs import (
    _migrate_add_organization_audit_table,
    _migrate_add_organization_table,
    _migrate_add_person_audit_table,
    _migrate_add_person_name_fields,
    _migrate_add_person_table,
    _migrate_add_role_audit_table,
    _migrate_add_role_person_table,
    _migrate_add_role_table,
    _migrate_person_drop_job_title_notnull,
)
from .legacy_study_design import (
    _migrate_add_element_intervention_table,
    _migrate_add_endpoint_audit_table,
    _migrate_add_endpoint_table,
    _migrate_add_estimand_audit_table,
    _migrate_add_estimand_intervention_table,
    _migrate_add_estimand_table,
    _migrate_add_estimand_variable_table,
    _migrate_add_footnote_audit_table,
    _migrate_add_footnote_table,
    _migrate_add_indication_audit_table,
    _migrate_add_indication_code_table,
    _migrate_add_indication_table,
    _migrate_add_intercurrent_event_table,
    _migrate_add_objective_audit_table,
    _migrate_add_objective_table,
    _migrate_add_study_identifier_audit_table,
    _migrate_add_study_identifier_table,
    _migrate_add_study_intervention_audit_table,
    _migrate_add_study_intervention_code_table,
    _migrate_add_study_intervention_table,
    _migrate_add_study_title_audit_table,
    _migrate_add_study_title_table,
)
from .legacy_terminology import (
    _migrate_create_country_codes_table,
    _migrate_create_geographic_regions_table,
    _migrate_drop_ddf_terminology_tables,
    _migrate_drop_protocol_terminology_tables,
)


def _bcp_backfill() -> None:
    """Populate BC properties for every SoA unless disabled by env."""
    flag = os.environ.get("SOA_EAGER_BCP_POPULATION", "1").strip().lower()
    if flag not in ("0", "false"):
        populate_biomedical_concept_properties_for_all_soas()


MIGRATIONS = (
    ("migrate_study_cell_add_order_index", _migrate_study_cell_add_order_index),
    ("migrate_activity_concept_add_href", _migrate_activity_concept_add_href),
    ("migrate_activity_concept_add_dss", _migrate_activity_concept_add_dss),
    ("migrate_matrix_cells_add_instance_id", _migrate_matrix_cells_add_instance_id),
    (
        "migrate_matrix_cells_backfill_instance_id",
        _migrate_matrix_cells_backfill_instance_id,
    ),
    (
        "migrate_instances_add_member_of_timeline",
        _migrate_instances_add_member_of_timeline,
    ),
    ("migrate_timing_add_member_of_timeline", _migrate_timing_add_member_of_timeline),
    ("migrate_visit_columns", _migrate_visit_columns),
    ("migrate_add_epoch_type", _migrate_add_epoch_type),
    ("migrate_add_arm_uid", _migrate_add_arm_uid),
    ("migrate_drop_arm_element_link", _migrate_drop_arm_element_link),
    ("migrate_add_epoch_id_to_visit", _migrate_add_epoch_id_to_visit),
    ("migrate_visit_add_label_desc", _migrate_visit_add_label_desc),
    ("migrate_add_epoch_seq", _migrate_add_epoch_seq),
    ("migrate_add_epoch_label_desc", _migrate_add_epoch_label_desc),
    ("migrate_add_epoch_uid", _migrate_add_epoch_uid),
    ("migrate_add_study_fields", _migrate_add_study_fields),
    ("drop_unused_override_table", _drop_unused_override_table),
    ("migrate_element_table", _migrate_element_table),
    ("migrate_rename_cell_table", _migrate_rename_cell_table),
    ("migrate_copy_cell_data", _migrate_copy_cell_data),
    ("migrate_element_id", _migrate_element_id),
    ("migrate_rollback_add_elements_restored", _migrate_rollback_add_elements_restored),
    ("migrate_rollback_add_cells_skipped", _migrate_rollback_add_cells_skipped),
    (
        "migrate_rollback_add_encounters_instances",
        _migrate_rollback_add_encounters_instances,
    ),
    ("migrate_activity_add_uid", _migrate_activity_add_uid),
    ("migrate_arm_add_type_fields", _migrate_arm_add_type_fields),
    ("migrate_element_audit_columns", _migrate_element_audit_columns),
    ("migrate_biomedical_concept_audit", _migrate_biomedical_concept_audit),
    (
        "migrate_backfill_biomedical_concept_codes",
        _migrate_backfill_biomedical_concept_codes,
    ),
    ("migrate_repoint_stale_bc_code_chains", _migrate_repoint_stale_bc_code_chains),
    ("bcp_backfill", _bcp_backfill),
    ("migrate_add_soa_id_indexes", _migrate_add_soa_id_indexes),
    ("migrate_add_footnote_table", _migrate_add_footnote_table),
    ("migrate_add_footnote_audit_table", _migrate_add_footnote_audit_table),
    ("migrate_matrix_cells_add_superscript", _migrate_matrix_cells_add_superscript),
    ("migrate_activity_add_superscript", _migrate_activity_add_superscript),
    ("migrate_epoch_add_superscript", _migrate_epoch_add_superscript),
    ("migrate_visit_add_superscript", _migrate_visit_add_superscript),
    ("migrate_add_bc_surrogate_table", _migrate_add_bc_surrogate_table),
    ("migrate_add_activity_surrogate_table", _migrate_add_activity_surrogate_table),
    ("migrate_add_bc_surrogate_audit_table", _migrate_add_bc_surrogate_audit_table),
    ("migrate_add_concept_group_table", _migrate_add_concept_group_table),
    (
        "migrate_concept_group_add_cdisc_source_columns",
        _migrate_concept_group_add_cdisc_source_columns,
    ),
    (
        "migrate_activity_concept_add_concept_group_uid",
        _migrate_activity_concept_add_concept_group_uid,
    ),
    (
        "migrate_surrogate_add_concept_group_uid",
        _migrate_surrogate_add_concept_group_uid,
    ),
    (
        "migrate_activity_surrogate_add_concept_group_uid",
        _migrate_activity_surrogate_add_concept_group_uid,
    ),
    (
        "migrate_activity_concept_add_bc_category_name",
        _migrate_activity_concept_add_bc_category_name,
    ),
    (
        "migrate_drop_protocol_terminology_tables",
        _migrate_drop_protocol_terminology_tables,
    ),
    ("migrate_drop_ddf_terminology_tables", _migrate_drop_ddf_terminology_tables),
    ("migrate_add_activity_concept_dss_table", _migrate_add_activity_concept_dss_table),
    (
        "migrate_activity_concept_dss_add_display",
        _migrate_activity_concept_dss_add_display,
    ),
    (
        "migrate_activity_concept_dss_add_extension_attribute_uid",
        _migrate_activity_concept_dss_add_extension_attribute_uid,
    ),
    ("migrate_add_activity_concept_crf_table", _migrate_add_activity_concept_crf_table),
    ("migrate_add_objective_table", _migrate_add_objective_table),
    ("migrate_add_objective_audit_table", _migrate_add_objective_audit_table),
    ("migrate_add_endpoint_table", _migrate_add_endpoint_table),
    ("migrate_add_endpoint_audit_table", _migrate_add_endpoint_audit_table),
    ("migrate_add_study_amendment_table", _migrate_add_study_amendment_table),
    (
        "migrate_add_study_amendment_audit_table",
        _migrate_add_study_amendment_audit_table,
    ),
    (
        "migrate_add_study_amendment_reason_table",
        _migrate_add_study_amendment_reason_table,
    ),
    (
        "migrate_add_study_amendment_reason_audit_table",
        _migrate_add_study_amendment_reason_audit_table,
    ),
    (
        "migrate_add_study_amendment_impact_table",
        _migrate_add_study_amendment_impact_table,
    ),
    (
        "migrate_add_study_amendment_impact_audit_table",
        _migrate_add_study_amendment_impact_audit_table,
    ),
    ("migrate_add_study_change_table", _migrate_add_study_change_table),
    ("migrate_add_study_change_audit_table", _migrate_add_study_change_audit_table),
    (
        "migrate_add_document_content_reference_table",
        _migrate_add_document_content_reference_table,
    ),
    (
        "migrate_add_document_content_reference_audit_table",
        _migrate_add_document_content_reference_audit_table,
    ),
    ("migrate_add_bcp_response_code_table", _migrate_add_bcp_response_code_table),
    (
        "migrate_add_amendment_geographic_scope_table",
        _migrate_add_amendment_geographic_scope_table,
    ),
    (
        "migrate_add_amendment_geographic_scope_audit_table",
        _migrate_add_amendment_geographic_scope_audit_table,
    ),
    (
        "migrate_add_amendment_subject_enrollment_table",
        _migrate_add_amendment_subject_enrollment_table,
    ),
    (
        "migrate_add_amendment_subject_enrollment_audit_table",
        _migrate_add_amendment_subject_enrollment_audit_table,
    ),
    (
        "migrate_add_amendment_governance_date_table",
        _migrate_add_amendment_governance_date_table,
    ),
    (
        "migrate_add_amendment_governance_date_audit_table",
        _migrate_add_amendment_governance_date_audit_table,
    ),
    (
        "migrate_add_governance_date_geographic_scope_table",
        _migrate_add_governance_date_geographic_scope_table,
    ),
    ("migrate_add_decode_to_code_association", _migrate_add_decode_to_code_association),
    ("migrate_remap_code_association_codes", _migrate_remap_code_association_codes),
    (
        "migrate_backfill_code_association_decode",
        _migrate_backfill_code_association_decode,
    ),
    ("migrate_create_country_codes_table", _migrate_create_country_codes_table),
    (
        "migrate_create_geographic_regions_table",
        _migrate_create_geographic_regions_table,
    ),
    (
        "migrate_add_location_code_uid_to_geo_scope",
        _migrate_add_location_code_uid_to_geo_scope,
    ),
    ("migrate_repair_broken_bc_code_chains", _migrate_repair_broken_bc_code_chains),
    ("migrate_add_study_title_table", _migrate_add_study_title_table),
    ("migrate_add_study_title_audit_table", _migrate_add_study_title_audit_table),
    ("migrate_add_organization_table", _migrate_add_organization_table),
    ("migrate_add_organization_audit_table", _migrate_add_organization_audit_table),
    ("migrate_add_role_table", _migrate_add_role_table),
    ("migrate_add_role_audit_table", _migrate_add_role_audit_table),
    ("migrate_add_study_intervention_table", _migrate_add_study_intervention_table),
    (
        "migrate_add_study_intervention_code_table",
        _migrate_add_study_intervention_code_table,
    ),
    (
        "migrate_add_study_intervention_audit_table",
        _migrate_add_study_intervention_audit_table,
    ),
    ("migrate_add_estimand_table", _migrate_add_estimand_table),
    (
        "migrate_add_estimand_intervention_table",
        _migrate_add_estimand_intervention_table,
    ),
    ("migrate_add_intercurrent_event_table", _migrate_add_intercurrent_event_table),
    ("migrate_add_estimand_audit_table", _migrate_add_estimand_audit_table),
    ("migrate_add_estimand_variable_table", _migrate_add_estimand_variable_table),
    ("migrate_add_indication_table", _migrate_add_indication_table),
    ("migrate_add_indication_code_table", _migrate_add_indication_code_table),
    ("migrate_add_indication_audit_table", _migrate_add_indication_audit_table),
    ("migrate_add_person_table", _migrate_add_person_table),
    ("migrate_add_person_audit_table", _migrate_add_person_audit_table),
    ("migrate_add_role_person_table", _migrate_add_role_person_table),
    ("migrate_person_drop_job_title_notnull", _migrate_person_drop_job_title_notnull),
    ("migrate_add_person_name_fields", _migrate_add_person_name_fields),
    ("migrate_add_study_identifier_table", _migrate_add_study_identifier_table),
    (
        "migrate_add_study_identifier_audit_table",
        _migrate_add_study_identifier_audit_table,
    ),
    ("migrate_soa_add_tool_extension_uids", _migrate_soa_add_tool_extension_uids),
    ("migrate_add_element_intervention_table", _migrate_add_element_intervention_table),
    (
        "migrate_backfill_crf_href_latest_version",
        _migrate_backfill_crf_href_latest_version,
    ),
    (
        "migrate_add_activity_grouping_extension_table",
        _migrate_add_activity_grouping_extension_table,
    ),
    ("migrate_add_uid_counter_table", _migrate_add_uid_counter_table),
    ("migrate_index_all_soa_id_tables", _migrate_index_all_soa_id_tables),
)

# name -> why it must keep running on every boot
RECURRING = {
    "migrate_matrix_cells_backfill_instance_id": (
        "rollback of legacy snapshots inserts visit-keyed cells"
    ),
    "migrate_add_arm_uid": ("assigns StudyArm_N to any arm with a NULL arm_uid"),
    "migrate_add_epoch_uid": (
        "assigns StudyEpoch_N to any epoch with a NULL epoch_uid"
    ),
    "migrate_element_id": (
        "assigns/repairs StudyElement_N; rollback of old snapshots restores elements without one"
    ),
    "migrate_backfill_biomedical_concept_codes": (
        "repairs BCs missing their code chain"
    ),
    "migrate_repoint_stale_bc_code_chains": (
        "repairs BC code chains left stale by edits"
    ),
    "bcp_backfill": ("populates BC properties for all SoAs (SOA_EAGER_BCP_POPULATION)"),
    "migrate_remap_code_association_codes": (
        "re-resolves stored codes against CDISC CT"
    ),
    "migrate_backfill_code_association_decode": (
        "fills decodes for code associations created without one"
    ),
    "migrate_repair_broken_bc_code_chains": ("repairs BCs whose code row is missing"),
    "migrate_backfill_crf_href_latest_version": (
        "points CRF hrefs at the latest package version"
    ),
    "migrate_index_all_soa_id_tables": (
        "indexes soa-scoped tables added by any later migration"
    ),
}
