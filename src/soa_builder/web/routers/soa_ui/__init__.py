"""HTMX UI for editing a SoA (moved from app.py).

Split into submodules; this package re-exports the original names so
existing imports keep working. Submodules are imported in route
registration order.
"""

from ._shared import *  # noqa: F401,F403
from .editor import (  # noqa: F401
    ui_create_soa,
    ui_delete_soa,
    ui_edit,
    ui_help,
    ui_index,
    ui_update_meta,
    ui_usdm_model_explorer,
)
from .elements import (  # noqa: F401
    ui_add_element,
    ui_delete_element,
    ui_set_element_transition_end_rule,
    ui_set_element_transition_start_rule,
    ui_update_element,
)
from .cells import (  # noqa: F401
    ui_activity_concepts_cell,
    ui_activity_superscript_edit,
    ui_activity_superscript_save,
    ui_activity_superscript_view,
    ui_cell_superscript_edit,
    ui_cell_superscript_save,
    ui_cell_superscript_view,
    ui_encounter_superscript_edit,
    ui_encounter_superscript_save,
    ui_encounter_superscript_view,
    ui_epoch_superscript_edit,
    ui_epoch_superscript_save,
    ui_epoch_superscript_view,
    ui_set_cell,
    ui_toggle_cell,
    ui_toggle_cell_instance,
)
from .transitions import (  # noqa: F401
    ui_set_timing,
    ui_set_transition_end_rule,
    ui_set_visit_transition_start_rule,
)
from .activities import (  # noqa: F401
    ui_add_activity,
    ui_add_activity_concept,
    ui_delete_activity,
    ui_remove_activity_concept,
    ui_reorder_activities,
    ui_set_activity_concepts,
)
from ._shared import (  # noqa: F401
    _SOA_CASCADE_TABLES,
    _cleanup_orphaned_concept_rows,
    _concept_cache,
    _connect,
    _enrich_biomedical_concept_bg,
    _enrich_code_bg,
    _fetch_arms_for_edit,
    _fetch_matrix,
    _get_activity_concepts,
    _get_activity_surrogates,
    _get_bc_categories_for_cell,
    _get_cdisc_api_key,
    _get_concept_groups_for_cell,
    _get_concepts_override,
    _get_countries_options,
    _get_next_code_uid,
    _get_next_concept_uid,
    _get_org_type_options,
    _get_role_type_options,
    _html,
    _list_organizations,
    _list_roles,
    _list_study_cells,
    _record_activity_audit,
    _record_element_audit,
    _record_reorder_audit,
    _record_visit_audit,
    _render_activity_td,
    _render_cell_td,
    _render_encounter_th,
    _render_epoch_th,
    _table_has_columns,
    _upsert_biomedical_concept,
)
