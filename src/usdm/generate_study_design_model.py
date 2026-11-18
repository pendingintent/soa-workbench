#!/usr/bin/env python3
"""Build InterventionalStudyDesign.model (USDM v4.0 Code-Output).

The model is a required Code from the SDTM Intervention Model Response
codelist (C99076, CORE DDF00216), stored on the ``soa`` row. Until one
is set, an empty Code keeps the required attribute present.
"""

from typing import Any, Dict

from soa_builder.web.db import _connect

MODEL_CODE_ID = "Code_StudyDesignModel"
CDISC_CODE_SYSTEM = "http://www.cdisc.org"


def build_usdm_intervention_model(soa_id: int) -> Dict[str, Any]:
    conn = _connect()
    try:
        row = conn.execute(
            "SELECT intervention_model_code, intervention_model_decode,"
            " intervention_model_version FROM soa WHERE id=?",
            (soa_id,),
        ).fetchone()
    finally:
        conn.close()
    code, decode, version = row if row else (None, None, None)
    return {
        "id": MODEL_CODE_ID,
        "extensionAttributes": [],
        "code": code or "",
        "codeSystem": CDISC_CODE_SYSTEM if code else "",
        "codeSystemVersion": (version or "") if code else "",
        "decode": (decode or "") if code else "",
        "instanceType": "Code",
    }
