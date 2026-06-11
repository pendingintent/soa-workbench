import json
import os
import tempfile
from typing import Any, Dict

from usdm.generate_usdm import build_usdm
from usdm.create_define_json import USDMDefineJSONProcessor


def build_define_json(
    soa_id: int,
    sdtmct: str,
    sdtmig: str = "3.4",
    cosmosversion: str = "v2",
    studyversion: int = 0,
    studydesign: int = 0,
    docversion: int = 0,
) -> Dict[str, Any]:
    usdm_data = build_usdm(soa_id)

    with tempfile.TemporaryDirectory() as tmpdir:
        usdm_path = os.path.join(tmpdir, "usdm.json")
        output_path = os.path.join(tmpdir, "define.json")

        with open(usdm_path, "w") as fh:
            json.dump(usdm_data, fh)

        processor = USDMDefineJSONProcessor(
            usdm_file=usdm_path,
            output_template=output_path,
            sdtmig=sdtmig,
            sdtmct=sdtmct,
            studyversion=studyversion,
            studydesign=studydesign,
            docversion=docversion,
            cdisc_api_key=None,
            cosmosversion=cosmosversion,
            debug=False,
        )
        processor.process()

    return processor.template
