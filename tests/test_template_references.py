"""Every template name referenced in the web app must exist on disk.

A missing template only fails when its code path runs (the DSS detail
error path referenced a dss_detail.html that never existed).
"""

import re
from pathlib import Path

WEB = Path(__file__).resolve().parent.parent / "src" / "soa_builder" / "web"
TEMPLATE_NAME = re.compile(r"""["']([\w/-]+\.html)["']""")


def test_referenced_templates_exist():
    templates_dir = WEB / "templates"
    missing = sorted(
        f"{path.relative_to(WEB)}: {name}"
        for path in WEB.rglob("*.py")
        for name in TEMPLATE_NAME.findall(path.read_text())
        if not (templates_dir / name).exists()
    )
    assert not missing, "Templates referenced but missing:\n" + "\n".join(missing)
