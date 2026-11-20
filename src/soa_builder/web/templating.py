"""Shared Jinja2 templates and static directory for the web app."""

from fastapi.templating import Jinja2Templates
import os

from .codelist_config import ORGANIZATION_TYPE_CODELIST


TEMPLATES_DIR = os.path.join(os.path.dirname(__file__), "templates")


STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")


os.makedirs(TEMPLATES_DIR, exist_ok=True)
os.makedirs(STATIC_DIR, exist_ok=True)
templates = Jinja2Templates(directory=TEMPLATES_DIR)
# organizations_partial.html names the codelist in its DDF00200 hint
templates.env.globals["org_type_codelist"] = ORGANIZATION_TYPE_CODELIST
