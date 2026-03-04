import logging
import os

from fastapi import APIRouter, HTTPException, Request
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from ..utils import soa_exists
from ..db import _connect


router = APIRouter()
logger = logging.getLogger("soa_builder.web.routers.audits")
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(__file__), "..", "templates")
)


@router.get("/ui/soa/{soa_id}/audits", response_class=HTMLResponse)
def ui_list_audits(request: Request, soa_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    conn = _connect()
    # print("Connection made...{}".format(soa_id))

    # Get the Activity Audits
    activity_cur = conn.cursor()
    activity_cur.execute(
        "SELECT id, activity_id, action, before_json, after_json, performed_at FROM activity_audit WHERE soa_id=? ORDER BY id DESC LIMIT 20",
        (soa_id,),
    )
    activity_audits = [
        {
            "id": r[0],
            "activity_id": r[1],
            "action": r[2],
            "before_json": r[3],
            "after_json": r[4],
            "performed_at": r[5],
        }
        for r in activity_cur.fetchall()
    ]
    activity_cur.close()

    # Get the Arm Audits
    arm_cur = conn.cursor()
    arm_cur.execute(
        "SELECT id, arm_id, action, before_json, after_json, performed_at FROM arm_audit WHERE soa_id=? ORDER BY id DESC LIMIT 20",
        (soa_id,),
    )
    arm_audits = [
        {
            "id": r[0],
            "arm_id": r[1],
            "action": r[2],
            "before_json": r[3],
            "after_json": r[4],
            "performed_at": r[5],
        }
        for r in arm_cur.fetchall()
    ]
    arm_cur.close()

    # Get Epoch Audits
    epoch_cur = conn.cursor()
    epoch_cur.execute(
        "SELECT id, epoch_id, action, before_json, after_json, performed_at FROM epoch_audit WHERE soa_id=? ORDER BY id DESC LIMIT 20",
        (soa_id,),
    )
    epoch_audits = [
        {
            "id": r[0],
            "epoch_id": r[1],
            "action": r[2],
            "before_json": r[3],
            "after_json": r[4],
            "performed_at": r[5],
        }
        for r in epoch_cur.fetchall()
    ]
    epoch_cur.close()

    # Get Element Audits
    element_cur = conn.cursor()
    element_cur.execute(
        "SELECT id,element_id,action,before_json,after_json,performed_at FROM element_audit WHERE soa_id=? ORDER BY id DESC LIMIT 20",
        (soa_id,),
    )
    element_audits = [
        {
            "id": r[0],
            "element_id": r[1],
            "action": r[2],
            "before_json": r[3],
            "after_json": r[4],
            "performed_at": r[5],
        }
        for r in element_cur.fetchall()
    ]
    element_cur.close()

    # Get StudyCell Audits
    study_cell_cur = conn.cursor()
    study_cell_cur.execute(
        "SELECT id,study_cell_id,action,before_json,after_json,performed_at FROM study_cell_audit WHERE soa_id=? ORDER BY id DESC LIMIT 20",
        (soa_id,),
    )
    study_cell_audits = [
        {
            "id": r[0],
            "study_cell_id": r[1],
            "action": r[2],
            "before_json": r[3],
            "after_json": r[4],
            "performed_at": r[5],
        }
        for r in study_cell_cur.fetchall()
    ]
    study_cell_cur.close()

    # Get Transition Rule Audits
    transition_rule_cur = conn.cursor()
    transition_rule_cur.execute(
        "SELECT id,transition_rule_id,action,before_json,after_json,performed_at FROM transition_rule_audit WHERE soa_id=? ORDER BY id DESC LIMIT 20",
        (soa_id,),
    )
    transition_rule_audits = [
        {
            "id": r[0],
            "transition_rule_id": r[1],
            "action": r[2],
            "before_json": r[3],
            "after_json": r[4],
            "performed_at": r[5],
        }
        for r in transition_rule_cur.fetchall()
    ]
    transition_rule_cur.close()

    # Get Visit Audits
    visit_cur = conn.cursor()
    visit_cur.execute(
        "SELECT id,visit_id,action,before_json,after_json,performed_at FROM visit_audit WHERE soa_id=? ORDER BY id DESC LIMIT 20",
        (soa_id,),
    )
    visit_audits = [
        {
            "id": r[0],
            "visit_id": r[1],
            "action": r[2],
            "before_json": r[3],
            "after_json": r[4],
            "performed_at": r[5],
        }
        for r in visit_cur.fetchall()
    ]
    visit_cur.close()

    # Get Timing Audits
    timing_cur = conn.cursor()
    timing_cur.execute(
        "SELECT id,timing_id,action,before_json,after_json,performed_at FROM timing_audit WHERE soa_id=? ORDER BY id DESC LIMIT 20",
        (soa_id,),
    )
    timing_audits = [
        {
            "id": r[0],
            "timing_id": r[1],
            "action": r[2],
            "before_json": r[3],
            "after_json": r[4],
            "performed_at": r[5],
        }
        for r in timing_cur.fetchall()
    ]
    timing_cur.close()

    # Get Scheduled Activity Instances Audits
    instance_cur = conn.cursor()
    instance_cur.execute(
        "SELECT id,instance_id,action,before_json,after_json,performed_at FROM instance_audit WHERE soa_id=? ORDER BY id DESC LIMIT 20",
        (soa_id,),
    )
    instance_audits = [
        {
            "id": r[0],
            "instance_id": r[1],
            "action": r[2],
            "before_json": r[3],
            "after_json": r[4],
            "performed_at": r[5],
        }
        for r in instance_cur.fetchall()
    ]
    instance_cur.close()

    # Get Biomedical Concept Audits
    bc_cur = conn.cursor()
    bc_cur.execute(
        """SELECT id,biomedical_concept_id,action,before_json,after_json,performed_at FROM biomedical_concept_audit WHERE soa_id=? ORDER BY id DESC LIMIT 20""",
        (soa_id,),
    )
    bc_audits = [
        {
            "id": r[0],
            "biomedical_concept_id": r[1],
            "action": r[2],
            "before_json": r[3],
            "after_json": r[4],
            "performed_at": r[5],
        }
        for r in bc_cur.fetchall()
    ]
    bc_cur.close()

    return templates.TemplateResponse(
        request,
        "audits.html",
        {
            "activity_audits": activity_audits,
            "arm_audits": arm_audits,
            "epoch_audits": epoch_audits,
            "element_audits": element_audits,
            "study_cell_audits": study_cell_audits,
            "transition_rule_audits": transition_rule_audits,
            "visit_audits": visit_audits,
            "timing_audits": timing_audits,
            "instance_audits": instance_audits,
            "bc_audits": bc_audits,
            "soa_id": soa_id,
        },
    )
