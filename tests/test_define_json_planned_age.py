"""Define-JSON AGEMIN/AGEMAX with a one-sided planned age.

A planned age bound recorded as n/a is exported as ``null`` (USDM v4.1
behaviour), so the Define-JSON processor must treat a null bound as
absent instead of failing on it.
"""

from usdm.create_define_json import _planned_age_bound_present


def _range(low, high):
    def q(v):
        return None if v is None else {"value": v}

    return {"minValue": q(low), "maxValue": q(high)}


def test_null_maximum_is_absent():
    population = {"plannedAge": _range(18, None), "cohorts": []}

    assert _planned_age_bound_present(population, "minValue") is True
    assert _planned_age_bound_present(population, "maxValue") is False


def test_bound_from_a_cohort_counts():
    population = {
        "plannedAge": None,
        "cohorts": [{"plannedAge": _range(None, 65)}],
    }

    assert _planned_age_bound_present(population, "minValue") is False
    assert _planned_age_bound_present(population, "maxValue") is True


def test_no_population_age():
    assert _planned_age_bound_present({}, "minValue") is False
