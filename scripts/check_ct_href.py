from soa_builder.web.utils import (
    get_latest_sdtm_ct_href,
    get_encounter_environment_sv,
    load_environmental_setting_options,
)

if __name__ == "__main__":
    href = get_latest_sdtm_ct_href()
    print(f"Latest SDTM CT href: {href}")

    submission_value = get_encounter_environment_sv(3, "C127785")
    print(f"Submission Value for C127785: {submission_value}")

    environment_list = load_environmental_setting_options()
    for e in environment_list:
        print(e)
