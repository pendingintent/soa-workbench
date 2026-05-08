# I need to capture more information from the user when they create a freeze.
- Review `schema/USDM_API_v4.0.0.json` and the python module `usdm` and create pydantic models in `src/soa_builder/web/schemas.py` for `study_amendment`, `study_amendment_impact`, `study_amendment_reason`
- Add a checkbox that the user can use when creating a freeze to identify a freeze as a protocol amendment (freezes.py & freezes.html)
- If the user identifies the freeze as a protocol amendment, the user must be presented with a form to add properties for the amendment that correspond to `study_amendment`
    - The UID for an Amendment must be "StudyAmendment_{n}"
    - A primary reason must be required when creating an amendment
    - The UID for a `primaryReason` must be "StudyAmendmentReason_{n}"
    - The user must choose the code for a `primaryReason` from a drop-down select populated with `submission_value` from API call to the DDF terminology with codelist_code=C207415.
    - If the user chooses codelist_code=C207415 and code=C17649, the user must complete an text box containing the value for `otherReason`.
    - The user can create 0..n `secondaryReasons` for an amendment.  This is optional.
    - The UID for a `secondaryReason` must be "StudyAmendmentReason_{n}"
    - The user must choose the code for a `secondaryReason` from a drop-down select populated with `submission_value` from API call to the DDF terminology with codelist_code=C207415.
    - If the user chooses codelist_code=C207415 and code=C17649, the user must complete an text box containing the value for `otherReason`.
    - The user must be able to enter 0..n `changes` to be associated with the amendment.
    - The UID for a `change` must be "StudyChange_{n}"
    - The user must be able to add 0..n `changeSections`
    - The UID for a `changedSection` must be "DocumentContentReference_{n}"
    - The user must be able to add `impacts` to an Amendment.
    - The UID for an `impact` must be "SteudyAmendmentImpact_{n}"
    - The use must choose a `type` for an `impact` from a drop-down select populated with `submission_value` from API call to the DDF terminology with codelist_code=C215481
    - Defer implmentation of `geographicScopes`, `enrollments` and `dateValues`
- Create new database objects as required and follow the same design principals used elsewhere in the project.



## Coding Notes:
- UIDs are auto-generated monotonically, scanning live + audit tables so deleted UIDs are never recycled.
- Follow the design principals used for other classes in the USDM.
- Use caching where appropriate to prevent multiple calls to the API and enhance performance of the application.