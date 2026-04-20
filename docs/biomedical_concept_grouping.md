# Biomedical Concepts

## New Features
- Biomedical Concepts can be grouped together using tags in the UI.
- Tags are first created by the user.
- User can select from a list of concepts to apply tags as well as search for a particular concept.
- Biomedical Concept Surrogates can be assigned to a concept group.
- A biomedical concept category can be assigned to a group.


## Architecture
- The biomedical concept groupings ARE NOT soa specific, and once created, can be used in any SOA.
- New database table to store name, label, description, concept_group_uid.
- New database table to store the relationship between a concept group and the assigned concepts.
- Concept group can be assigned to an activity.  The assigned concepts in the group will follow the same behavior as a biomedical concept assigned individually.


## Web front end
- New page for the creation of a group and assignment of concepts to the group.
- New menu bar section to organize the biomedical concept and dataset specialization pages.

## Approach
- Reuse code/functions where possible.
- Existing behavior for the application and biomedical concepts is preserved.

