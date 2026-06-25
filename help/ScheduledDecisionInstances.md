# How ScheduledDecisionInstances are used with ScheduledActivityInstances in USDM:

1. What a ScheduledDecisionInstance is and its purpose
2. How it relates to ScheduledActivityInstance
3. The structure/attributes of each
4. How they are linked (e.g. defaultConditionId, conditionAssignments, etc.)
5. A concrete example showing conditional branching in a schedule timeline
6. Any USDM-IG v4 rules or constraints that govern their use

## ScheduledDecisionInstance and ScheduledActivityInstance in USDM-IG v4

Both are concrete subclasses of the abstract `ScheduledInstance` class — they are peers in a `ScheduleTimeline.instances` collection.


<table>
  <tr>
    <td></td>
    <td><strong>ScheduledActivityInstance</strong></td>
    <td><strong>ScheduledDecisionInstance</strong></td>
  </tr>
  <tr>
    <td><strong>Role</strong></td>
    <td>Work node — where visits and activities live</td>
    <td>Routing node — evaluates conditions and directs flow</td>
  </tr>
  <tr>
    <td><strong>Visual</strong></td>
    <td>Grey circle in diagrams</td>
    <td>Grey diamond</td>
  </tr>
  <tr>
    <td><strong>Key attributes</strong></td>
    <td>activities, encounter, timeline, timelineExit</td>
    <td>conditionAssignments (1..*)</td>
  </tr>
</table>		
	
### How They Link

`conditionAssignments` (on `ScheduledDecisionInstance`) — the primary routing mechanism. Each `ConditionAssignment` has:

* `condition` — free-text logical criterion, e.g. `"not reached cycle 12 and fulfilling eligibility to enter next cycle"`
* `conditionTarget` (mandatory 1:1) — the `ScheduledInstance` to route to when true

`defaultCondition` (inherited from `ScheduledInstance`) — fallback when no condition is met. The spec says this _should_ be set for multi-branch switches.

Evaluation order:

1. Test each `ConditionAssignment.condition` in sequence
2. First match → route to its `conditionTarget`
3. No match → route to `defaultCondition`


### Concrete Example — Oncology Cycle Loop

```JSON
{
  "id": "ScheduledDecisionInstance_1",
  "instanceType": "ScheduledDecisionInstance",
  "name": "Continue Treatment Decision",
  "defaultConditionId": "ScheduledActivityInstance_3",  // → End of Treatment
  "conditionAssignments": [
    {
      "id": "ConditionAssignment_1",
      "instanceType": "Condition",
      "condition": "not reached cycle 12 and fulfilling eligibility to enter next cycle",
      "conditionTargetId": "ScheduledActivityInstance_2"  // → Next Cycle Start
    }
  ]
}
```

Flow: `Cycle Visit (SAI_1)` → `Decision (SDI_1)` → either loops back to `SAI_2` (next cycle) or falls through to `SAI_3` (end of treatment). The spec explicitly allows `conditionTarget` to reference an earlier instance, creating cycles.

Key USDM-IG v4 Constraints
* `conditionAssignments` cardinality is 1..* — a decision instance with zero assignments is invalid
* `ConditionAssignment.conditionTarget` cardinality is 1 — never optional
* `defaultCondition` is 0..1 but should be present for multi-branch switches (undefined behaviour otherwise)
* A ScheduledDecisionInstance has no `activities`, `encounter`, or `timelineExit` — it carries no clinical work
* `instanceType` discriminator must be `"ScheduledDecisionInstance"` on the node and `"Condition"` on each ConditionAssignment