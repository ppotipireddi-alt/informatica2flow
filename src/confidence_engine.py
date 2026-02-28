# ---------------------------------------------------------
# Confidence Engine
# ---------------------------------------------------------
def compute_confidence(scores, validation_report):

    base = sum(scores) / len(scores) if scores else 0.7

    if validation_report.get("empty_sql"):
        base -= 0.2

    return max(0, min(base, 1))


from confidence_engine import compute_confidence
from migration_state import MigrationState

def confidence_node(state: MigrationState):

    overall = compute_confidence(
        state["confidence_scores"],
        state["validation_report"]
    )

    state["overall_confidence"] = overall
    state["review_required"] = overall < 0.75

    return state