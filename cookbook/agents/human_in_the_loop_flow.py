"""
Human-in-the-Loop Flow Example: Expense Approval Workflow

This example demonstrates a human approval step that pauses execution
until a human provides input. Uses HumanInputManager with asyncio to
simulate a human reviewing and approving an expense report.

Usage:
    python cookbook/human_in_the_loop_flow.py
"""

from water.core import Flow, create_task
from water.agents import create_human_task, HumanInputManager
from pydantic import BaseModel
from typing import Dict, Any
import asyncio

# Data schemas
class ExpenseReport(BaseModel):
    employee: str
    amount: float
    category: str
    description: str

class ReviewedExpense(BaseModel):
    employee: str
    amount: float
    category: str
    description: str
    approved: bool
    reviewer_notes: str

class ProcessedExpense(BaseModel):
    employee: str
    amount: float
    approved: bool
    reimbursement_id: str
    status: str

# Step 1: Validate the expense
def validate_expense(params: Dict[str, Any], context) -> Dict[str, Any]:
    """Check that the expense has all required fields and sane values."""
    data = params["input_data"]
    print(f"  [validate] Expense from {data['employee']}: ${data['amount']:.2f}")
    return data

# Step 3: Process the approval decision
def process_decision(params: Dict[str, Any], context) -> Dict[str, Any]:
    """Issue reimbursement or rejection based on the human decision."""
    data = params["input_data"]
    approved = data.get("approved", False)
    status = "reimbursed" if approved else "rejected"
    reimbursement_id = f"RMB-{data['employee'][:3].upper()}-001" if approved else "N/A"
    print(f"  [process] Decision: {status}")
    return {
        "employee": data["employee"],
        "amount": data["amount"],
        "approved": approved,
        "reimbursement_id": reimbursement_id,
        "status": status,
    }

# Set up the human input manager
him = HumanInputManager()

# Create tasks
validate_task = create_task(
    id="validate_expense",
    description="Validate expense report fields",
    input_schema=ExpenseReport,
    output_schema=ExpenseReport,
    execute=validate_expense,
)

approval_task = create_human_task(
    id="manager_approval",
    description="Manager reviews and approves/rejects expense",
    prompt="Please review this expense report and approve or reject.",
    input_schema=ExpenseReport,
    output_schema=ReviewedExpense,
    human_input_manager=him,
    timeout=30.0,
)

process_task = create_task(
    id="process_decision",
    description="Process the approval or rejection",
    input_schema=ReviewedExpense,
    output_schema=ProcessedExpense,
    execute=process_decision,
)

# Build flow
expense_flow = Flow(id="expense_approval", description="Expense approval with human review")
expense_flow.then(validate_task).then(approval_task).then(process_task).register()

async def simulate_human_approval(manager: HumanInputManager):
    """Simulate a human providing approval after a short delay."""
    # Wait a moment for the flow to reach the human task
    while not await manager.get_pending():
        await asyncio.sleep(0.05)

    pending = await manager.get_pending()
    for request_id, prompt in pending.items():
        print(f"\n  [human] Received prompt: '{prompt}'")
        print(f"  [human] Reviewing... (request_id={request_id})")
        await asyncio.sleep(0.5)  # Simulate thinking time
        print("  [human] Approved with notes.\n")
        await manager.provide_input(request_id, {
            "approved": True,
            "reviewer_notes": "Looks good. Pre-approved vendor.",
        })

async def main():
    """Run the expense approval flow example."""
    print("=== Expense Approval (Human-in-the-Loop) ===\n")

    expense = {
        "employee": "Manthan",
        "amount": 450.00,
        "category": "Travel",
        "description": "Flight to SF for client meeting",
    }

    # Run the flow and the simulated human in parallel
    try:
        flow_task = asyncio.create_task(expense_flow.run(expense))
        human_task = asyncio.create_task(simulate_human_approval(him))

        result = await flow_task
        await human_task

        print(f"  Result: {result}")
        print("  flow completed successfully!")
    except Exception as e:
        print(f"  ERROR - {e}")

    print("\nDone.")

if __name__ == "__main__":
    asyncio.run(main())
