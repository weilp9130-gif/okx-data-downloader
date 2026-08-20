"""任务状态机测试：合法/非法转换（含 ASSIGNED/retry 例外）、终态锁定"""

import unittest

from app.task.state_machine import (
    IllegalTransitionError,
    JobStateMachine,
    TERMINAL_STATES,
)


class TestStateMachine(unittest.TestCase):
    def test_pending_to_queued(self):
        self.assertTrue(JobStateMachine.can_transition("PENDING", "QUEUED"))

    def test_queued_to_assigned(self):
        self.assertTrue(JobStateMachine.can_transition("QUEUED", "ASSIGNED"))

    def test_assigned_to_running(self):
        self.assertTrue(JobStateMachine.can_transition("ASSIGNED", "RUNNING"))

    def test_running_to_success_failed_cancelled(self):
        self.assertTrue(JobStateMachine.can_transition("RUNNING", "SUCCESS"))
        self.assertTrue(JobStateMachine.can_transition("RUNNING", "FAILED"))
        self.assertTrue(JobStateMachine.can_transition("RUNNING", "CANCELLED"))

    def test_queued_pause_resume_cycle(self):
        self.assertTrue(JobStateMachine.can_transition("QUEUED", "PAUSED"))
        self.assertTrue(JobStateMachine.can_transition("PAUSED", "QUEUED"))

    def test_assigned_to_failed_on_start_failure(self):
        self.assertTrue(JobStateMachine.can_transition("ASSIGNED", "FAILED"))
        self.assertTrue(JobStateMachine.can_transition("ASSIGNED", "CANCELLED"))

    def test_non_terminal_to_interrupted(self):
        for state in ("PENDING", "QUEUED", "PAUSED", "ASSIGNED", "RUNNING"):
            self.assertTrue(
                JobStateMachine.can_transition(state, "INTERRUPTED"),
                f"{state} → INTERRUPTED 应合法",
            )

    def test_terminal_has_no_out_edges(self):
        for terminal in TERMINAL_STATES:
            for target in TERMINAL_STATES | {"QUEUED", "RUNNING", "PAUSED"}:
                if target != terminal:
                    self.assertFalse(
                        JobStateMachine.can_transition(terminal, target),
                        f"{terminal} → {target} 应非法",
                    )

    def test_no_regression_to_pending(self):
        self.assertFalse(JobStateMachine.can_transition("QUEUED", "PENDING"))
        self.assertFalse(JobStateMachine.can_transition("RUNNING", "ASSIGNED"))

    def test_running_back_to_queued_illegal(self):
        self.assertFalse(JobStateMachine.can_transition("RUNNING", "QUEUED"))

    def test_transition_raises(self):
        with self.assertRaises(IllegalTransitionError):
            JobStateMachine.transition("SUCCESS", "QUEUED")

    def test_retry_exception(self):
        # retry 例外：FAILED → QUEUED
        self.assertFalse(JobStateMachine.can_transition("FAILED", "QUEUED"))
        self.assertTrue(JobStateMachine.can_transition("FAILED", "QUEUED", retry=True))
        JobStateMachine.transition("FAILED", "QUEUED", retry=True)
        # 其他状态不允许 retry 例外
        self.assertFalse(JobStateMachine.can_transition("RUNNING", "QUEUED", retry=True))


if __name__ == "__main__":
    unittest.main()
