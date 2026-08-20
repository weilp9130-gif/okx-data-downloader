"""任务状态机：状态转换的唯一权威

状态：
    PENDING → QUEUED → ASSIGNED → RUNNING → SUCCESS/FAILED/CANCELLED/INTERRUPTED
    QUEUED  ⇄ PAUSED（仅 QUEUED/PAUSED 之间）
    任何非终态可进入 INTERRUPTED（重启/崩溃遗留）

规则：
    - 终态（SUCCESS/FAILED/CANCELLED/INTERRUPTED）没有任何出边
    - 禁止回迁
    - retry 例外：retry_count < max_retry 时 FAILED → QUEUED（新 attempt）
"""

TERMINAL_STATES = frozenset({"SUCCESS", "FAILED", "CANCELLED", "INTERRUPTED"})

# 合法转换表（不含 retry 例外）
ALLOWED_TRANSITIONS = {
    "PENDING": frozenset({"QUEUED", "CANCELLED", "INTERRUPTED"}),
    "QUEUED": frozenset({"ASSIGNED", "CANCELLED", "PAUSED", "FAILED", "INTERRUPTED"}),
    "PAUSED": frozenset({"QUEUED", "CANCELLED", "INTERRUPTED"}),
    "ASSIGNED": frozenset({"RUNNING", "FAILED", "CANCELLED", "INTERRUPTED"}),
    "RUNNING": frozenset({"SUCCESS", "FAILED", "CANCELLED", "INTERRUPTED"}),
}


class IllegalTransitionError(Exception):
    """非法状态转换"""

    def __init__(self, from_state: str, to_state: str, reason: str = ""):
        self.from_state = from_state
        self.to_state = to_state
        self.reason = reason
        super().__init__(
            f"非法状态转换: {from_state} → {to_state}"
            + (f" ({reason})" if reason else "")
        )


class JobStateMachine:
    """任务状态机（纯逻辑，无 I/O）"""

    @staticmethod
    def is_terminal(state: str) -> bool:
        return state in TERMINAL_STATES

    @classmethod
    def can_transition(
        cls, from_state: str, to_state: str, retry: bool = False
    ) -> bool:
        """校验 from → to 是否合法

        Args:
            from_state: 当前状态
            to_state: 目标状态
            retry: 是否走自动重试例外（FAILED → QUEUED）
        """
        if retry:
            return from_state == "FAILED" and to_state == "QUEUED"
        allowed = ALLOWED_TRANSITIONS.get(from_state, frozenset())
        return to_state in allowed

    @classmethod
    def transition(
        cls, from_state: str, to_state: str, retry: bool = False
    ) -> None:
        """执行转换校验；非法抛 IllegalTransitionError"""
        if not cls.can_transition(from_state, to_state, retry=retry):
            raise IllegalTransitionError(from_state, to_state)
