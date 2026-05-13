from __future__ import annotations

from octopal.runtime.octo import followup_pipeline as _followup_pipeline
from octopal.runtime.workers.contracts import (
    WorkerInstructionRequest,
    WorkerResult,
    WorkerSpec,
)

_enqueue_internal_result = _followup_pipeline._enqueue_internal_result


class OctoWorkerInstructionRuntimeMixin:
    async def handle_worker_instruction_request(
        self,
        *,
        spec: WorkerSpec,
        request: WorkerInstructionRequest,
    ) -> None:
        if request.target != "octo":
            return
        chat_id = self.get_worker_chat_id(spec.id)
        result = WorkerResult(
            status="awaiting_instruction",
            summary=f"Worker {spec.id} requested instruction: {request.question}",
            output={
                "status": "awaiting_instruction",
                "instruction_request": request.model_dump(mode="json"),
            },
            questions=[request.question],
        )
        _enqueue_internal_result(
            self,
            chat_id,
            spec.id,
            spec.task,
            result,
            correlation_id=spec.correlation_id,
            notify_user=None,
        )
        await self._emit_worker_event(
            chat_id,
            "worker_awaiting_instruction",
            {
                "run_id": spec.id,
                "worker_template_id": spec.template_id,
                "instruction_request": request.model_dump(mode="json"),
            },
        )
