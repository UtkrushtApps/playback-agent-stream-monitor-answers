# Solution Steps

1. Define a Redis event/cancellation protocol in `streaming.py`: use a global stream (`agent:progress`) for the monitor, a per-run stream (`agent:run:{run_id}:events`) for auditability, and a TTL-backed cancellation key (`agent:cancel:{run_id}`).

2. Normalize every progress event into a concise schema with `schema_version`, timestamp, `run_id`, `event_type`, `phase`, `step`, `status`, `summary`, and JSON `details`; redact sensitive key names and avoid publishing raw ticket text, prompts, secrets, or model messages.

3. Implement `publish_progress` to append the normalized event to both streams with bounded `MAXLEN`, `request_cancel` to set the cancel key and publish a control event, and `is_cancel_requested` to check the cancel key.

4. Implement bounded autonomy in `orchestrator.py`: set a conservative maximum number of plan/tool steps and wrap synchronous model/tool calls in `asyncio.to_thread` plus `asyncio.wait_for` so the coroutine cannot hang indefinitely.

5. At run start, publish `run_started`; for each iteration publish `step_started`, `model_call_started`, `model_call_finished`, `tool_decision`, `tool_call_started`, `tool_call_finished`, and `step_finished` as appropriate.

6. Check cancellation cooperatively only at safe boundaries: before a step, after the model call, before the tool call, and after the tool call. If cancellation is requested, write a clear terminal result and publish `run_finished` with `status=cancelled`.

7. Write terminal state exactly once through a helper that first persists `agent:run:{run_id}:result` as JSON, then publishes a terminal stream event. Use terminal statuses such as `completed`, `cancelled`, `failed`, and `terminated`.

8. If the model returns `final`, finish normally. If the model/tool path exceeds the maximum step count, terminate with a clear bounded-autonomy reason. If a timeout or unexpected exception occurs, fail safely and publish a terminal event.

9. Implement the monitor as a separate Redis Stream consumer that reads `agent:progress` with `XREAD`, tracks per-run started steps and elapsed time, and calls `request_cancel` when a non-terminal run exceeds the defined runaway policy.

10. Keep the monitor separately runnable with `python -m agent_orchestrator.monitor` via `main()`, and make `monitor_once` exit after a configurable number of idle polls for tests and one-shot operation.

