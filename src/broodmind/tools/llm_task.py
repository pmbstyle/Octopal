from __future__ import annotations
import json
from typing import Any
from broodmind.providers.base import InferenceProvider, Message

# We need a JSON schema validator. `fastapi` depends on `pydantic`, which has
# its own validation, but for arbitrary schemas, a dedicated library is better.
# `jsonschema` is a common choice. We will add it to `pyproject.toml`.
try:
    import jsonschema
except ImportError:
    jsonschema = None


async def run_llm_subtask(args: dict[str, Any], provider: InferenceProvider) -> str:
    """
    Executes a one-off LLM task with a specific prompt and returns a
    schema-validated JSON object.
    """
    prompt = args.get("prompt")
    if not prompt or not isinstance(prompt, str):
        return json.dumps({"error": "run_llm_subtask error: a valid 'prompt' string is required."})

    input_data = args.get("input")
    schema = args.get("schema")

    if schema and not jsonschema:
        return json.dumps({
            "error": "run_llm_subtask error: 'jsonschema' package is required for schema validation. Please install it."
        })

    # Construct the prompt for the sub-task LLM
    system_prompt = (
        "You are a JSON-only function. Your sole purpose is to process the following task "
        "and return ONLY a single, valid JSON object that conforms to the user's request. "
        "Do not include any commentary, explanations, or markdown fences. Do not call any tools."
    )
    
    full_prompt = f"Task: {prompt}"
    if input_data:
        try:
            input_json = json.dumps(input_data, indent=2)
            full_prompt += f"\n\nInput Data:\n{input_json}"
        except TypeError:
            return json.dumps({"error": "run_llm_subtask error: 'input' data must be JSON serializable."})

    messages = [
        Message(role="system", content=system_prompt),
        Message(role="user", content=full_prompt),
    ]

    try:
        response_text = await provider.complete(messages)
        
        try:
            parsed_json = json.loads(response_text)
        except json.JSONDecodeError:
            return json.dumps({"error": "run_llm_subtask error: LLM returned invalid JSON."})
        
        if schema:
            try:
                jsonschema.validate(instance=parsed_json, schema=schema)
            except jsonschema.ValidationError as e:
                return json.dumps({
                    "error": f"run_llm_subtask error: LLM output failed schema validation: {e.message}"
                })
        
        return json.dumps(parsed_json)

    except Exception as e:
        return json.dumps({"error": f"run_llm_subtask error: An unexpected error occurred: {e}"})
