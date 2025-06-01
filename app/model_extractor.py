from typing import List, Dict, Any
from collections import deque
import re

def get_team_model_summary(manifest: Dict[str, Any], target_model_names: List[str]) -> Dict[str, Any]:
    node_id_to_node = {}
    name_to_node_id = {}

    for node_id, node in manifest.get("nodes", {}).items():
        if node.get("resource_type") == "model":
            model_name = node["name"]
            node_id_to_node[node_id] = node
            name_to_node_id[model_name] = node_id

    collected_node_ids = set()

    def collect_related_nodes(start_model_names):
        queue = deque()
        visited = set()

        for name in start_model_names:
            node_id = name_to_node_id.get(name)
            if node_id:
                queue.append(node_id)

        while queue:
            current = queue.popleft()
            if current in visited:
                continue
            visited.add(current)
            collected_node_ids.add(current)

            node = node_id_to_node.get(current)
            if not node:
                continue

            for dep in node.get("depends_on", {}).get("nodes", []):
                if dep.startswith("model."):
                    queue.append(dep)

            for other_id, other_node in node_id_to_node.items():
                if current in other_node.get("depends_on", {}).get("nodes", []):
                    queue.append(other_id)

    collect_related_nodes(target_model_names)

    model_summary = {}

    for node_id in collected_node_ids:
        node = node_id_to_node[node_id]
        model_name = node["name"]
        model_summary[model_name] = {
            "description": node.get("description", ""),
            "columns": {
                col_name: col_data.get("description", "")
                for col_name, col_data in node.get("columns", {}).items()
            }
        }

    return model_summary

def format_prompt_context(summary: Dict[str, Any], relevant_models: List[str]) -> str:
    lines = []
    for model in relevant_models:
        if model not in summary:
            continue
        meta = summary[model]
        lines.append(f"Model: {model}")
        if meta["description"]:
            lines.append(f"Description: {meta['description']}")
        if meta.get("columns"):
            lines.append("Columns:")
            for col, desc in meta["columns"].items():
                if desc:
                    lines.append(f"- {col}: {desc}")
                else:
                    lines.append(f"- {col}")
        lines.append("")
    return "\n".join(lines).strip()

def get_relevant_models_from_question(question: str, summary: Dict[str, Any], max_matches: int = 5) -> List[str]:
    question_lower = question.lower()
    keywords = set(re.findall(r"\b\w+\b", question_lower))
    scores = []

    for model_name, meta in summary.items():
        score = 0
        combined_text = model_name + " " + meta.get("description", "")
        combined_text += " " + " ".join(meta.get("columns", {}).keys())
        combined_text += " " + " ".join(meta.get("columns", {}).values())

        combined_text = combined_text.lower()
        for word in keywords:
            if word in combined_text:
                score += 1
        if score > 0:
            scores.append((model_name, score))

    scores.sort(key=lambda x: x[1], reverse=True)
    return [model for model, _ in scores[:max_matches]]
