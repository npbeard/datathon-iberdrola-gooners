"""Normalize machine-specific absolute paths inside notebook JSON."""

from __future__ import annotations

import json
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
NOTEBOOKS_DIR = ROOT / "notebooks"


def scrub_text(text: str) -> str:
    repo_path = str(ROOT)
    text = text.replace(repo_path, ".")
    text = re.sub(r"/opt/anaconda3(?:/envs/[^/]+)?/bin/python\s+\./scripts/", "python scripts/", text)
    text = re.sub(r"\./maps/", "maps/", text)
    text = re.sub(r"\./data/", "data/", text)
    text = re.sub(r"\./src/", "src/", text)
    return text


def scrub_notebook(path: Path) -> bool:
    notebook = json.loads(path.read_text(encoding="utf-8"))
    original = json.dumps(notebook, ensure_ascii=False, sort_keys=True)

    for cell in notebook.get("cells", []):
        if "source" in cell:
            cell["source"] = [scrub_text(part) for part in cell["source"]]

        for output in cell.get("outputs", []):
            if "text" in output:
                output["text"] = [scrub_text(part) for part in output["text"]]
            data = output.get("data", {})
            for key, value in list(data.items()):
                if isinstance(value, list):
                    data[key] = [scrub_text(part) if isinstance(part, str) else part for part in value]
                elif isinstance(value, str):
                    data[key] = scrub_text(value)

    updated = json.dumps(notebook, ensure_ascii=False, sort_keys=True)
    if updated == original:
        return False

    path.write_text(json.dumps(notebook, ensure_ascii=False, indent=1), encoding="utf-8")
    return True


def main() -> None:
    changed = 0
    for notebook_path in sorted(NOTEBOOKS_DIR.glob("*.ipynb")):
        if scrub_notebook(notebook_path):
            changed += 1
            print(f"Scrubbed: {notebook_path}")
    print(f"Notebook files updated: {changed}")


if __name__ == "__main__":  # pragma: no cover
    main()
