import sys
import json
import inspect
from pathlib import Path


# ============================================================
# PATH SETUP
# ============================================================

ROOT_DIR = Path(__file__).resolve().parent.parent
SRC_DIR = ROOT_DIR / "src"
AGENTS_DIR = SRC_DIR / "agents"
TOOLS_DIR = AGENTS_DIR / "tools"

for path in [ROOT_DIR, SRC_DIR, AGENTS_DIR, TOOLS_DIR]:
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)


# ============================================================
# IMPORTS
# ============================================================

from src.agents.tools.sharepoint_processor import (
    RulesEngine,
    ClientRouter,
    AIClientRouter,
    VendorIntelligenceEngine,
    extract_pdf_text,
)


# ============================================================
# HELPERS
# ============================================================

RULES_DIR = ROOT_DIR / "src" / "agents" / "data" / "rules"


def print_header(title: str):
    print("\n" + "=" * 120)
    print(title)
    print("=" * 120)


def safe_signature(obj):
    try:
        return str(inspect.signature(obj))
    except Exception:
        return "(signature unavailable)"


def describe_object(name: str, obj):
    print_header(f"{name} INSPECTION")
    print(f"Type: {type(obj)}")
    print()

    public_attrs = []
    for attr_name in dir(obj):
        if attr_name.startswith("_"):
            continue
        try:
            value = getattr(obj, attr_name)
        except Exception:
            continue
        public_attrs.append((attr_name, value))

    methods = []
    properties = []

    for attr_name, value in public_attrs:
        if callable(value):
            methods.append((attr_name, value))
        else:
            properties.append((attr_name, value))

    print("PUBLIC METHODS")
    if methods:
        for method_name, method in methods:
            print(f" - {method_name}{safe_signature(method)}")
    else:
        print(" - none")

    print()
    print("PUBLIC PROPERTIES")
    if properties:
        for prop_name, value in properties[:50]:
            rendered = repr(value)
            if len(rendered) > 200:
                rendered = rendered[:200] + "..."
            print(f" - {prop_name} = {rendered}")
    else:
        print(" - none")


def list_rule_files():
    print_header("RULE FILES")
    if not RULES_DIR.exists():
        print(f"Rules directory does not exist: {RULES_DIR}")
        return []

    files = sorted([p for p in RULES_DIR.rglob("*") if p.is_file()])

    if not files:
        print(f"No files found in: {RULES_DIR}")
        return []

    for file_path in files:
        rel = file_path.relative_to(ROOT_DIR)
        print(rel)

    return files


def preview_rule_files(files):
    print_header("RULE FILE PREVIEW")
    previewable_exts = {".json", ".yaml", ".yml", ".csv", ".txt"}

    shown = 0
    for file_path in files:
        if file_path.suffix.lower() not in previewable_exts:
            continue

        print(f"\n--- {file_path.relative_to(ROOT_DIR)} ---")
        try:
            content = file_path.read_text(encoding="utf-8", errors="ignore")
            print(content[:1000])
        except Exception as e:
            print(f"Could not read file: {e}")

        shown += 1
        if shown >= 10:
            break

    if shown == 0:
        print("No previewable rule files found.")


def try_method(obj, method_name: str, *args, **kwargs):
    if not hasattr(obj, method_name):
        return {
            "exists": False,
            "output": None,
            "error": None,
        }

    method = getattr(obj, method_name)

    if not callable(method):
        return {
            "exists": True,
            "output": None,
            "error": f"{method_name} exists but is not callable",
        }

    try:
        output = method(*args, **kwargs)
        return {
            "exists": True,
            "output": output,
            "error": None,
        }
    except Exception as e:
        return {
            "exists": True,
            "output": None,
            "error": f"{type(e).__name__}: {e}",
        }


def inspect_engine_behavior(pdf_path: Path):
    print_header("ENGINE BEHAVIOR TEST")

    if not pdf_path.exists():
        print(f"PDF not found: {pdf_path}")
        return

    text = extract_pdf_text(pdf_path)
    print(f"PDF: {pdf_path}")
    print(f"Text length: {len(text or '')}")
    print()
    print("TEXT PREVIEW")
    print((text or "")[:1500])

    rules_engine = RulesEngine(RULES_DIR)
    client_router = ClientRouter(RULES_DIR)
    ai_client_router = AIClientRouter(RULES_DIR)
    vendor_engine = VendorIntelligenceEngine(RULES_DIR)

    describe_object("RulesEngine", rules_engine)
    describe_object("ClientRouter", client_router)
    describe_object("AIClientRouter", ai_client_router)
    describe_object("VendorIntelligenceEngine", vendor_engine)

    print_header("RULES ENGINE METHOD TESTS")

    for method_name in ["extract", "run", "process", "match", "classify"]:
        result = try_method(rules_engine, method_name, text)
        print(f"\nMethod: {method_name}")
        print(json.dumps(result, indent=2, ensure_ascii=False, default=str))

    print_header("CLIENT ROUTER METHOD TESTS")

    for method_name in ["route", "run", "process", "match"]:
        result = try_method(client_router, method_name, text)
        print(f"\nMethod: {method_name}")
        print(json.dumps(result, indent=2, ensure_ascii=False, default=str))

    print_header("AI CLIENT ROUTER METHOD TESTS")

    for method_name in ["route", "run", "process", "match"]:
        result = try_method(ai_client_router, method_name, text)
        print(f"\nMethod: {method_name}")
        print(json.dumps(result, indent=2, ensure_ascii=False, default=str))

    print_header("VENDOR INTELLIGENCE METHOD TESTS")

    vendor_candidates = [
        "Dell Canada Inc.",
        "Dell",
        None,
    ]
    doc_type_candidates = [
        "invoice",
        None,
    ]

    for method_name in ["classify", "run", "process", "match"]:
        if hasattr(vendor_engine, method_name):
            print(f"\nMethod: {method_name}")
            for vendor_name in vendor_candidates:
                for doc_type in doc_type_candidates:
                    try:
                        output = getattr(vendor_engine, method_name)(vendor_name, doc_type)
                        print(json.dumps({
                            "vendor_name": vendor_name,
                            "doc_type": doc_type,
                            "output": output,
                        }, indent=2, ensure_ascii=False, default=str))
                    except Exception as e:
                        print(json.dumps({
                            "vendor_name": vendor_name,
                            "doc_type": doc_type,
                            "error": f"{type(e).__name__}: {e}",
                        }, indent=2, ensure_ascii=False, default=str))


def main():
    files = list_rule_files()
    preview_rule_files(files)

    pdf_path = ROOT_DIR / "tests" / "documents" / "abcd.pdf"
    inspect_engine_behavior(pdf_path)


if __name__ == "__main__":
    main()