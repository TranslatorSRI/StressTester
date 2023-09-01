from typing import Any, Dict, List, Optional


def generate_message(curies: List[str], kp_overrides: Optional[Dict[str, Any]] = {}) -> Dict[str, Any]:
    """Create a message to send to Translator services"""
    predicates = kp_overrides.get("predicates") or ["biolink:treats"]

    return {
        "message": {
            "query_graph": {
                "nodes": {
                    "chemical": {
                        "categories": ["biolink:ChemicalEntity"],
                        "is_set": False,
                        "constraints": [],
                    },
                    "f": {
                        "ids": curies,
                        "is_set": False,
                        "constraints": [],
                    },
                },
                "edges": {
                    "edge_1": {
                        "subject": "chemical",
                        "object": "f",
                        "predicates": predicates,
                        "attribute_constraints": [],
                        "qualifier_constraints": [],
                    },
                },
            },
        },
    }