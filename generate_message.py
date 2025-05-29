from typing import Any, Dict, List, Optional


def generate_kp_message(curies: List[str], kp_overrides: Optional[Dict[str, Any]] = {}) -> Dict[str, Any]:
    """Create a message to send to Translator services"""
    predicates = kp_overrides.get("predicates") or ["biolink:treats_or_applied_or_studied_to_treat"]

    return {
        "message": {
            "query_graph": {
                "nodes": {
                    "chemical": {
                        "categories": ["biolink:ChemicalEntity"],
                        "set_interpretation": "BATCH",
                    },
                    "f": {
                        "ids": curies,
                        "set_interpretation": "BATCH",
                    },
                },
                "edges": {
                    "edge_1": {
                        "subject": "chemical",
                        "object": "f",
                        "predicates": predicates,
                    },
                },
            },
        },
    }


def generate_ara_message():
    return {
        "message": {
            "query_graph": {
                "nodes": {
                    "ON": {
                        "categories": [
                            "biolink:Disease"
                        ],
                        "ids": [
                            "MONDO:0005301"
                        ]
                    },
                    "SN": {
                        "categories": [
                            "biolink:ChemicalEntity"
                        ]
                    }
                },
                "edges": {
                    "t_edge": {
                        "object": "ON",
                        "subject": "SN",
                        "predicates": [
                            "biolink:treats"
                        ],
                        "knowledge_type": "inferred"
                    }
                }
            }
        },
        "bypass_cache": True,
    }