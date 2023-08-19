def get_injection_specs():
    return {
        "StoreToZarr": {
            "target_root": "OUTPUT_ROOT",
        },
        "WriteCombinedReference": {
            "target_root": "OUTPUT_ROOT",
        },
        "OpenURLWithFSSpec": {"cache": "CACHE_ROOT"},
    }
