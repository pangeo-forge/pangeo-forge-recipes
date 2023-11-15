def get_injection_specs():
    return {
        "StoreToZarr": {
            "target_root": "TARGET_STORAGE",
        },
        "WriteCombinedReference": {
            "target_root": "TARGET_STORAGE",
        },
        "OpenURLWithFSSpec": {"cache": "INPUT_CACHE_STORAGE"},
    }
