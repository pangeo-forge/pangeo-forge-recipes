def get_injection_specs():
    return {
        "Config": {
            "target_root": "TARGET_STORAGE",
            "cache": "INPUT_CACHE_STORAGE",
        },
        "StoreToZarr": {
            "target_root": "TARGET_STORAGE",
        },
        "WriteCombinedReference": {
            "target_root": "TARGET_STORAGE",
        },
        "OpenURLWithFSSpec": {"cache": "INPUT_CACHE_STORAGE"},
    }
