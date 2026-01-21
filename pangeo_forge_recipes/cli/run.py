import importlib.util

import apache_beam as beam
import yaml


def extract_runner_options(config):
    return config["runner"]


def extract_storage_options(config):
    return config["storage"]


def configure_target_root(target_root, meta):
    return f"{target_root}/{meta['id']}"


def load_recipe(name, root):
    path = root.joinpath(name).with_suffix(".py")
    spec = importlib.util.spec_from_file_location(name, str(path))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    return module


def create_runner(options):
    # force default runner for now
    return None


def run_recipe(path, runtime_config):
    # steps:
    # - import recipe
    # - extract recipe function
    # - create runner according to `config`
    # - create pipeline
    # - call func with pipeline additional parameters from `config`
    meta = yaml.safe_load(path.joinpath("meta.yaml").read_text())

    runner_options = extract_runner_options(runtime_config)
    storage_options = extract_storage_options(runtime_config)

    for recipe_spec in meta["recipes"]:
        recipe_name, func_name = recipe_spec["object"].split(":")
        store_kwargs = recipe_spec.get("store_kwargs", {})

        recipe_module = load_recipe(recipe_name, path)
        recipe_func = getattr(recipe_module, func_name)

        target_root = configure_target_root(storage_options["target_root"], meta)

        runner = create_runner(runner_options)
        with beam.Pipeline(runner=runner) as p:
            recipe_func(p, target_root=target_root, store_kwargs=store_kwargs)
