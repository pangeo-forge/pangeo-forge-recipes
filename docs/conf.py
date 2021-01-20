# import sphinx_pangeo_theme  # noqa

# -- Project information -----------------------------------------------------

project = "pangeo-forge"
copyright = "2020, Pangeo Community"
author = "Pangeo Community"


# -- General configuration ---------------------------------------------------

extensions = [
    "myst_parser",
]

templates_path = ["_templates"]
exclude_patterns = []
master_doc = "index"

# -- Options for HTML output -------------------------------------------------

html_theme = "sphinx_rtd_theme"
html_static_path = ["_static"]

myst_heading_anchors = 2
