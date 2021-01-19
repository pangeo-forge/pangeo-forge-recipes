import sphinx_pangeo_theme  # noqa

# -- Project information -----------------------------------------------------

project = "pangeo-forge"
copyright = "2020, Pangeo Community"
author = "Pangeo Community"


# -- General configuration ---------------------------------------------------

extensions = [
    "myst_parser",
    "sphinx.ext.autodoc",
    # "numpydoc",
    "sphinx_autodoc_typehints",
]

templates_path = ["_templates"]
exclude_patterns = []
master_doc = "index"

# -- Options for HTML output -------------------------------------------------

html_theme = "pangeo"
html_static_path = ["_static"]
html_sidebars = {"index": [], "**": ["localtoc.html"]}

myst_heading_anchors = 2
