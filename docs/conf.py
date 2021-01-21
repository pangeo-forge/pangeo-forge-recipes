# import sphinx_pangeo_theme  # noqa
import sphinx_book_theme  # noqa

# -- Project information -----------------------------------------------------

project = "Pangeo Forge"
copyright = "2020, Pangeo Community"
author = "Pangeo Community"


# -- General configuration ---------------------------------------------------

extensions = [
    "myst_nb",
    "sphinx.ext.autodoc",
    # "numpydoc",
    "sphinx_autodoc_typehints",
    "sphinx_copybutton",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "**.ipynb_checkpoints"]
master_doc = "index"

# we always have to manually run the notebooks because they are slow / expensive
jupyter_execute_notebooks = "off"

# -- Options for HTML output -------------------------------------------------

html_theme = "sphinx_book_theme"
html_logo = "_static/pangeo-forge-logo-blue.png"
html_static_path = ["_static"]
myst_heading_anchors = 2
html_css_files = [
    "custom.css",
]
