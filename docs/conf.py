# import pangeo_sphinx_book_theme  # noqa

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

# templates_path = ["_templates"]
exclude_patterns = ["_build", "**.ipynb_checkpoints"]
master_doc = "index"

# we always have to manually run the notebooks because they are slow / expensive
jupyter_execute_notebooks = "auto"
execution_excludepatterns = ["tutorials/*"]

# -- Options for HTML output -------------------------------------------------

# https://sphinx-book-theme.readthedocs.io/en/latest/configure.html
html_theme = "pangeo_sphinx_book_theme"
html_theme_options = {}
html_logo = "_static/pangeo-forge-logo-blue.png"
html_static_path = ["_static"]
html_css_files = ["_static/pangeo-sphinx-book-theme.css",]
myst_heading_anchors = 2
