import sys

# import sphinx_pangeo_theme  # noqa
import sphinx_book_theme  # noqa

sys.path.append("..")

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

# https://sphinx-book-theme.readthedocs.io/en/latest/configure.html
html_theme = "sphinx_book_theme"
html_theme_options = {
    "repository_url": "https://github.com/pangeo-forge/pangeo-forge",
    "repository_branch": "master",
    "path_to_docs": "docs",
    "use_repository_button": True,
    "use_issues_button": True,
    "use_edit_page_button": True,
}
html_logo = "_static/pangeo-forge-logo-blue.png"
html_static_path = ["_static"]
myst_heading_anchors = 2
html_css_files = [
    "custom.css",
]
