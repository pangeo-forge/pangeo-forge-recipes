# -- Project information -----------------------------------------------------

project = "Pangeo Forge"
copyright = "2020, Pangeo Community"
author = "Pangeo Community"

# -- General configuration ---------------------------------------------------

extensions = [
    "myst_nb",
    "sphinx.ext.autodoc",
    "sphinx.ext.extlinks",
    # "numpydoc",
    "sphinx_autodoc_typehints",
    "sphinx_copybutton",
]

extlinks = {
    "issue": ("https://github.com/pangeo-forge/pangeo-forge-recipes/issues/%s", "GH issue "),
    "pull": ("https://github.com/pangeo-forge/pangeo-forge-recipes/pull/%s", "GH PR "),
}

exclude_patterns = ["_build", "**.ipynb_checkpoints"]
master_doc = "index"

# we always have to manually run the notebooks because they are slow / expensive
jupyter_execute_notebooks = "auto"
execution_excludepatterns = ["tutorials/xarray_zarr/*", "tutorials/hdf_reference/*"]

# -- Options for HTML output -------------------------------------------------

# https://sphinx-book-theme.readthedocs.io/en/latest/configure.html
html_theme = "pangeo_sphinx_book_theme"
html_theme_options = {
    "repository_url": "https://github.com/pangeo-forge/pangeo-forge-recipes",
    "repository_branch": "master",
    "path_to_docs": "docs",
    "use_repository_button": True,
    "use_issues_button": True,
    "use_edit_page_button": True,
}
html_logo = "_static/pangeo-forge-logo-blue.png"
html_static_path = ["_static"]

myst_heading_anchors = 2
