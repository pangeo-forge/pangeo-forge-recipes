# -- Project information -----------------------------------------------------

project = "Pangeo Forge"
copyright = "2023, Pangeo Community"
author = "Pangeo Community"

# -- General configuration ---------------------------------------------------

extensions = [
    "myst_parser",
    "sphinx.ext.autodoc",
    # "sphinx.ext.extlinks",
    "sphinx.ext.graphviz",
    "sphinx_autodoc_typehints",
    "sphinx_copybutton",
    "sphinx_togglebutton",
    "sphinxext.opengraph",
    "sphinx_tabs.tabs",
]

extlinks = {
    "issue": ("https://github.com/pangeo-forge/pangeo-forge-recipes/issues/%s", "GH issue "),
    "pull": ("https://github.com/pangeo-forge/pangeo-forge-recipes/pull/%s", "GH PR "),
}

exclude_patterns = ["_build"]
master_doc = "index"


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

myst_heading_anchors = 3

autodoc_mock_imports = ["apache_beam"]

# should be set automatically on RTD based on html_baseurl
# ogp_site_url = "https://pangeo-forge.readthedocs.io/"
ogp_image = "_static/pangeo-forge-logo-blue.png"
ogp_use_first_image = True
