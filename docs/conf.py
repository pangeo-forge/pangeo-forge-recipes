# -- Project information -----------------------------------------------------

project = "Pangeo Forge"
copyright = "2023, Pangeo Community"
author = "Pangeo Community"

# -- General configuration ---------------------------------------------------

extensions = [
    "myst_nb",
    "sphinx.ext.autodoc",
    "sphinx.ext.extlinks",
    "sphinx.ext.graphviz",
    # "numpydoc",
    "sphinx_autodoc_typehints",
    "sphinx_copybutton",
    "sphinx_togglebutton",
    "sphinxext.opengraph",
    "sphinx_panels",
    "sphinx_tabs.tabs",
]

extlinks = {
    "issue": ("https://github.com/pangeo-forge/pangeo-forge-recipes/issues/%s", "GH issue "),
    "pull": ("https://github.com/pangeo-forge/pangeo-forge-recipes/pull/%s", "GH PR "),
}

exclude_patterns = ["_build", "**.ipynb_checkpoints"]
master_doc = "index"

# we always have to manually run the notebooks because they are slow / expensive
jupyter_execute_notebooks = "auto"
execution_excludepatterns = [
    "tutorials/*",
    "introduction_tutorial/*",
]

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
myst_enable_extensions = ["substitution"]

github_comment_header = (
    "<img "
    'style="background: white; border: 1px solid rgba(0,0,0,0.25); border-radius: 50%; width:2em" '
    'src="../_static/{username}.png" '
    'alt="{username}"/> '
    "<span "
    'style="font-size: 1.1em; font-weight: 600">'
    "{username}"
    "</span>"
    "<span "
    'style="font-size: 1.1em; font-weight: 400; color: rgba(0,0,0,0.5)">'
    " commented"
    "</span>"
)
myst_substitutions = {
    "pangeo_forge_bot_header": github_comment_header.format(username="pangeo-forge-bot"),
    "human_maintainer_header": github_comment_header.format(username="human-maintainer"),
    "recipe_contributor_header": github_comment_header.format(username="recipe-contributor"),
}

autodoc_mock_imports = ["apache_beam"]

# should be set automatically on RTD based on html_baseurl
# ogp_site_url = "https://pangeo-forge.readthedocs.io/"
ogp_image = "_static/pangeo-forge-logo-blue.png"
ogp_use_first_image = True
