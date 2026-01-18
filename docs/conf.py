# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "sparkctl"
copyright = "2026, Alliance for Energy Innovation, LLC"
author = "Daniel Thom"
release = "0.1.0"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "myst_parser",
    "sphinx.ext.githubpages",
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.todo",
    "sphinx_copybutton",
    "sphinx_tabs.tabs",
    "sphinx_click",
    "sphinxcontrib.mermaid",
    "sphinxcontrib.autodoc_pydantic",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

source_suffix = {
    ".txt": "markdown",
    ".md": "markdown",
}

html_theme = "furo"
html_title = "sparkctl documentation"
html_theme_options = {
    "navigation_with_keys": True,
}
html_static_path = ["_static"]
html_css_files = ["custom.css"]

todo_include_todos = True
autoclass_content = "both"
autodoc_member_order = "bysource"
todo_include_todos = True
copybutton_only_copy_prompt_lines = True
copybutton_exclude = ".linenos, .gp, .go"
copybutton_line_continuation_character = "\\"
copybutton_here_doc_delimiter = "EOT"
copybutton_prompt_text = "$"
copybutton_copy_empty_lines = False

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
}

# Mermaid configuration
mermaid_d3_zoom = False
mermaid_init_js = """
mermaid.initialize({
    startOnLoad: true,
    flowchart: {
        useMaxWidth: true,
        htmlLabels: true,
        curve: 'basis'
    },
    theme: 'default'
});
"""
