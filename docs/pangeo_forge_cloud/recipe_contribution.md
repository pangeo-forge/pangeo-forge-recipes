# Recipe Contribution

Congratulations! You're reading about the _most exciting part_ of Pangeo Forge: **Recipe Contribution**. By contributing your recipe to {doc}`../pangeo_forge_cloud/index`, you're creating a maintainable dataset resource for the entire community.

To begin, you'll need a thorough understanding of the source dataset you wish to ingest, and a vision for the desired target dataset (desired format, chunk structure, etc.) and any cleaning / processing steps needed along the way.

```{note}
✨ _**No code required!**_ ✨ If there's dataset you'd love to see in [analysis-ready, cloud optimized (ARCO)](https://ieeexplore.ieee.org/abstract/document/9354557) format, but you're not ready to code just yet, we invite you to [open a new Issue](https://github.com/pangeo-forge/staged-recipes/issues/new/choose) on `pangeo-forge/staged-recipes` describing your dataset of interest. The community can then collaborate on developing recipes for these datasets together.

Browse [existing {badge}`proposed recipe,badge-primary badge-pill` Issues here](https://github.com/pangeo-forge/staged-recipes/labels/proposed%20recipe) for inspiration and insight into what others in the community are working on. If you see a dataset you're interested in there, feel free to chime in on the the discussion thread!
```

## Required files

Once you know what dataset you'd like to transform into [analysis-ready, cloud optimized (ARCO)](https://ieeexplore.ieee.org/abstract/document/9354557) format, it's time to begin writing the contribution itself. Every recipe contribution has two required files, a [Recipe module](#recipe-module) and a [`meta.yaml`](#metayaml), described below.

### Recipe module

The **Recipe module** is a Python file (i.e. a text file with the `.py` extension) which defines one or more {doc}`Recipe Objects <../pangeo_forge_recipes/recipe_user_guide/recipes>` for your dataset of interest.

To write this Python file, you will need to understand the basics of {doc}`../pangeo_forge_recipes/index`. The {doc}`../introduction_tutorial/index` is a great place to start, and working through the {doc}`../pangeo_forge_recipes/tutorials/index` and {doc}`../pangeo_forge_recipes/recipe_user_guide/index` can help develop your understanding.

During the development process, it is recommended to [run subsets of your dataset transformation](../introduction_tutorial/intro_tutorial_part2.ipynb) as you go along, to get a feel for how the resulting ARCO dataset will look. This can be done with either a [local installation](../pangeo_forge_recipes/installation.md) of `pangeo-forge-recipes`, or with the in-browser Pangeo Forge [Sandbox](../pangeo_forge_recipes/installation.md).

### `meta.yaml`

The **`meta.yaml`** is a file which contains metadata and configuration your recipe contribution, including:

- Identifying name(s) for your submitted recipe object(s) along with the name of the **Recipe module** in which they can be found.

    ```{note}
    No specific name for the recipe module is required. Instead, the name of the recipe module is defined in the `meta.yaml`.
    ```

- The version of {doc}`../pangeo_forge_recipes/index` used to develop your recipe(s)
- The source data provider's information and license under which the source data is distributed
- Your name, GitHub username, and Orcid ID
- The [Bakery](./core_concepts.md) on which to run your recipe(s)

Please refer to [this template](https://github.com/pangeo-forge/sandbox/blob/main/recipe/meta.yaml) and/or the **Create a `meta.yaml` file** section of the {doc}`../introduction_tutorial/intro_tutorial_part3` tutorial for further details on how to create this file.


## Making a PR

Once you have your [Required files](#required-files) ready to go, it's time to submit them! All new recipe contributions to {doc}`../pangeo_forge_cloud/index` are staged and evaluated via Pull Requests (PRs) against the [`pangeo-forge/staged-recipes`](https://github.com/pangeo-forge/staged-recipes) repository.

To make a PR with your contribution:

1.  [Fork](https://docs.github.com/en/free-pro-team@latest/github/getting-started-with-github/fork-a-repo) the [`pangeo-forge/staged-recipes`](https://github.com/pangeo-forge/staged-recipes) GitHub repository
2. Within the `recipes/` directory of your fork, create a subdirectory with a descriptive name for your dataset.

    ```{note}
    The name you chose for this subdirectory will be used to generate the name for the
    [Feedstock](./core_concepts.md) repo generated from your PR.
    ```

3. Add your [Required files](#required-files) (**Recipe module** and `meta.yaml`) to this new subdirectory of your fork, so that your directory tree now looks like this:

    ```
    staged-recipes/recipes/
                    └──{dataset-name}/
                            ├──meta.yaml
                            └──{recipe-module-name}.py
    ```

3. Open a Pull Request against [`pangeo-forge/staged-recipes`](https://github.com/pangeo-forge/staged-recipes) from your fork.


## PR Checks

Once you've opened a PR against `pangeo-forge/staged-recipes`, a series of checks will be performed to ensure that your [Required files](#required-files) adhere to the expected format and that the {doc}`Recipe Objects <../pangeo_forge_recipes/recipe_user_guide/recipes>` contained within your **Recipe module** can produce the expected datasets when executed in subsetted form.

A full listing of the checks performed on each PR is provided in {doc}`../pangeo_forge_cloud/pr_checks_reference`.

## Creating a new feedstock

Once your [Required files](#required-files) have passed all of the [PR Checks](#pr-checks) documented in {doc}`../pangeo_forge_cloud/pr_checks_reference`, a Pangeo Forge maintainer will merge your PR.

In Pange Forge, merging a PR takes on a special meaning. Rather than integrating your files into the `pangeo-forge/staged-recipes` repository, merging your PR results in the _**automatic creation**_ of a new [Feedstock](core_concepts.md) repository, which is itself automatically populated with the files you've submitted in your PR.

Creation of this new repository will trigger the first full production build of the recipe(s) you've contributed, and the datasets produced by this build will be added to the Pangeo Forge [Catalog](core_concepts.md).

## Maintaining the feedstock

By creating a new repository to house your contribution, there is now a dedicated place for the provenance of datasets built from your recipe(s) to live. Your GitHub account will be granted maintainer permissions on this new repository at the time that it is is created. Subsequent GitHub Issues and PRs on this new [Feedstock](core_concepts.md) repository can be used to correct and/or improve the recipe(s) contained within it, and rebuild the datasets they produce, as needed.
