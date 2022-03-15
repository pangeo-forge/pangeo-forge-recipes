# Recipe Contribution

To contribute a recipe, you will need to understand the basics of [Pangeo Forge Recipes](../recipe_user_guide/index).
The {doc}`../introduction_tutorial/index` is a great place to start, and working through the {doc}`../tutorials/index` can help develop your understanding.

You will also need a thorough understanding of the source dataset you wish to ingest,
and a vision for the desired target dataset (desired format, chunk structure, etc.)
and any cleaning / processing steps needed along the way.

## Required files

### `meta.yaml`


### Recipe module


## Making a Pull Request (PR)

Once you are ready to begin, head over to <https://github.com/pangeo-forge/staged-recipes>.
This repo is used to triage all new recipes for including in the Recipe Box.
The basic steps for adding you recipe are

1. [fork](https://docs.github.com/en/free-pro-team@latest/github/getting-started-with-github/fork-a-repo) the <https://github.com/pangeo-forge/staged-recipes> GitHub repository
2. Add your recipe(s) to a new folder.
3. Submit the new recipe(s) as a pull request

At this point the pangeo-forge maintainers / bots will verify that your recipe
is shipshape and ready for inclusion in the Recipe Box.

## After opening your PR

### 1. Automated checks by `@pangeo-forge-bot`:

- [ ] Presence of a `meta.yaml` and `recipe.py` in your PR
- [ ] That these files exist within the correct directory structure
- [ ] That `meta.yaml` contains the required fields

If all of these criteria are met, a new recipe run will be created, and you will see a message like this:

```{panels}
:column: col-lg-12 p-2

{{ pangeo_forge_bot_header }}
^^^^^^^^^^^^^^
🎉 New recipe runs created for the following recipes at sha `ce059a4`:
 - `woa18-1deg-monthly`: https://pangeo-forge.org/dashboard/recipe-run/11
```

```{panels}
:column: col-lg-12 p-2

**recipe-contributor**
^^^^^^^^^^^^^^

content

```

### 2. Running a recipe test

TODO: describe iterative process

### 3. Merge PR

Automatically creates feedstock and production run of your recipe

### 4. Maintaining your feedstock
