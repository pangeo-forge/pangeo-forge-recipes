# Recipe Contribution

You are free to use Pangeo Forge Recipes in private, {doc}`executing <../recipe_user_guide/execution>`
them on your own system / infrastructure however you wish.
However, the real point of Pangeo Forge is to create a crowdsourced database
of public recipes, which are executed automatically in the cloud via {doc}`bakeries`.
This database is called the Pangeo Forge **Recipe Box**.

To contribute a recipe, you will need to understand the basics of Pangeo Forge {doc}`../recipe_user_guide/recipes`.
Working through the {doc}`../tutorials/index` is the best way to develop your understanding.
You will also need a thorough understanding of the source dataset you wish to ingest,
and a vision for the desired target dataset (desired format, chunk structure, etc.)
and any cleaning / processing steps needed along the way.

Once you are ready to begin, head over to <https://github.com/pangeo-forge/staged-recipes>.
This repo is used to triage all new recipes for including in the Recipe Box.
The basic steps for adding you recipe are

1. [fork](https://docs.github.com/en/free-pro-team@latest/github/getting-started-with-github/fork-a-repo) the <https://github.com/pangeo-forge/staged-recipes> GitHub repository
2. Add your recipe(s) to a new folder.
3. Submit the new recipe(s) as a pull request

At this point the pangeo-forge maintainers / bots will verify that your recipe
is shipshape and ready for inclusion in the Recipe Box.

## Steps

### 1. Automated checks by `@pangeo-forge-bot`:

- [ ] Presence of a `meta.yaml` and `recipe.py` in your PR
- [ ] That these files exist within the correct directory structure
- [ ] That `meta.yaml` contains the required fields

If all of these criteria are met, a new recipe run will be created, and you will see a message like this:

```
TODO: add example message
```

### 2. Running a recipe test

TODO: describe iterative process

### 3. Merge PR

Automatically creates feedstock and production run of your recipe

### 4. Maintaining your feedstock
