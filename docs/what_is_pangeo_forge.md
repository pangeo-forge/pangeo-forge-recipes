# What is Pangeo Forge?

## High Level Overview

Pangeo Forge is an open source framework for data Extraction, Transformation, and Loading (ETL) of scientific data.
The goal of Pangeo Forge is to make it easy to extract data from traditional data
archives and deposit in cloud object storage in [analysis-ready, cloud-optimized (ARCO)](https://ieeexplore.ieee.org/abstract/document/9354557) format.

Pangeo Forge is made of two main components:

- {doc}`pangeo_forge_recipes/index` - an open source Python package, which allows you
  to create and run ETL pipelines ("recipes") and run them from your own computers.
- {doc}`pangeo_forge_cloud/index` - a cloud-based automation framework with runs these recipes in the cloud from code stored in GitHub.

Pangeo Forge is inspired directly by [Conda Forge](https://conda-forge.org/), a
community-led collection of recipes for building conda packages.
We hope that Pangeo Forge can play the same role for datasets.
## The Pangeo Forge Paper

We wrote a long academic paper about Pangeo Forge.
This is the best thing to read if you want a deep dive into why we created
Pangeo Forge and how it works.

````{panels}
:column: col-lg-12 p-2
**Pangeo Forge: Crowdsourcing Analysis-Ready, Cloud Optimized Data Production**

Stern Charles, Abernathey Ryan, Hamman Joseph, Wegener Rachel, Lepore Chiara, Harkins Sean, Merose Alexander

_Frontiers in Climate_, 10 February 2022

<https://doi.org/10.3389/fclim.2021.782909>

**Abstract:**
Pangeo Forge is a new community-driven platform that accelerates science by providing high-level recipe frameworks alongside cloud compute infrastructure for extracting data from provider archives, transforming it into analysis-ready, cloud-optimized (ARCO) data stores, and providing a human- and machine-readable catalog for browsing and loading. In abstracting the scientific domain logic of data recipes from cloud infrastructure concerns, Pangeo Forge aims to open a door for a broader community of scientists to participate in ARCO data production. A wholly open-source platform composed of multiple modular components, Pangeo Forge presents a foundation for the practice of reproducible, cloud-native, big-data ocean, weather, and climate science without relying on proprietary or cloud-vendor-specific tooling.
````

If you use Pangeo Forge in academic work, please cite this paper.

````{admonition} Bibtex entry for Pangeo Forge paper
:class: dropdown

```text
@ARTICLE{10.3389/fclim.2021.782909,
AUTHOR={Stern, Charles and Abernathey, Ryan and Hamman, Joseph and Wegener, Rachel and Lepore, Chiara and Harkins, Sean and Merose, Alexander},
TITLE={Pangeo Forge: Crowdsourcing Analysis-Ready, Cloud Optimized Data Production},
JOURNAL={Frontiers in Climate},
VOLUME={3},
YEAR={2022},
URL={https://www.frontiersin.org/article/10.3389/fclim.2021.782909},
DOI={10.3389/fclim.2021.782909},
ISSN={2624-9553},
}
```

````



## Frequently Asked Questions

### Is Pangeo Forge the right tool for my dataset?

Pangeo Forge is currently focused primarily on multidimensional array data
(e.g. data that fit the NetCDF data model and can be opened by Xarray.)
This includes NetCDF, GRIB, HDF4/5, Zarr, OPeNDAP, and many other obscure formats.
Pangeo Forge is _not_ currently focused on tabular data
(e.g. data that can be opened by Pandas; CSV, SQL, Parquet, Arrow, etc.)
This data model and these formats are very well supported by other ETL tools in the modern data stack.

So if you want to do ETL on multidimensional array data, Pangeo Forge is for you!
For more feedback, [open an issue](https://github.com/pangeo-forge/staged-recipes/issues) in staged recipes.

### How is Pangeo Forge funded?

Right now, the development of Pangeo Forge is funded exclusively by the US National Science Foundation
EarthCube program, via award [2026932](https://www.nsf.gov/awardsearch/showAward?AWD_ID=2026932)
to Lamont Doherty Earth Observatory. The lead PI of the award is Ryan Abernathey.

Going forward, we hope to establish a multi-stakeholder coalition to support
the operation of Pangeo Forge as a stable, reliable service for the research community.
Please reach out if you're interested in helping with this effort.

### How can I help with development?

Check out our {doc}`pangeo_forge_recipes/development/development_guide` for how to get started!
