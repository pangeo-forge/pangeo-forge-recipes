# Defining a Recipe (Intro Tutorial Part 1)

Welcome to the Pangeo Forge introduction tutorial!

This tutorial is split into three parts:
1. Defining a recipe
1. Running a recipe locally
2. Setting up a recipe to run in the cloud

Throughout this tutorial we are going to convert NOAA OISST stored in netCDF to Zarr. OISST is a global, gridded ocean sea surface temperature dataset at daily 1/4 degree resolution. By the end of this tutorial sequence you will have converted some OISST data to zarr, be able to access a sample on your computer, and see how to propose the recipe for cloud deployment!

Here we tackle **Part 1 - Defining a Recipe**. We will assume that you already have `pangeo-forge-recipes` installed.

## Steps to Creating a Recipe

The two major pieces of creating a recipe are:

1. Creating a generalized URL pattern
1. Defining a File Pattern object
2. Defining a Recipe Class object

We will talk about each of these steps in turn.

### Where should I write this code?
Eventually, all of the code defining the recipe will need to go in a file called `recipe.py`. If you typically work in scripts and want to start that way from the beginning that is great. It is also totally fine to work on your recipe code in a Jupyter Notebook and then copy the final code to a single script later. The choice between the two is personal preference.

## A Generalized URL Pattern for OISST

Like many datasets, OISST is availble via an HTTP URL.

[https://www.ncei.noaa.gov/data/sea-surface-temperature-optimum-interpolation/v2.1/access/avhrr/](https://www.ncei.noaa.gov/data/sea-surface-temperature-optimum-interpolation/v2.1/access/avhrr/)

By putting the URL into a webbrowser we can explore the organization of OISST. This is important because Pangeo Forge relies on the organization of a dataset URL to scale data access.

As subset of the webpage that displays the OISST data looks like this:

```
Parent Directory	 	-	 
198109/	2020-05-15 17:08	-	 
198110/	2020-05-15 17:08	-	 
198111/	2020-05-15 17:08	-	 
198112/	2020-05-15 17:08	-	 
198201/	2020-05-15 17:08	-	 
198202/	2020-05-15 17:08	-	 
198203/	2020-05-15 17:08	-	 
198204/	2020-05-15 17:08	-	 
198205/	2020-05-15 17:08	-	 
198206/	2020-05-15 17:08	-	
```

We see that each folder on this page is listing a 4-digit year followed by 2-digit month. Clicking on a particular month (for example `198112/`) we see:

```
Parent Directory	 	-	 
oisst-avhrr-v02r01.19811201.nc	2020-05-15 11:14	1.6M	 
oisst-avhrr-v02r01.19811202.nc	2020-05-15 11:15	1.6M	 
oisst-avhrr-v02r01.19811203.nc	2020-05-15 11:07	1.6M	 
oisst-avhrr-v02r01.19811204.nc	2020-05-15 11:07	1.6M	 
oisst-avhrr-v02r01.19811205.nc	2020-05-15 11:08	1.6M	 
oisst-avhrr-v02r01.19811206.nc	2020-05-15 11:07	1.6M	 
oisst-avhrr-v02r01.19811207.nc	2020-05-15 11:08	1.6M	 
oisst-avhrr-v02r01.19811208.nc	2020-05-15 11:08	1.6M	 
oisst-avhrr-v02r01.19811209.nc	2020-05-15 11:10	1.6M	 
```

By putting together the full URL for a single file we can see that the OISST dataset for December 9th, 1981 would be accessed using the URL:

[https://www.ncei.noaa.gov/data/sea-surface-temperature-optimum-interpolation/v2.1/access/avhrr/198112/oisst-avhrr-v02r01.19811209.nc](https://www.ncei.noaa.gov/data/sea-surface-temperature-optimum-interpolation/v2.1/access/avhrr/198112/oisst-avhrr-v02r01.19811209.nc)

Copying and pasting that url into a webbrowser will download that single file to your computer.

### A generalized URL pattern
We can generalize the URL to say that OISST datasets are accessed using a URL of the format:

`https://www.ncei.noaa.gov/data/sea-surface-temperature-optimum-interpolation/v2.1/access/avhrr/{year-month)/oisst-avhrr-v02r01.{year-month-day}.nc`

where `{year-month}` and `{year-month-day}` change for each file. Of the three dimensions of this dataset - latitude, longitude and time - the individual files are split up by time.

### Why does this matter so much?
Pangeo Forge File Patterns are built on the premise that 1) datasets accessible by URL will be organized in a predictable way 2) the URL organization tells us something about the structure of the dataset. Knowing the generalized structure of the OISST URL leads us to the next step of creating a recipe - defining the File Pattern.

## Defining a File Pattern object

There are several different ways to define [File Patterns](https://pangeo-forge.readthedocs.io/en/latest/recipe_user_guide/file_patterns.html) in Pangeo Forge. In this tutorial we are going to use `pattern_from_file_sequence`.  The input to the `pattern_from_file_sequence` function is a list of urls to the files of the dataset that we want to convert.  In other words, our goal is to create a list that looks like this:

```
["https://www.ncei.noaa.gov/data/sea-surface-temperature-optimum-interpolation/v2.1/ \ 
access/avhrr/ 199006/oisst-avhrr-v02r01.19900601.nc", "https://www.ncei.noaa.gov/data \ 
/sea-surface-temperature-optimum-interpolation/v2.1/access/avhrr/199006/oisst-avhrr-v02r01. \ 
19900602.nc", "https://www.ncei.noaa.gov/data/sea-surface-temperature-optimum-interpolation \ 
/v2.1/access/avhrr/199006/oisst-avhrr-v02r01.19900603.nc", ... ]
```

To create the list of URLs we will use the library `pandas` and Python format strings.

### Create a format string & function

First, let's use `pd.date_range()` to create a list of dates. We will just define the first month of data for this tutorial, but in practice you could define the date range to be the entire range of the dataset. We will also use `freq='D'` because OISST is a daily dataset.


```python
import pandas as pd
```


```python
dates = pd.date_range('1982-01-01', '1982-02-01', freq='D')
print(dates[:10])
```

    DatetimeIndex(['1982-01-01', '1982-01-02', '1982-01-03', '1982-01-04',
                   '1982-01-05', '1982-01-06', '1982-01-07', '1982-01-08',
                   '1982-01-09', '1982-01-10'],
                  dtype='datetime64[ns]', freq='D')


Great, we have our dates as a list of Python datetimes. Now we want to create a Python format string so that we can use this list of dates to programatically create all of our URLs. 

For OISST the format string for our generalized URL looks like this:


```python
input_url_pattern = (
    'https://www.ncei.noaa.gov/data/sea-surface-temperature-optimum-interpolation/'
    'v2.1/access/avhrr/{yyyymm}/oisst-avhrr-v02r01.{yyyymmdd}.nc'
    )
```

We have subsituted the parts of the URL that change for each file with `{}`. The `{}` allow us to use Python's `.format()` function for strings and the `.strftime()` method for datetime objects to create each file's url. For example, we could create the format string for the first day of the dataset like this:


```python
print('formating OISST URL for the date ', dates[0])

input_url_pattern.format(
    yyyymm=dates[0].strftime('%Y%m'), yyyymmdd=dates[0].strftime('%Y%m%d')
)
```

    formating OISST URL for the date  1982-01-01 00:00:00





    'https://www.ncei.noaa.gov/data/sea-surface-temperature-optimum-interpolation/v2.1/access/avhrr/198201/oisst-avhrr-v02r01.19820101.nc'



`%Y%m` and `%Y%m%d` are the format specifiers that describe `{year-month}` and `{year-month-day}` as they appeared in the generalized OISST URL. You can view a reference for `.strftime()` format specifiers [here](https://strftime.org/).

We programatically created a URL string for the first file! 

### Create a list of all the dataset urls

Now let's put these pieces together. We will put our 3 parts
1. the `dates` sequence
2. the `input_url_pattern` format string
3. the `.format()` function

into a list comprehension to create the urls for all the files in 1 month of the OISST dataset.


```python
input_urls = [
    input_url_pattern.format(
        yyyymm=day.strftime('%Y%m'), yyyymmdd=day.strftime('%Y%m%d')
    )
    for day in dates
]
```


```python
print(input_urls[:3])
```

    ['https://www.ncei.noaa.gov/data/sea-surface-temperature-optimum-interpolation/v2.1/access/avhrr/198201/oisst-avhrr-v02r01.19820101.nc', 'https://www.ncei.noaa.gov/data/sea-surface-temperature-optimum-interpolation/v2.1/access/avhrr/198201/oisst-avhrr-v02r01.19820102.nc', 'https://www.ncei.noaa.gov/data/sea-surface-temperature-optimum-interpolation/v2.1/access/avhrr/198201/oisst-avhrr-v02r01.19820103.nc']


There we have it!  A list of URLs for the OISST dataset. By changing the dates in the `pandas` `pd.date_range()` function we could generate data access URLs for the full OISST dataset.

### Create the File Pattern object

Now we return to our `pattern_from_file_sequence` function. The implementation looks like this:


```python
from pangeo_forge_recipes.patterns import pattern_from_file_sequence
```


```python
pattern = pattern_from_file_sequence(input_urls, 'time', nitems_per_file=1)
```

The arguments are:
* `input_urls` - the list of file urls we just created
* `'time'` - indicates which variable is changing between files. Because each file of OISST is a new day of data with the same variable (sea surface temperature) and spatial extent, `time` is the input for OISST.
* `nitems_per_file` - specifies that each OISST file contains a single timestep (1 day per file for OISST)

When we print `pattern` we see that it is a `FilePattern` object with 32 timesteps.


```python
print(pattern)
```

    <FilePattern {'time': 32}>


## Defining a Recipe Class object

Now that we have our File Pattern object the [Recipe Class](https://pangeo-forge.readthedocs.io/en/latest/recipe_user_guide/recipes.html) comes pretty quickly. In this tutorial we want to convert our dataset to zarr, so we will use the `XarrayZarrRecipe` class. Implementing the class looks like this:


```python
from pangeo_forge_recipes.recipes import XarrayZarrRecipe
```


```python
recipe = XarrayZarrRecipe(pattern, inputs_per_chunk=10)
```

The arguments are:
1. the FilePattern object
2. `inputs_per_chunk` - indicates how many files should go into a single chunk of the zarr store

In more complex recipes additional arguments that may get used, but for this tutorial these two are all we need.

## End of Part 1
And there you have it - your first recipe object! Inside that object is all the information about the dataset that is needed to run the data conversion. Pretty compact!

In part 2 of the tutorial, we will use our recipe object, `recipe` to convert some data locally.


