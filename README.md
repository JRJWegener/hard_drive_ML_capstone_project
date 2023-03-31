# Taking your data for a spin!


A machine learning project by 

[Kelechi John](https://github.com/kelechijohn)  
[Frederik Bruns](https://github.com/freddiCoded)  
[Jakob Wegener](https://github.com/JRJWegener)  

## Introduction
Hard drive failure is a major problem for cloud storage businesses and maybe your very own hard drive will fail at some point and your data will be lost! In this project we use sensor data to detect hard drive failure before it happens. We transform the data into a linear format with convolutional kernels and classify with a random forest model.

## Structure

The data import and target creation happens in this [notebook](notebooks/import_target_creation.ipynb).  
There is one notebook where we did [exploratory Data Analysis](notebooks/EDA.ipynb).  
After EDA we prepared the data for modeling in the [transforming](notebooks/transforming.ipynb) notebook.  
If you want to see the modeling part, [here](notebooks/modeling.ipynb) is the place to look.  

We tackled this problem in and out and if you would like to see a summary of our findings and methods, you can have a look at this [presentation](presentation.pdf)!


Download the dataset here: https://www.backblaze.com/b2/hard-drive-test-data.html#downloading-the-raw-hard-drive-test-data

To make the data import work save the data into the "data" folder.



## Requirements:

- pyenv with Python: 3.9.4

### Setup

Use the requirements file in this repo to create a new environment.

```BASH
make setup

#or

pyenv local 3.9.8
python -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements_dev.txt
```

The `requirements.txt` file contains the libraries needed for deployment.





