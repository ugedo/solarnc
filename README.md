# Globla Horizontal Solar Irradiance Nowcasting

This toolset is designed to systematically explore different machine learning
algorithms for solar irradiation forecasting. Every tool reads a json file as
input that describes the whole experiment. Each tool processes only the relevant
entries for the step it has to complete. The goals in the design are:

- Take advantage of reapeated steps between different experiments, avoiding the
  recomputation of the same data again
- Autodocument the experiment. The json file from which we generate the data
  enables the replication of the experiment by others.
- Be as flexible as possible, so that we can quickly repeat an experiment in
  which we only want to change on step, add a new feature, etc.


## Phases

The experiments are organized in the following phases, for which and independent
tool has been designed:

1. Format
2. Extend
3. Split
4. Feature selection
5. Train
6. Test

The Format phase is used to process the raw input data from a given database and
generate the files with the format that the rest of the phases expect: one cvs
file per in which the first column is a timestamp and the rest of the columns
are the irradiation samples at that timestamp for the different sensors/stations
available. This phase is also used to process the original data to remove NaNs,
remove broken sensors or problematic days. This data cleaning must be provided
by the user as a python function as we describe below.

The Extend phase is used to add new columns to the original data. Theses can be
used to include data from external sources or generate columns computed from the
original data. A typical use of this phase is to generate normalized irradiation
columns for a given clear sky model, or add columns with different combinations
of original columns for non-linear models.

The Split phase generates two files, one for the train phase and one for the
test phase. The split is performed selecting complete days for each set.

The Feature Selection phase selects the features for the machine learning
algorithm, which very generally can be expressed as Y = f(X), where Y is a
vector in which each sample is a prediction (it can contain several columns if
we have several forecasting horizons), and X is a matrix in which each row
contains the features needed to compute the prediction of the corresponding
component of Y. The Feature Selection phase generates two cvs files for each
input cvs (a day of data). The corresponding features.cvs contains the rows of
the X matrix for that day, while the rows of the corresponding targets.cvs
contain the elements of Y.

The Train phase performs the training of one or more algorithms (the f 
function int the mathematical expression of the previous paragraph) on the days
selected as training days in the split phase.

The Test phase performs the tests of one or more algorithms on the days selected
as test days in the split phase.

The data needed for the tools that implement each phase is provided through a
json file that describes all the phases, and contains the paths for all the
files. In the end the json file completely describes the experiment and allows
to reproduced exactly in the future.

## Json format

The format of the json file is described by the solarnc_schema.json file. The
tools look for it in a folder called jsons, that should be located in the parent
directory of the path where the tools are installed. This may change in the
future, if we opt for a better policy to find this json file.

## Tools

### format.py

This first tool in the toolset is the only one that needs code provided by the
user to complete its job. It reads the json file, and extracts two entries:

- dataset, that describes the original dataset. This entry should contain the
  following fields: name, path, timezone and stations. The latter is a list of
  the stations in the dataset. Check the solarnc_schema for more details.

- format, the entry that describes what the tool would do. It is basically a
  field that is given to the users provided python function. It should contain
  the path for the output files and the list of stations that we want to include
  (we can remove some of the existing ones if we want to).  It should also
  contain the python module and the name of the function in that module that
  will do the formating. This file should be provided by the user. As an
  example, see the file nrelformat, which implements the format phase for the
  hawaii_nrel data. It may contain other fields required for the user's provided
  module.

The tool will handle the parsing of the json file and the creation of the
required directories, before calling the user's provided module.

### extend.py

This tool reads the extend entry in the json file, which describes the set of
functions that have to be applied on the formated data to generate new columns.
For each function we indicate the filename that contains its implementation and
the parameters for the function. An example can be found in the csm_pvlib (clear
sky model using pvlib) function provided by the solar solarnc python module.
This function can be used to generate new columns with the irradiance normalized
using several clear sky models implemented in the pvlib library.

### split.py

This tool is used to split the original set of days into two separate sets, one
for train and one for test. The tool reads the split entry from the json config
file, that tells the tool where to store the output cvs files and which method
should be used to perform the split. The only method actually implemented is a
random split, that requieres an argument that tells the percentage of days that
should be included in the train set, the rest will be in the test set.


### fselect.py

This tools implements the feature selection phase. The model considers two kind
of features:

- station features: these correspond to variables from each station
- non-station features: correspond to individual columns added to the data set,
  generally with exogenous data.

In addition, these features are divided in two groups:

- lagged: the model will include lagged samples of these variables with the
  period indicated in the fselect entry of the configuration file.
- unlagged: only one value from these variables will be used as feature

As an example, the normalized irradiance measured at each station would be a
station lagged feature, and the normalized elevation and azimuth would be
station unlagged features. A variable indicating the average wind velocity in
the region could be considered a lagged feature while the season to which that
day belongs could be an unlagged feature.

The tool reads the fselect entry of the configuration file, that contains a
sampling period, a window, a set of features and a forecasting target. The
latter consists of a variable to be forecasted, a list of stations for which we
want to forecast, a forecating horizon and a forecasting interval: at instant t
the model will forecast the mean irradiance value in the interval:

		[min(t1, t2), max(t1, t2)]

where:

		t1 = t + horizon
		t2 = t + horizon + interval

If the interval is negative the model will be trained to predict the average
irradiance in an interval that ends at the forecasting horizon, while if it is
positive the forecasting horizon will establish the start of the forecasting
interval. In the future we could consider adding an offset to be able to center
the forecasting horizon in the middle of the forecasted interval.

The window establishes the time interval in which we consider lagged samples for
the lagged variables, which is [t - window, t]. Window should be a positive time
interval (for example 30min).

The period property indicates the sampling period considered in the window
interval. If the original period is smaller (i.e. faster sampling rate), the
tool will take average values of the original samples (see
[dataframe.resample](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.resample.html?highlight=resample#pandas.DataFrame.resample)).

### train.py

This is the tool that trains one or more models using the feature files
generated in the feature selection step. In the config file we just specify the
model name, the name of the output file and the parameters needed for the model.
For the moment, models should be available in scikitlearn, which is the library
used for training. This may evolve in the future to include other backend
libraries that support different models.

### test.py

This tool performs the test phase on the trained models using the test set
selected in the split phase.


## References

- Related papers:
	- A. Eschenbach et al., "Spatio-Temporal Resolution of Irradiance Samples in
	  Machine Learning Approaches for Irradiance Forecasting," in IEEE Access,
	  vol. 8, pp. 51518-51531, 2020, doi:
	  [10.1109/ACCESS.2020.2980775](https://doi.org/10.1109/ACCESS.2020.2980775).
- [Json Schemas](https://json-schema.org/). Documentation for json schemas.
- Python libraries used:
	- [Pandas](https://pandas.pydata.org/)
	- [numpy](https://numpy.org/)
	- [json](https://docs.python.org/3/library/json.html)
	- [scikit-learn](https://scikit-learn.org/stable/)
	- [pv-lib](https://pvlib-python.readthedocs.io/en/stable/)
	- [multiprocessing](https://docs.python.org/3/library/multiprocessing.html)
	- [joblib](https://joblib.readthedocs.io/en/latest/)
	- [optparse](https://docs.python.org/3/library/optparse.html)
	- [itertools](https://docs.python.org/3/library/itertools.html<Paste>)

