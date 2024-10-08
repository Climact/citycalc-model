# Citycalc Model - Patex

Source code for the Patex model used in the European Citycalc Project.
This repository contains the code for the Patex model, which is a Python package that can be used to run the Citycalc model locally.

## Testing the model locally

You can run the Patex with the `patex` Python CLI script. For more info, check the help message with:

```sh
python -m patex --help
```

You'll note that the raw outputs of the Patex can be written to a specific location with `-o PATH`. You can compare two such files to ensure the model's outputs have not changed with the `patex.compare_model_outputs` script. Again, check the help with:

```sh
python -m patex.compare_model_outputs --help
```

If you're making modifications to the model which shouldn't affect its output, first run the `patex` script on at least one region and set of levers, saving the output somewhere (e.g., `output_master_be_1.pkl`). Then, after you've made a change, run `patex` again on the modified model, saving the output to a different file (e.g., `output_new_be_1`), then compare the two with `patex.compare_model_outputs` to ensure you haven't changed anything. Once you're done, make sure to delete those files. Do not to commit them to the Git repository.

## Data

When using the 'remote' version of the model, the patex is downloading the data from the s3 bucket associated to the project.
Those data are stored in a parquet format and available [here](https://citycalc-public.s3.eu-central-1.amazonaws.com/index.html).

## License

Shield: [![CC BY-NC-SA 4.0][cc-by-nc-sa-shield]][cc-by-nc-sa]

This work is licensed under a
[Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License][cc-by-nc-sa].

[![CC BY-NC-SA 4.0][cc-by-nc-sa-image]][cc-by-nc-sa]

[cc-by-nc-sa]: http://creativecommons.org/licenses/by-nc-sa/4.0/
[cc-by-nc-sa-image]: https://licensebuttons.net/l/by-nc-sa/4.0/88x31.png
[cc-by-nc-sa-shield]: https://img.shields.io/badge/License-CC%20BY--NC--SA%204.0-lightgrey.svg
