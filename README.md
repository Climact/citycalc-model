# Pathways Explorer

Source code for the Patex model.

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

## Contribution guidelines

- When contributing a function, make sure that it _does not modify its inputs_. For example, don't do the following:

  ```python
  def something(df):
      df['foo'] = 123
      # rest of the function ...
  ```

  This will edit the input `df`, which might be used somewhere else, resulting in (sometimes hard to find) bugs. Instead, copy the dataframe:

  ```python
  def something(df):
      df = df.copy()
      df['foo'] = 123
      # rest of the function ...
  ```
