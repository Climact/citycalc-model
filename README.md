# Pathways Explorer

Source code for the PatEx model.

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
