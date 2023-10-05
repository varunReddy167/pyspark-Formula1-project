# Databricks notebook source
def get_data_source(p_data_source):
  """
  This function gets the data source from the Databricks widget.

  Args:
    p_data_source: The name of the Databricks widget.

  Returns:
    The data source.
  """

  v_data_source = dbutils.widgets.get(p_data_source)
  return v_data_source
