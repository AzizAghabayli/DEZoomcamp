if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import re


@transformer
def transform(data, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """

    # Create column lpep_pickup_date by converting lpep_pickup_datetime to a date.
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    # Rename columns
    def camel_to_snake(name):
        name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
        return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()
    
    data.columns = [camel_to_snake(col) for col in data.columns]

    # Remove rows with 0 passengers and 0 trip distance.
    print(f"Preprocessing: rows with zero passengers: {data['passenger_count'].isin([0]).sum()}")
    print(f"Preprocessing: rows with zero trip distance: {data['trip_distance'].isin([0]).sum()}")

    return data[(data['passenger_count'] > 0) & (data['trip_distance'] > 0)]


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    
    assert 'vendor_id' in output.columns, "'vendor_id' is not one of the column names"
    assert output['passenger_count'].isin([0]).sum() == 0, 'There are rides with zero passengers'
    assert output['trip_distance'].isin([0]).sum() == 0, 'There are rides with zero trip distance'
