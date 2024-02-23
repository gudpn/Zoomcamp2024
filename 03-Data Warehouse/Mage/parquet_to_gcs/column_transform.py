if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    #Rename columns in Camel Case to Snake Case, e.g. VendorID to vendor_id.
  
    data = data.rename(columns=
        {
            'VendorID'		:'vendor_id'		,
            'RatecodeID'	:'ratecode_id'	    ,
            'PULocationID'	:'pu_location_id'	,
            'DOLocationID'	:'do_location_id'	    
        })

    print(data["vendor_id"].unique())
    return data




@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
