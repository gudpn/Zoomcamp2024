if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    """
    

    #Create a new column lpep_pickup_date by converting lpep_pickup_datetime to a date.
    #Rename columns in Camel Case to Snake Case, e.g. VendorID to vendor_id.
    Add three assertions:
        vendor_id is one of the existing values in the column (currently)
        passenger_count is greater than 0
        trip_distance is greater than 0

    """
    print(data.shape)
    # Row count of  passenger count  > 0 and trip distance > zero.
    # There are data that have 0 or blank in that column
    print(' 1 ',  data[(data['trip_distance'] >0 ) & (data['passenger_count'] > 0)].shape[0] )

    #Remove rows where the passenger count is equal to 0 or the trip distance is equal to zero.
    data = data[(data['trip_distance'] >0 ) & (data['passenger_count'] > 0)]
    #Create a new column lpep_pickup_date by converting lpep_pickup_datetime to a date.
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
    assert output[(output['trip_distance'] >0 )].shape[0] >0 , 'There are rows that trip_distance is not greater than 0'
    assert output[(output['passenger_count'] >0 )].shape[0] >0 , 'There are rows that passenger_count is not greater than 0'
    assert 'vendor_id' in output.columns ,  'vendor_id is not one of the existing values in the column'