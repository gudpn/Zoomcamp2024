## Question 1. Data Loading

Once the dataset is loaded, what's the shape of the data?
    print(data.shape)
    
    266,855 rows x 20 columns


## Question 2. Data Transformation

Upon filtering the dataset where the passenger count is greater than 0 and the trip distance is greater than zero, how many rows are left?

    print( data[(data['trip_distance'] >0 ) & (data['passenger_count'] > 0)].shape[0] )
    
    139,370 rows


## Question 3. Data Transformation

Which of the following creates a new column lpep_pickup_date by converting lpep_pickup_datetime to a date?


    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date


## Question 4. Data Transformation

What are the existing values of VendorID in the dataset?

    print(data["vendor_id"].unique())

    1 or 2


## Question 5. Data Transformation

How many columns need to be renamed to snake case?
    data.columns = (data.columns
                    .str.replace(' ','_')
                    .str.lower()
                    )
    or 

    data = data.rename(columns=
        {
            'VendorID'		:'vendor_id'		,
            'RatecodeID'	:'ratecode_id'	    ,
            'PULocationID'	:'pu_location_id'	,
            'DOLocationID'	:'do_location_id'	    
        })
    

    4

## Question 6. Data Exporting

Once exported, how many partitions (folders) are present in Google Cloud?

    96
    56
    67
    108

96?

95 total + 1 folder 
